require('dotenv').config();
const sql = require('mssql');
const cron = require('node-cron');
const logger = require('./logger');

// Configuração do banco de dados
const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    options: {
        encrypt: process.env.DB_ENCRYPT !== 'false',
        trustServerCertificate: process.env.DB_TRUST_CERT === 'true',
        requestTimeout: 300000, // 5 minutos
        connectionTimeout: 30000, // 30 segundos
    },
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    }
};

// Configurações do sistema de limpeza
const configuracao = {
    tabela: process.env.TABELA,
    colunaData: process.env.COLUNA_DATA,
    mesesRetencao: parseInt(process.env.MESES_RETENCAO) || 3,
    batchSize: parseInt(process.env.BATCH_SIZE) || 1000,
    intervaloMs: parseInt(process.env.INTERVALO_MS) || 5000, // Menor intervalo para cleanup diário
    maxRetries: parseInt(process.env.MAX_RETRIES) || 3,
    horarioExecucao: process.env.HORARIO_EXECUCAO || '0 10 * * *', // 10:00 AM todos os dias
    timezone: process.env.TIMEZONE || 'America/Sao_Paulo'
};

// Validação de variáveis de ambiente
function validarVariaveisAmbiente() {
    const requiredVars = ['DB_USER', 'DB_PASSWORD', 'DB_SERVER', 'DB_DATABASE', 'TABELA', 'COLUNA_DATA'];
    const missing = requiredVars.filter(varName => !process.env[varName]);
    
    if (missing.length > 0) {
        throw new Error(`Variáveis de ambiente obrigatórias não encontradas: ${missing.join(', ')}`);
    }
}

// Função para calcular data de corte (3 meses atrás)
function calcularDataCorte(mesesAtras = 3) {
    const agora = new Date();
    const dataCorte = new Date(agora);
    dataCorte.setMonth(agora.getMonth() - mesesAtras);
    
    // Ajustar para o primeiro dia do mês para evitar problemas com dias que não existem
    dataCorte.setDate(1);
    dataCorte.setHours(0, 0, 0, 0);
    
    return dataCorte;
}

// Função para sanitizar nome da tabela
function sanitizarNomeTabela(nomeTabela) {
    return nomeTabela.replace(/[^a-zA-Z0-9_]/g, '');
}

// Função para fazer retry em caso de erro
async function executarComRetry(operacao, maxTentativas = 3, delayMs = 1000) {
    for (let tentativa = 1; tentativa <= maxTentativas; tentativa++) {
        try {
            return await operacao();
        } catch (error) {
            logger.warn(`Tentativa ${tentativa}/${maxTentativas} falhou: ${error.message}`);
            
            if (tentativa === maxTentativas) {
                throw error;
            }
            
            const delay = delayMs * Math.pow(2, tentativa - 1);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

// Função para verificar e limpar registros antigos
async function executarLimpezaAutomatica() {
    let pool = null;
    const inicioExecucao = new Date();
    
    try {
        logger.info('🚀 Iniciando processo de limpeza automática...');
        
        const { tabela, colunaData, mesesRetencao, batchSize, intervaloMs, maxRetries } = configuracao;
        const tabelaSanitizada = sanitizarNomeTabela(tabela);
        const colunaSanitizada = sanitizarNomeTabela(colunaData);
        
        // Calcular data de corte
        const dataCorte = calcularDataCorte(mesesRetencao);
        
        logger.info(`📅 Data de corte calculada: ${dataCorte.toISOString().split('T')[0]}`);
        logger.info(`🗂️ Mantendo apenas registros dos últimos ${mesesRetencao} meses`);
        
        // Conectar ao banco
        pool = await sql.connect(config);
        logger.info('✅ Conexão com o banco estabelecida');
        
        // Verificar se existem registros para limpar
        const countResult = await pool.request()
            .input('dataCorte', sql.DateTime, dataCorte)
            .query(`
                SELECT COUNT(*) as total 
                FROM ${tabelaSanitizada} 
                WHERE ${colunaSanitizada} < @dataCorte
            `);
        
        const totalRegistrosAntigos = countResult.recordset[0].total;
        
        if (totalRegistrosAntigos === 0) {
            logger.info('✅ Nenhum registro antigo encontrado. Base de dados está limpa!');
            return { sucesso: true, totalDeletados: 0, tempoExecucao: 0 };
        }
        
        logger.info(`🧹 Encontrados ${totalRegistrosAntigos} registros antigos para limpeza`);
        
        // Executar limpeza em lotes
        let totalDeletados = 0;
        let loteAtual = 1;
        const totalLotes = Math.ceil(totalRegistrosAntigos / batchSize);
        
        while (true) {
            const resultado = await executarComRetry(async () => {
                // Buscar IDs para deletar
                const idsResult = await pool.request()
                    .input('dataCorte', sql.DateTime, dataCorte)
                    .query(`
                        SELECT TOP (${batchSize}) idMonitoramento 
                        FROM ${tabelaSanitizada}
                        WHERE ${colunaSanitizada} < @dataCorte
                        ORDER BY ${colunaSanitizada} ASC
                    `);
                
                const ids = idsResult.recordset.map(row => row.idMonitoramento);
                
                if (ids.length === 0) {
                    return { deletados: 0, ids: [] };
                }
                
                // Deletar usando parâmetros preparados
                const request = pool.request();
                const placeholders = ids.map((id, index) => {
                    request.input(`id${index}`, sql.Int, id);
                    return `@id${index}`;
                }).join(',');
                
                const deleteResult = await request.query(`
                    DELETE FROM ${tabelaSanitizada}
                    WHERE idMonitoramento IN (${placeholders})
                `);
                
                return {
                    deletados: deleteResult.rowsAffected[0] || 0,
                    ids: ids
                };
            }, maxRetries);
            
            const { deletados, ids } = resultado;
            
            if (deletados === 0) {
                break;
            }
            
            totalDeletados += deletados;
            const progresso = ((totalDeletados / totalRegistrosAntigos) * 100).toFixed(1);
            
            logger.info(`🔹 Lote ${loteAtual}/${totalLotes} - Deletados ${deletados} registros (${progresso}%)`);
            
            loteAtual++;
            
            if (deletados < batchSize) {
                break;
            }
            
            // Aguardar antes do próximo lote
            if (totalDeletados < totalRegistrosAntigos) {
                await new Promise(resolve => setTimeout(resolve, intervaloMs));
            }
        }
        
        const fimExecucao = new Date();
        const tempoExecucao = Math.round((fimExecucao - inicioExecucao) / 1000);
        
        logger.info(`🎉 Limpeza automática concluída com sucesso!`);
        logger.info(`📊 Total de registros deletados: ${totalDeletados}`);
        logger.info(`⏱️ Tempo de execução: ${tempoExecucao} segundos`);
        
        return { 
            sucesso: true, 
            totalDeletados, 
            tempoExecucao,
            dataCorte: dataCorte.toISOString()
        };
        
    } catch (err) {
        const fimExecucao = new Date();
        const tempoExecucao = Math.round((fimExecucao - inicioExecucao) / 1000);
        
        logger.error(`❌ Erro durante limpeza automática: ${err.message}`);
        logger.error(err.stack);
        
        return { 
            sucesso: false, 
            erro: err.message,
            tempoExecucao
        };
    } finally {
        if (pool) {
            try {
                await pool.close();
                logger.info('🔌 Conexão com o banco fechada');
            } catch (closeErr) {
                logger.error(`⚠️ Erro ao fechar conexão: ${closeErr.message}`);
            }
        }
    }
}

// Função para verificar saúde do sistema
async function verificarSaude() {
    let pool = null;
    
    try {
        logger.info('🏥 Verificando saúde do sistema...');
        
        pool = await sql.connect(config);
        
        const { tabela, colunaData, mesesRetencao } = configuracao;
        const tabelaSanitizada = sanitizarNomeTabela(tabela);
        const colunaSanitizada = sanitizarNomeTabela(colunaData);
        const dataCorte = calcularDataCorte(mesesRetencao);
        
        // Verificar estatísticas da tabela
        const statsResult = await pool.request()
            .input('dataCorte', sql.DateTime, dataCorte)
            .query(`
                SELECT 
                    COUNT(*) as total_registros,
                    COUNT(CASE WHEN ${colunaSanitizada} < @dataCorte THEN 1 END) as registros_antigos,
                    MIN(${colunaSanitizada}) as data_mais_antiga,
                    MAX(${colunaSanitizada}) as data_mais_recente
                FROM ${tabelaSanitizada}
            `);
        
        const stats = statsResult.recordset[0];
        
        logger.info(`📊 Estatísticas da tabela ${tabela}:`);
        logger.info(`   • Total de registros: ${stats.total_registros}`);
        logger.info(`   • Registros antigos (>${mesesRetencao} meses): ${stats.registros_antigos}`);
        logger.info(`   • Data mais antiga: ${stats.data_mais_antiga}`);
        logger.info(`   • Data mais recente: ${stats.data_mais_recente}`);
        
        return {
            sucesso: true,
            totalRegistros: stats.total_registros,
            registrosAntigos: stats.registros_antigos,
            dataMaisAntiga: stats.data_mais_antiga,
            dataMaisRecente: stats.data_mais_recente
        };
        
    } catch (err) {
        logger.error(`❌ Erro na verificação de saúde: ${err.message}`);
        return { sucesso: false, erro: err.message };
    } finally {
        if (pool) {
            try {
                await pool.close();
            } catch (closeErr) {
                logger.error(`⚠️ Erro ao fechar conexão: ${closeErr.message}`);
            }
        }
    }
}

// Classe principal do agendador
class LimpezaAutomatica {
    constructor() {
        this.tarefaAgendada = null;
        this.estatisticas = {
            execucoes: 0,
            sucessos: 0,
            falhas: 0,
            ultimaExecucao: null,
            proximaExecucao: null
        };
    }
    
    // Iniciar o agendamento
    iniciar() {
        try {
            validarVariaveisAmbiente();
            
            logger.info('🕘 Iniciando sistema de limpeza automática...');
            logger.info(`⏰ Agendamento: ${configuracao.horarioExecucao} (${configuracao.timezone})`);
            logger.info(`📅 Retenção: ${configuracao.mesesRetencao} meses`);
            logger.info(`📦 Lote: ${configuracao.batchSize} registros`);
            
            // Agendar tarefa
            this.tarefaAgendada = cron.schedule(
                configuracao.horarioExecucao, 
                async () => {
                    await this.executarTarefa();
                },
                {
                    scheduled: true,
                    timezone: configuracao.timezone
                }
            );
            
            // Calcular próxima execução
            this.calcularProximaExecucao();
            
            logger.info(`✅ Sistema iniciado com sucesso!`);
            logger.info(`🔮 Próxima execução: ${this.estatisticas.proximaExecucao}`);
            
            // Verificar saúde inicial
            setTimeout(() => this.verificarSaudeInicial(), 5000);
            
        } catch (error) {
            logger.error(`💥 Erro ao iniciar sistema: ${error.message}`);
            throw error;
        }
    }
    
    // Parar o agendamento
    parar() {
        if (this.tarefaAgendada) {
            this.tarefaAgendada.stop();
            this.tarefaAgendada = null;
            logger.info('🛑 Sistema de limpeza automática parado');
        }
    }
    
    // Executar tarefa agendada
    async executarTarefa() {
        this.estatisticas.execucoes++;
        this.estatisticas.ultimaExecucao = new Date().toISOString();
        
        logger.info(`🎯 Executando limpeza automática (#${this.estatisticas.execucoes})`);
        
        const resultado = await executarLimpezaAutomatica();
        
        if (resultado.sucesso) {
            this.estatisticas.sucessos++;
            logger.info(`✅ Execução #${this.estatisticas.execucoes} concluída com sucesso`);
        } else {
            this.estatisticas.falhas++;
            logger.error(`❌ Execução #${this.estatisticas.execucoes} falhou: ${resultado.erro}`);
        }
        
        this.calcularProximaExecucao();
        logger.info(`🔮 Próxima execução: ${this.estatisticas.proximaExecucao}`);
    }
    
    // Verificar saúde inicial
    async verificarSaudeInicial() {
        logger.info('🏥 Executando verificação inicial de saúde...');
        await verificarSaude();
    }
    
    // Calcular próxima execução
    calcularProximaExecucao() {
        // Esta é uma aproximação - para cálculo exato seria necessário uma biblioteca mais robusta
        const agora = new Date();
        const amanha = new Date(agora);
        amanha.setDate(agora.getDate() + 1);
        amanha.setHours(10, 0, 0, 0);
        
        this.estatisticas.proximaExecucao = amanha.toISOString();
    }
    
    // Obter estatísticas
    obterEstatisticas() {
        return { ...this.estatisticas };
    }
    
    // Executar limpeza manual
    async executarManual() {
        logger.info('🔧 Executando limpeza manual...');
        return await executarLimpezaAutomatica();
    }
}

// Instância global
const limpezaAutomatica = new LimpezaAutomatica();

// Graceful shutdown
process.on('SIGINT', async () => {
    logger.info('🛑 Recebido sinal de interrupção. Finalizando graciosamente...');
    limpezaAutomatica.parar();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    logger.info('🛑 Recebido sinal de término. Finalizando graciosamente...');
    limpezaAutomatica.parar();
    process.exit(0);
});

// Iniciar automaticamente se for o arquivo principal
if (require.main === module) {
    limpezaAutomatica.iniciar();
    
    // Manter processo vivo
    process.stdin.resume();
}