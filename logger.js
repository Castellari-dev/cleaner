const { createLogger, format, transports } = require('winston');
const { combine, timestamp, printf, colorize } = format;
const path = require('path');

const logFormat = printf(({ level, message, timestamp }) => {
  return `[${timestamp}] ${level}: ${message}`;
});

const logger = createLogger({
  level: 'info',
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    logFormat
  ),
  transports: [
    new transports.Console({ format: combine(colorize(), timestamp(), logFormat) }),
    new transports.File({ filename: path.join(__dirname, 'delete.log') })
  ],
  exceptionHandlers: [
    new transports.File({ filename: path.join(__dirname, 'exceptions.log') })
  ]
});

module.exports = logger;