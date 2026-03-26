import winston from 'winston';

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console({
            level: 'debug',
            stderrLevels: ['error', 'warn', 'info', 'http', 'verbose', 'debug', 'silly'],
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.timestamp(),
                winston.format.printf(({ level, message, timestamp, ...metadata }) => {
                    let msg = `${timestamp} [${level}] ${message}`;
                    if (Object.keys(metadata).length > 0) {
                        msg += ` ${JSON.stringify(metadata)}`;
                    }
                    return msg;
                })
            )
        })
    ]
});

export default logger;
