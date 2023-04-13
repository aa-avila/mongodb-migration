import logger from 'simple-node-logger';

const loggerInstance = logger.createSimpleLogger();

loggerInstance.setLevel(process.env.LOGGER_LEVEL);

export default loggerInstance;
