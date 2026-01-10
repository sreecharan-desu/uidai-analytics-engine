import app from './app';
import { config } from './config';
import logger from './utils/logger';

const startServer = async () => {
  
  const port = process.env.PORT || 3000;
  
  app.listen(port, () => {
    logger.info(`Server running in ${config.nodeEnv} mode on port ${port}`);
  });
};

startServer();
