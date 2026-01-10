import app from './app';
import { config } from './config';
import logger from './utils/logger';
import { connectDB } from './db';

const startServer = async () => {
  await connectDB();
  
  const port = process.env.PORT || 3000;
  
  app.listen(port, () => {
    logger.info(`Server running in ${config.nodeEnv} mode on port ${port}`);
  });
};

startServer();
