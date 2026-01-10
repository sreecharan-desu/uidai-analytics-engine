import mongoose from 'mongoose';
import { config } from '../config';
import logger from '../utils/logger';

export const connectDB = async () => {
  try {
    await mongoose.connect(config.mongoUri, {
      dbName: config.dbName,
    });
    logger.info('MongoDB connected successfully');
  } catch (error) {
    logger.error('MongoDB connection failed', error);
    process.exit(1);
  }
};
