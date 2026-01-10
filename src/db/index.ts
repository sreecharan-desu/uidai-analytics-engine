import mongoose from 'mongoose';
import { config } from '../config';
import logger from '../utils/logger';

// Cache the connection to reuse across invocations in Serverless environment
let cachedConnection: typeof mongoose | null = null;

export const connectDB = async () => {
    if (cachedConnection && mongoose.connection.readyState === 1) {
        return cachedConnection;
    }

    try {
        const conn = await mongoose.connect(config.mongoUri, {
            dbName: config.dbName,
            maxPoolSize: 10, // Limit pool size for serverless
            serverSelectionTimeoutMS: 5000, // Fail fast if DB unreachable
            socketTimeoutMS: 45000,
        });

        cachedConnection = conn;
        // Wait for connection to be fully established
        if (conn.connection.readyState !== 1) {
            await conn.connection.asPromise();
        }
        logger.info('MongoDB connected successfully');
        return conn;

    } catch (error) {
        logger.error('MongoDB connection failed', error);
        // Do not exit process in serverless, just throw
        throw error;
    }
};
