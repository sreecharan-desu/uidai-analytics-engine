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
        // Enforce a hard timeout on the connection attempt
        const timeoutPromise = new Promise((_, reject) => 
            setTimeout(() => reject(new Error('MongoDB Connection Timeout')), 2000)
        );

        const connectionPromise = async () => {
             const conn = await mongoose.connect(config.mongoUri, {
                dbName: config.dbName,
                maxPoolSize: 10,
                serverSelectionTimeoutMS: 5000,
                socketTimeoutMS: 45000,
            });
            // Wait for readiness
            if (conn.connection.readyState !== 1) {
                await conn.connection.asPromise();
            }
            return conn;
        };

        const conn = await Promise.race([connectionPromise(), timeoutPromise]) as typeof mongoose;

        cachedConnection = conn;
        logger.info('MongoDB connected successfully');
        return conn;

    } catch (error) {
        logger.warn('MongoDB connection failed or timed out. Skipping DB.', error);
        // We throw so the caller knows DB is down and can skip L2 Cache logic
        throw error;
    }
};
