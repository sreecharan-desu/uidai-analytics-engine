import mongoose from 'mongoose';
import { config } from '../config';

const clearDB = async () => {
    try {
        await mongoose.connect(config.mongoUri, { dbName: config.dbName });
        console.log('Connected to DB. Clearing collections...');

        const collections = ['aadhaar_enrolment_raw', 'aadhaar_demographic_update_raw', 'aadhaar_biometric_update_raw'];
        
        for (const col of collections) {
            try {
                await mongoose.connection.db?.collection(col).drop();
                console.log(`Dropped ${col}`);
            } catch (e: any) {
                if (e.code === 26) {
                    console.log(`Collection ${col} not found (already empty)`);
                } else {
                    console.error(`Error dropping ${col}:`, e);
                }
            }
        }
        
        console.log('Database cleared.');

    } catch (err) {
        console.error(err);
    } finally {
        await mongoose.disconnect();
    }
};

clearDB();
