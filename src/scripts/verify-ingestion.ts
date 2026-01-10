import mongoose from 'mongoose';
import { config } from '../config';
import { EnrolmentModel, DemographicModel, BiometricModel } from '../models/AadhaarData';

const verify = async () => {
    try {
        await mongoose.connect(config.mongoUri, { dbName: config.dbName });
        console.log('Connected to DB');

        const enrolmentCount = await EnrolmentModel.countDocuments();
        const demographicCount = await DemographicModel.countDocuments();
        const biometricCount = await BiometricModel.countDocuments();

        console.log('--- Verification Results ---');
        console.log(`Enrolment Records: ${enrolmentCount}`);
        console.log(`Demographic Records: ${demographicCount}`);
        console.log(`Biometric Records: ${biometricCount}`);

    } catch (err) {
        console.error(err);
    } finally {
        await mongoose.disconnect();
    }
};

verify();
