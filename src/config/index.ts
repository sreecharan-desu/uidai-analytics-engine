import dotenv from 'dotenv';
dotenv.config();

const requiredEnvVars = [
  'DATA_GOV_API_KEY',
  'MONGODB_URI',
  'DB_NAME',
  'CRON_SECRET',
  'CLIENT_API_KEY',
];

const missingVars = requiredEnvVars.filter((key) => !process.env[key]);

if (missingVars.length > 0) {
  throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
}

export const config = {
  dataGovApiKey: process.env.DATA_GOV_API_KEY!,
  mongoUri: process.env.MONGODB_URI!,
  dbName: process.env.DB_NAME!,
  cronSecret: process.env.CRON_SECRET!,
  ingestionBatchSize: parseInt(process.env.INGESTION_BATCH_SIZE || '1000', 10),
  ingestionTimeoutMs: parseInt(process.env.INGESTION_TIMEOUT_MS || '300000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  resources: {
    enrolment: 'ecd49b12-3084-4521-8f7e-ca8bf72069ba',
    demographic: '19eac040-0b94-49fa-b239-4f2fd8677d53',
    biometric: '65454dab-1517-40a3-ac1d-47d4dfe6891c',
  },
  clientApiKey: process.env.CLIENT_API_KEY!,
};
