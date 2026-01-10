import dotenv from 'dotenv';
dotenv.config();

const requiredEnvVars = [
  'DATA_GOV_API_KEY',
  'CLIENT_API_KEY',
  'UPSTASH_REDIS_REST_URL',
  'UPSTASH_REDIS_REST_TOKEN'
];

const missingVars = requiredEnvVars.filter((key) => !process.env[key]);

if (missingVars.length > 0) {
  throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
}

export const config = {
  dataGovApiKey: process.env.DATA_GOV_API_KEY!,
  clientApiKey: process.env.CLIENT_API_KEY!,
  upstashRedisRestUrl: process.env.UPSTASH_REDIS_REST_URL!,
  upstashRedisRestToken: process.env.UPSTASH_REDIS_REST_TOKEN!,
  nodeEnv: process.env.NODE_ENV || 'development',
  resources: {
    enrolment: 'ecd49b12-3084-4521-8f7e-ca8bf72069ba',
    demographic: '19eac040-0b94-49fa-b239-4f2fd8677d53',
    biometric: '65454dab-1517-40a3-ac1d-47d4dfe6891c',
  },
};
