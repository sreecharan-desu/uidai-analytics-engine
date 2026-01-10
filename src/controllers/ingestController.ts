import { Request, Response } from 'express';
import { DataGovService } from '../services/dataGovService';
import { EnrolmentModel, DemographicModel, BiometricModel } from '../models/AadhaarData';
import { config } from '../config';
import logger from '../utils/logger';

const service = new DataGovService();

export const ingestMonthlyData = async (req: Request, res: Response) => {
  const cronSecret = req.headers['x-cron-secret'];
  
  if (cronSecret !== config.cronSecret) {
    logger.warn('Unauthorized ingestion attempt');
    return res.status(401).json({ error: 'Unauthorized' });
  }

  logger.info('Received monthly ingestion trigger');

  const results: Record<string, any> = {};

  // Sequential execution
  // 1. Enrolment
  results.enrolment = await service.fetchAndIngest(config.resources.enrolment, EnrolmentModel);
  
  // 2. Demographic Update
  results.demographic = await service.fetchAndIngest(config.resources.demographic, DemographicModel);
  
  // 3. Biometric Update
  results.biometric = await service.fetchAndIngest(config.resources.biometric, BiometricModel);

  const hasFailure = Object.values(results).some(r => r.status === 'error');
  
  if (hasFailure) {
      // We return 200 with error details or 207 Multi-Status?
      // GitHub actions fails on non-2xx.
      // If critical failure, 500. But if partial, maybe 200 with warnings?
      // Requirement: "Returns status and record count per dataset"
      // User says: "One dataset failure must not block others"
      // I'll return 200 JSON with details, but maybe status 500 if ALL fail? 
      // Safe to return 200 and let the JSON body speak, UNLESS we want GitHub Action to fail.
      // "Fail on non-2xx response" -> Github Action config.
      // If I want the Action to fail if ANY failed, I should return non-2xx.
      // But if 2 succeed and 1 fail, do we want to mark the cron as failed? Yes, usually.
      return res.status(500).json({ message: 'Partial or full failure', results });
  }

  return res.status(200).json({ message: 'Ingestion completed', results });
};
