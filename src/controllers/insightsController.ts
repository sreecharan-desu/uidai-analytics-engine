import { Request, Response } from 'express';
import { EnrolmentModel, DemographicModel, BiometricModel } from '../models/AadhaarData';
import logger from '../utils/logger';

interface QueryFilters {
  [key: string]: any;
}

export const getInsights = async (req: Request, res: Response) => {
  try {
    const { dataset, filters = {}, limit = 100, page = 1 } = req.body;

    if (!dataset) {
      return res.status(400).json({ error: 'Dataset type is required (enrolment, demographic, or biometric)' });
    }

    let model;
    switch (dataset.toLowerCase()) {
      case 'enrolment':
        model = EnrolmentModel;
        break;
      case 'demographic':
        model = DemographicModel;
        break;
      case 'biometric':
        model = BiometricModel;
        break;
      default:
        return res.status(400).json({ error: 'Invalid dataset type. Must be one of: enrolment, demographic, biometric' });
    }

    // Convert page/limit to number
    const pageNum = Math.max(1, Number(page));
    const limitNum = Math.min(1000, Math.max(1, Number(limit))); // Hard cap at 1000 for performance
    const skip = (pageNum - 1) * limitNum;

    logger.info(`Insights query received for ${dataset}`, { filters, page: pageNum, limit: limitNum });

    // Build MongoDB query from filters
    // CAUTION: Direct user input needs sanitization in prod if filters are complex. 
    // Generally, strictly mapped filters are safer. 
    // For now assuming filters keys match schema fields EXACTLY.
    // We should exclude our internal metadata fields from accidental matching if sensitive, but here they are harmless.
    const query: QueryFilters = { ...filters };

    // Execute Query
    const results = await model.find(query)
      .skip(skip)
      .limit(limitNum)
      .lean(); // Faster query without mongoose document overhead

    const total = await model.countDocuments(query);

    return res.status(200).json({
      meta: {
        dataset,
        total,
        page: pageNum,
        limit: limitNum,
        founded: results.length
      },
      data: results
    });

  } catch (error: any) {
    logger.error('Error processing insights query', error);
    return res.status(500).json({ error: 'Internal Server Error', details: error.message });
  }
};
