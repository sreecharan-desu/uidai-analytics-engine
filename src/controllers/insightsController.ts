import { Request, Response } from 'express';
import { fetchInsights } from '../services/insightsService';
import logger from '../utils/logger';

export const getInsights = async (req: Request, res: Response) => {
  try {
    let { dataset, filters = {}, limit = 100, page = 1 } = req.body;

    if (!dataset) {
      return res.status(400).json({ error: 'Dataset type is required (enrolment, demographic, or biometric)' });
    }

    // Sanitize inputs
    const limitNum = Math.min(Math.max(parseInt(limit.toString()) || 100, 1), 1000);
    const pageNum = Math.max(parseInt(page.toString()) || 1, 1);

    // Merge dynamic filters from top-level body
    const reservedKeys = ['dataset', 'limit', 'page', 'filters'];
    const mergedFilters: any = { ...filters };
    Object.keys(req.body).forEach((key) => {
      if (!reservedKeys.includes(key)) {
        mergedFilters[key] = req.body[key];
      }
    });

    const result = await fetchInsights({
      dataset,
      filters: mergedFilters,
      limit: limitNum,
      page: pageNum,
    });

    return res.status(200).json(result);
  } catch (error: any) {
    logger.error('Error processing insights query', error);
    if (error.message === 'Invalid dataset type.') {
      return res.status(400).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Internal Server Error', details: error.message });
  }
};
