import { Router, Request, Response } from 'express';
import axios from 'axios';
import { Redis } from '@upstash/redis';
import logger from '../utils/logger';
import { config } from '../config';
import { validateApiKey } from '../middleware/auth';

const router = Router();
const redis = new Redis({
  url: config.upstashRedisRestUrl,
  token: config.upstashRedisRestToken,
});

router.use(validateApiKey);

interface QueryFilters {
  [key: string]: any;
}

const getInsights = async (req: Request, res: Response) => {
  try {
    let { dataset, filters = {}, limit = 100, page = 1 } = req.body;
    
    // Sanitize inputs
    limit = Math.min(Math.max(parseInt(limit.toString()) || 100, 1), 1000);
    page = Math.max(parseInt(page.toString()) || 1, 1);

    // Construct Cache Key based on inputs (including filters)
    // We sort keys to ensure stable cache key
    const filterKey = JSON.stringify(Object.keys(filters).sort().reduce((acc: any, key) => {
        acc[key] = filters[key];
        return acc;
    }, {}));
    
    // Also include top level dynamic filters from body
    const reservedKeys = ['dataset', 'limit', 'page', 'filters', 'select'];
    const dynamicFilters: any = { ...filters };
    Object.keys(req.body).forEach(key => {
        if (!reservedKeys.includes(key)) {
            dynamicFilters[key] = req.body[key];
        }
    });
    
    const stableDynamicKey = JSON.stringify(Object.keys(dynamicFilters).sort().reduce((acc: any, key) => {
        acc[key] = dynamicFilters[key];
        return acc;
    }, {}));

    const selectFields = req.body.select && Array.isArray(req.body.select) 
        ? [...req.body.select].sort().join(',') 
        : 'all';

    const cacheKey = `insight:${dataset}:${page}:${limit}:${selectFields}:${stableDynamicKey}`;

    // 1. Check Redis (L2 Cache)
    try {
        const cachedResult: any = await redis.get(cacheKey);
        if (cachedResult) {
            logger.info(`Serving from Redis Cache: ${cacheKey}`);
            return res.status(200).json({ ...cachedResult, meta: { ...cachedResult.meta, source: 'cache', from_cache: true } });
        }
    } catch (redisErr) {
        logger.warn('Redis Cache Error', redisErr);
    }

    if (!dataset) {
      return res.status(400).json({ error: 'Dataset type is required (enrolment, demographic, or biometric)' });
    }

    let resourceId;
    switch (dataset.toLowerCase()) {
      case 'enrolment':
        resourceId = config.resources.enrolment;
        break;
      case 'demographic':
        resourceId = config.resources.demographic;
        break;
      case 'biometric':
        resourceId = config.resources.biometric;
        break;
      default:
        return res.status(400).json({ error: 'Invalid dataset type.' });
    }
    
    
    const offset = (Number(page) - 1) * Number(limit);

    // 2. Fetch from External API (L3 Source)
    const apiUrl = `https://api.data.gov.in/resource/${resourceId}`;
    const apiParams: any = {
      'api-key': config.dataGovApiKey,
      format: 'json',
      limit: limit,
      offset: offset,
    };

    Object.keys(dynamicFilters).forEach(key => {
        apiParams[`filters[${key}]`] = dynamicFilters[key];
    });

    logger.info(`Fetching from Data.gov.in: ${dataset}`, { apiParams });

    const response = await axios.get(apiUrl, { params: apiParams });
    const data = response.data;

    if (data.status !== 'ok') {
        throw new Error(data.message || 'Error fetching from Source API');
    }

    const fields = data.field ? data.field.map((f: any) => f.id) : [];
    
    // Apply field selection if provided
    let records = data.records;
    if (req.body.select && Array.isArray(req.body.select)) {
        records = records.map((record: any) => {
            const filtered: any = {};
            req.body.select.forEach((f: string) => {
                if (record[f] !== undefined) filtered[f] = record[f];
            });
            return filtered;
        });
    }

    const responsePayload = {
      meta: {
        dataset,
        total: data.total,
        page,
        limit,
        from_cache: false,
        fields: fields,
        source: 'api'
      },
      data: records
    };
    
    // 3. Store in Redis
    try {
        await redis.set(cacheKey, responsePayload, { ex: 3600 * 24 }); // Cache for 24 hours
    } catch (err) {
        logger.warn('Failed to set Redis cache', err);
    }

    return res.status(200).json(responsePayload);

  } catch (error: any) {
    logger.error('Error processing insights query', error);
    return res.status(500).json({ error: 'Internal Server Error', details: error.message });
  }
};

router.post('/query', getInsights);

export default router;
