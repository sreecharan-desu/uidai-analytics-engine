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

    // Hardcoded fields per dataset as requested by the notebook
    let defaultSelect: string[] = [];
    if (dataset) {
      switch (dataset.toLowerCase()) {
        case 'enrolment':
          defaultSelect = ["date", "state", "district", "pincode", "age_0_5", "age_5_17", "age_18_greater"];
          break;
        case 'demographic':
          // Placeholder fields for demographic until notebook is shared
          defaultSelect = ["date", "state", "district", "pincode"]; 
          break;
        case 'biometric':
          // Placeholder fields for biometric until notebook is shared
          defaultSelect = ["date", "state", "district", "pincode", "bio_age_5_17", "bio_age_17_"];
          break;
        default:
          defaultSelect = [];
      }
    }

    // Construct Cache Key based on inputs (including filters)
    // We sort keys to ensure stable cache key
    const filterKey = JSON.stringify(Object.keys(filters).sort().reduce((acc: any, key) => {
        acc[key] = filters[key];
        return acc;
    }, {}));
    
    // Also include top level dynamic filters from body
    // 'select' is no longer a reserved key for user input, as we ignore it
    const reservedKeys = ['dataset', 'limit', 'page', 'filters']; 
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

    // Use our internal defaultSelect for caching
    const selectFieldsId = defaultSelect.length > 0 ? defaultSelect.sort().join(',') : 'all';

    const cacheKey = `insight:${dataset}:${page}:${limit}:${selectFieldsId}:${stableDynamicKey}`;

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

    // Normalize 'state' and 'district' filters to Title Case to improve hit rate
    const toTitleCase = (str: string) => {
        return str.toLowerCase().split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
    };

    Object.keys(dynamicFilters).forEach((key: string) => {
        let value = dynamicFilters[key];
        
        // Apply normalization for specific fields
        if (key.toLowerCase() === 'state' || key.toLowerCase() === 'district') {
            if (typeof value === 'string') {
                value = toTitleCase(value);
            }
        }
        
        apiParams[`filters[${key}]`] = value;
    });

    logger.info(`Fetching from Data.gov.in: ${dataset}`, { apiParams });

    const response = await axios.get(apiUrl, { params: apiParams });
    const data = response.data;

    if (data.status !== 'ok') {
        throw new Error(data.message || 'Error fetching from Source API');
    }

    const fields = data.field ? data.field.map((f: any) => f.id) : [];
    
    // Apply INTERNAL field selection
    let records = data.records;
    if (defaultSelect.length > 0) {
        records = records.map((record: any) => {
            const filtered: any = {};
            defaultSelect.forEach((f: string) => {
                if (record[f] !== undefined) filtered[f] = record[f];
            });
            // If the record has none of the selected fields, return it as is or empty? 
            // Usually we want to return the subset.
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
