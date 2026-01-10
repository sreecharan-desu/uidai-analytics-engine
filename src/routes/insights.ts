import { Router, Request, Response } from 'express';
import axios from 'axios';
import NodeCache from 'node-cache';
import { EnrolmentModel, DemographicModel, BiometricModel } from '../models/AadhaarData';
import logger from '../utils/logger';
import { config } from '../config';
import { validateApiKey } from '../middleware/auth';

const router = Router();
const cache = new NodeCache({ stdTTL: 300, checkperiod: 320 }); // 5 Minutes Cache

router.use(validateApiKey);

interface QueryFilters {
  [key: string]: any;
}

const getInsights = async (req: Request, res: Response) => {
  try {
    const { dataset, filters = {}, limit = 100, page = 1 } = req.body;

    // 0. Check In-Memory Cache
    const cacheKey = `${dataset}_${JSON.stringify(filters)}_${limit}_${page}`;
    const cachedData = cache.get(cacheKey);

    if (cachedData) {
        logger.info(`Serving from Cache: ${cacheKey}`);
        return res.status(200).json(cachedData);
    }

    if (!dataset) {
      return res.status(400).json({ error: 'Dataset type is required (enrolment, demographic, or biometric)' });
    }

    let resourceId;
    let model;
    switch (dataset.toLowerCase()) {
      case 'enrolment':
        resourceId = config.resources.enrolment;
        model = EnrolmentModel;
        break;
      case 'demographic':
        resourceId = config.resources.demographic;
        model = DemographicModel;
        break;
      case 'biometric':
        resourceId = config.resources.biometric;
        model = BiometricModel;
        break;
      default:
        return res.status(400).json({ error: 'Invalid dataset type.' });
    }
    
    // Construct API URL
    const offset = (Number(page) - 1) * Number(limit);
    const apiUrl = `https://api.data.gov.in/resource/${resourceId}`;
    
    const apiParams: any = {
      'api-key': config.dataGovApiKey,
      format: 'json',
      limit: limit,
      offset: offset,
    };

    // Exclude reserved keys to treat everything else as a potential filter or API param
    const reservedKeys = ['dataset', 'limit', 'page', 'filters'];
    const dynamicFilters = { ...filters };

    // Merge any top-level keys that aren't reserved into dynamicFilters
    // This allows users to send { dataset: '...', "State": "Telangana" } directly
    Object.keys(req.body).forEach(key => {
        if (!reservedKeys.includes(key)) {
            dynamicFilters[key] = req.body[key];
        }
    });

    Object.keys(dynamicFilters).forEach(key => {
        // Data.gov.in allows sorting and other params too, but usually filters[field]
        // If the key specifically starts with 'sort_' or matches known api params, handle accordingly?
        // For now, assume everything else is a filter for a field.
        apiParams[`filters[${key}]`] = dynamicFilters[key];
    });

    logger.info(`Fetching from Data.gov.in: ${dataset}`, { apiParams });

    // 2. Fetch from API
    const response = await axios.get(apiUrl, { params: apiParams });
    const data = response.data;

    if (data.status !== 'ok') {
        throw new Error(data.message || 'Error fetching from Source API');
    }

    const records = data.records;

    // 3. Cache in Mongo (Background) & In-Memory (Foreground)
    
    // Save to In-Memory Cache
    const responsePayload = {
      meta: {
        dataset,
        total: data.total,
        page,
        limit,
        from_cache: false 
      },
      data: records
    };
    
    // Set cache with flag true for next time
    cache.set(cacheKey, { ...responsePayload, meta: { ...responsePayload.meta, from_cache: true } });

    // Background Mongo Upsert
    if (records && records.length > 0) {
        const bulkOps = records.map((record: any) => {
            const content = JSON.stringify(Object.keys(record).sort().reduce((acc:any,k)=> {acc[k]=record[k]; return acc}, {}));
            const hash = require('crypto').createHash('md5').update(content).digest('hex');
            
            return {
                updateOne: {
                    filter: { resource_id: resourceId, record_hash: hash },
                    update: { $setOnInsert: { 
                        ...record, 
                        resource_id: resourceId, 
                        ingestion_timestamp: new Date(), 
                        source: 'data.gov.in',
                        record_hash: hash
                    }},
                    upsert: true
                }
            };
        });
        model.bulkWrite(bulkOps).catch(err => logger.error('Cache update failed', err));
    }

    return res.status(200).json(responsePayload);

  } catch (error: any) {
    logger.error('Error processing insights query', error);
    return res.status(500).json({ error: 'Internal Server Error', details: error.message });
  }
};

router.post('/query', getInsights);

export default router;
