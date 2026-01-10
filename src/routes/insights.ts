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

    // 0. Check In-Memory Cache (L1 Cache) - < 5ms
    const cacheKey = `${dataset}_${JSON.stringify(req.body)}`;
    const cachedData = cache.get(cacheKey);

    if (cachedData) {
        logger.info(`Serving from L1 Cache (Memory): ${cacheKey}`);
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
    
    // Construct Filters
    const reservedKeys = ['dataset', 'limit', 'page', 'filters'];
    const dynamicFilters: any = { ...filters };

    Object.keys(req.body).forEach(key => {
        if (!reservedKeys.includes(key)) {
            dynamicFilters[key] = req.body[key];
        }
    });

    const offset = (Number(page) - 1) * Number(limit);

    // 1. Check MongoDB (L2 Cache) - ~50-100ms
    // We treat our Mongo DB as a persistent cache of the Gov API.
    const dbQuery = { ...dynamicFilters, resource_id: resourceId };
    
    // We try to find enough records to satisfy the request
    const dbDocs = await model.find(dbQuery)
      .skip(offset)
      .limit(limit)
      .lean();

    // If we have a FULL page of data, we trust it and return it.
    // If we have PARTIAL data (less than limit), we might be at the end OR we might have missing valid data.
    // To be safe/fast: If > 0, we return what we have? 
    // Better: If length == limit, safe cache hit. 
    // If length < limit, check valid total count? 
    // Simply: If we found ANY data, use it? No, might simply be stale or incomplete.
    // Strategy: If dbDocs.length === limit, we assume HIT.
    // Use API fallback if 0.
    
    if (dbDocs.length > 0) {
        // If we found data, let's verify if we need to fetch more from API
        // If we found exactly 'limit', we good. 
        // If we found less, it could be end of list.
        const totalInDb = await model.countDocuments(dbQuery);
        
        // If we have a full page OR we are at the end (total <= offset + length)
        // Then valid HIT.
        if (dbDocs.length === limit || totalInDb <= offset + dbDocs.length) {
            logger.info(`Serving from L2 Cache (MongoDB) for ${dataset}`);
            
            const responsePayload = {
              meta: {
                dataset,
                total: totalInDb, // This is DB total, might be less than API total but grows over time
                page,
                limit,
                from_cache: true,
                source: 'database'
              },
              data: dbDocs
            };
            
            // Populating L1
            cache.set(cacheKey, responsePayload);
            return res.status(200).json(responsePayload);
        }
    }

    // 2. Fetch from External API (L3 Source) - ~2s
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
        from_cache: false,
        fields: data.field ? data.field.map((f: any) => f.id) : [] 
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
