import { Router, Request, Response } from 'express';
import axios from 'axios';
import { EnrolmentModel, DemographicModel, BiometricModel } from '../models/AadhaarData';
import logger from '../utils/logger';
import { config } from '../config';

const router = Router();

interface QueryFilters {
  [key: string]: any;
}

const getInsights = async (req: Request, res: Response) => {
  try {
    const { dataset, filters = {}, limit = 100, page = 1 } = req.body;

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

    // 1. Check Cache (MongoDB) logic could be complex with partial filters. 
    // Simplified Cache Strategy:
    // We treat MongoDB as a "Recent Search Cache". 
    // But honestly, matching exact API query to Mongo query is hard if we only cache "some" data.
    // "Use it for caching" -> cache the RESULT of the specific query? 
    // Or cache the records we find?
    // Let's implement: Query Mongo first. If empty (or not enough data?), query API -> Store -> Return.
    // But User said "Fetch directly from resource... use DB for caching".
    // So priority is API.
    
    // Construct API URL
    const offset = (Number(page) - 1) * Number(limit);
    const apiUrl = `https://api.data.gov.in/resource/${resourceId}`;
    
    // Convert filters to API format if needed, or pass them?
    // data.gov.in supports filters like `filters[field]=value`
    const apiParams: any = {
      'api-key': config.dataGovApiKey,
      format: 'json',
      limit: limit,
      offset: offset,
    };

    // basic mapping of filters to api params
    Object.keys(filters).forEach(key => {
        apiParams[`filters[${key}]`] = filters[key];
    });

    logger.info(`Fetching from Data.gov.in: ${dataset}`, { apiParams });

    // 2. Fetch from API
    const response = await axios.get(apiUrl, { params: apiParams });
    const data = response.data;

    if (data.status !== 'ok') {
        throw new Error(data.message || 'Error fetching from Source API');
    }

    const records = data.records;

    // 3. Cache in Mongo (Fire and Forget or Await?)
    // We update/upsert these records into Mongo so next time we "could" use them, 
    // OR just to have them. User said "use it for caching".
    // Doing it async to not block response.
    if (records && records.length > 0) {
        const bulkOps = records.map((record: any) => {
            // Generate simple hash or use composite unique keys if available
            // We use the same hashing logic as before or strict content.
            // Re-implementing simplified hash here for import-independent
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
        
        // Background cache update
        model.bulkWrite(bulkOps).catch(err => logger.error('Cache update failed', err));
    }

    return res.status(200).json({
      meta: {
        dataset,
        total: data.total, // Total from API
        page,
        limit,
        from_cache: false 
      },
      data: records
    });

  } catch (error: any) {
    logger.error('Error processing insights query', error);
    
    // Fallback? If API fails, try checking DB?
    // For now, just return error as "Direct from resource" was requested.
    return res.status(500).json({ error: 'Internal Server Error', details: error.message });
  }
};

router.post('/query', getInsights);

export default router;
