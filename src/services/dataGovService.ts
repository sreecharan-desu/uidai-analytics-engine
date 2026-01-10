import axios from 'axios';
import { Model } from 'mongoose';
import crypto from 'crypto';
import { config } from '../config';
import logger from '../utils/logger';

interface DataGovResponse {
  index_name: string;
  title: string;
  desc: string;
  org_type: string;
  org: string[];
  sector: string[];
  source: string;
  catalog_uuid: string;
  visualizable: string;
  created: number;
  updated: number;
  created_date: string;
  updated_date: string;
  target_bucket: {
    index: string;
    type: string;
    field: string;
  };
  field: Array<{
    id: string;
    name: string;
    type: string;
  }>;
  message: string;
  version: string;
  status: string;
  total: number;
  count: number;
  limit: string;
  offset: string;
  records: Array<Record<string, any>>;
}

export class DataGovService {
  private generateRecordHash(record: any): string {
    // stable stringify or just simplistic JSON.stringify (keys order might vary but usually consistent from same source)
    // For specific API, keys usually come in same order.
    // To be safer, sort keys.
    const sortedRecord = Object.keys(record).sort().reduce((acc: any, key) => {
      acc[key] = record[key];
      return acc;
    }, {});
    
    return crypto
      .createHash('md5')
      .update(JSON.stringify(sortedRecord))
      .digest('hex');
  }

  async fetchAndIngest(resourceId: string, model: Model<any>): Promise<{ status: string; count: number; error?: string }> {
    let offset = 0;
    const limit = config.ingestionBatchSize;
    let totalIngested = 0;
    let hasMore = true;

    logger.info(`Starting ingestion for resource: ${resourceId}`);

    try {
      while (hasMore) {
        // Check timeout? (Basic check)
        
        const url = `https://api.data.gov.in/resource/${resourceId}`;
        const params = {
          'api-key': config.dataGovApiKey,
          format: 'json',
          limit: limit,
          offset: offset,
        };

        logger.debug(`Fetching batch: offset=${offset}, limit=${limit}`);
        
        const response = await axios.get<DataGovResponse>(url, { params });
        const data = response.data;

        if (data.status !== 'ok') {
            // Some responses might not be "ok" but have data? standard data.gov.in response has status.
            throw new Error(`API Error: ${data.message || 'Unknown error'}`);
        }

        const records = data.records;
        if (!records || records.length === 0) {
          hasMore = false;
          break;
        }

        // Prepare bulk operations
        const bulkOps = records.map((record) => {
          const recordHash = this.generateRecordHash(record);
          
          const ingestRecord = {
            ...record,
            resource_id: resourceId,
            ingestion_timestamp: new Date(),
            source: 'data.gov.in',
            record_hash: recordHash // Helpful for debugging, though we use it in filter
          };

          return {
            updateOne: {
              filter: { resource_id: resourceId, record_hash: recordHash }, // unique identify by content
              update: { $setOnInsert: ingestRecord },
              upsert: true,
            },
          };
        });

        if (bulkOps.length > 0) {
          await model.bulkWrite(bulkOps);
          totalIngested += bulkOps.length; // Count processed
        }

        offset += limit;
        
        // Safety break if fetched less than limit
        if (records.length < limit) {
          hasMore = false;
        }
        
        // Total available in response
        if (data.total && offset >= data.total) {
            hasMore = false;
        }
      }

      logger.info(`Completed ingestion for ${resourceId}. Total processed: ${totalIngested}`);
      return { status: 'success', count: totalIngested };

    } catch (error: any) {
      logger.error(`Error ingesting ${resourceId}:`, error);
      return { status: 'error', count: totalIngested, error: error.message };
    }
  }
}
