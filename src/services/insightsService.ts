import axios from 'axios';
import logger from '../utils/logger';
import { config } from '../config';
import { redis } from '../utils/redis';
import { toTitleCase } from '../utils/transform';

interface FetchInsightsParams {
  dataset: string;
  filters: Record<string, any>;
  limit: number;
  page: number;
}

export const fetchInsights = async ({ dataset, filters, limit, page }: FetchInsightsParams) => {
  // Hardcoded fields per dataset
  let defaultSelect: string[] = [];
  if (dataset) {
    switch (dataset.toLowerCase()) {
      case 'enrolment':
        defaultSelect = ['date', 'state', 'district', 'pincode', 'age_0_5', 'age_5_17', 'age_18_greater'];
        break;
      case 'demographic':
        defaultSelect = ['date', 'state', 'district', 'pincode', 'demo_age_5_17', 'demo_age_17_'];
        break;
      case 'biometric':
        defaultSelect = ['date', 'state', 'district', 'pincode', 'bio_age_5_17', 'bio_age_17_'];
        break;
      default:
        defaultSelect = [];
    }
  }

  // Construct Cache Key
  const stableDynamicKey = JSON.stringify(
    Object.keys(filters)
      .sort()
      .reduce((acc: any, key) => {
        acc[key] = filters[key];
        return acc;
      }, {}),
  );

  const selectFieldsId = defaultSelect.length > 0 ? defaultSelect.sort().join(',') : 'all';
  const cacheKey = `insight:${dataset}:${page}:${limit}:${selectFieldsId}:${stableDynamicKey}`;

  // 1. Check Redis (L2 Cache)
  try {
    const cachedResult: any = await redis.get(cacheKey);
    if (cachedResult) {
      logger.info(`Serving from Redis Cache: ${cacheKey}`);
      return { ...cachedResult, meta: { ...cachedResult.meta, source: 'cache', from_cache: true } };
    }
  } catch (redisErr) {
    logger.warn('Redis Cache Error', redisErr);
  }

  // Map Resource ID
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
      throw new Error('Invalid dataset type.');
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

  // Filter Normalization
  Object.keys(filters).forEach((key: string) => {
    let value = filters[key];
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
      source: 'api',
    },
    data: records,
  };

  // 3. Store in Redis
  try {
    await redis.set(cacheKey, responsePayload, { ex: 3600 * 24 }); // Cache for 24 hours
  } catch (err) {
    logger.warn('Failed to set Redis cache', err);
  }

  return responsePayload;
};
