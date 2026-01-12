import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import { redis } from '../utils/redis';
import logger from '../utils/logger';

// Standardized State Mapping (from Notebook)
const STATE_STANDARD_MAP: Record<string, string> = {
  'andhra pradesh': 'Andhra Pradesh',
  'arunachal pradesh': 'Arunachal Pradesh',
  assam: 'Assam',
  bihar: 'Bihar',
  chhattisgarh: 'Chhattisgarh',
  goa: 'Goa',
  gujarat: 'Gujarat',
  haryana: 'Haryana',
  'himachal pradesh': 'Himachal Pradesh',
  jharkhand: 'Jharkhand',
  karnataka: 'Karnataka',
  kerala: 'Kerala',
  'madhya pradesh': 'Madhya Pradesh',
  maharashtra: 'Maharashtra',
  manipur: 'Manipur',
  meghalaya: 'Meghalaya',
  mizoram: 'Mizoram',
  nagaland: 'Nagaland',
  odisha: 'Odisha',
  orissa: 'Odisha',
  punjab: 'Punjab',
  rajasthan: 'Rajasthan',
  sikkim: 'Sikkim',
  'tamil nadu': 'Tamil Nadu',
  tamilnadu: 'Tamil Nadu',
  telangana: 'Telangana',
  tripura: 'Tripura',
  'uttar pradesh': 'Uttar Pradesh',
  uttarakhand: 'Uttarakhand',
  uttaranchal: 'Uttarakhand',
  'west bengal': 'West Bengal',
  westbengal: 'West Bengal',
  'west bangal': 'West Bengal',

  // UTs
  'andaman and nicobar islands': 'Andaman and Nicobar Islands',
  'andaman nicobar islands': 'Andaman and Nicobar Islands',
  chandigarh: 'Chandigarh',
  'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'dadra nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
  'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'daman diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'dadra and nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
  'dadra & nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
  delhi: 'Delhi',
  'new delhi': 'Delhi',
  'jammu and kashmir': 'Jammu and Kashmir',
  'jammu kashmir': 'Jammu and Kashmir',
  'jammu & kashmir': 'Jammu and Kashmir',
  ladakh: 'Ladakh',
  lakshadweep: 'Lakshadweep',
  puducherry: 'Puducherry',
  pondicherry: 'Puducherry',
};

const normalizeState = (raw: string) => {
  if (!raw) return 'Unknown';
  // Logic from notebook: lower, strip, remove non-alphanumeric (except space), remove extra spaces
  const clean = raw
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return STATE_STANDARD_MAP[clean] || raw;
};

const normalizeDistrict = (raw: string) => {
  if (!raw) return 'Unknown';
  // Notebook logic for district: strip and title case
  return raw
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
};

interface AggregationResult {
  total_updates: number;
  by_state: Record<string, number>;
  by_district: Record<string, Record<string, number>>; // Added for granular analysis
  by_age_group: Record<string, number>;
  by_month: Record<string, number>;
}

export const getAggregateInsights = async (dataset: string, year?: string): Promise<AggregationResult> => {
  const cacheKey = `agg_v5:${dataset}:${year || 'all'}`; // Bump version to v5
  const cached = await redis.get(cacheKey);
  if (cached) return cached as AggregationResult;

  const isYearSpecific = year && year !== 'all';
  const fileName = isYearSpecific ? `${dataset}_${year}.csv` : `${dataset}_full.csv`;
  const subdir = isYearSpecific ? 'split_data' : '';
  const filePath = path.join(process.cwd(), 'public', 'datasets', subdir, fileName);

  if (!fs.existsSync(filePath)) {
    throw new Error(`Dataset file not found: ${fileName}`);
  }

  logger.info(`Processing aggregation for ${fileName}...`);

  return new Promise((resolve, reject) => {
    const result: AggregationResult = {
      total_updates: 0,
      by_state: {},
      by_district: {},
      by_age_group: {},
      by_month: {},
    };

    const stream = fs.createReadStream(filePath);

    Papa.parse(stream, {
      header: true,
      worker: false,
      step: (results) => {
        const row: any = results.data;
        if (!row.state) return;

        const state = normalizeState(row.state);
        const district = normalizeDistrict(row.district || 'Unknown');

        // Parse Month (Assuming date format DD-MM-YYYY or YYYY-MM-DD)
        let month = 'Unknown';
        if (row.date) {
          // Try to parse month manually for speed
          const parts = row.date.split(/[-/]/);
          if (parts.length === 3) {
            // Heuristic: If first part > 12, it's YYYY-MM-DD. If middle <= 12, it's month.
            // Standard data.gov.in format is usually DD-MM-YYYY
            if (parts[2].length === 4) {
              // DD-MM-YYYY
              month = parts[1];
            } else if (parts[0].length === 4) {
              // YYYY-MM-DD
              month = parts[1];
            }
          }
        }

        let count = 0;
        let ageCounts: Record<string, number> = {};

        if (dataset === 'biometric') {
          const c1 = parseInt(row.bio_age_5_17) || 0;
          const c2 = parseInt(row.bio_age_17_) || 0;
          count = c1 + c2;
          ageCounts['5-17'] = c1;
          ageCounts['18+'] = c2;
        } else if (dataset === 'enrolment') {
          const c1 = parseInt(row.age_0_5) || 0;
          const c2 = parseInt(row.age_5_17) || 0;
          const c3 = parseInt(row.age_18_greater) || 0;
          count = c1 + c2 + c3;
          ageCounts['0-5'] = c1;
          ageCounts['5-17'] = c2;
          ageCounts['18+'] = c3;
        } else if (dataset === 'demographic') {
          const c1 = parseInt(row.demo_age_5_17) || 0;
          const c2 = parseInt(row.demo_age_17_) || 0;
          count = c1 + c2;
          ageCounts['5-17'] = c1;
          ageCounts['18+'] = c2;
        }

        if (count === 0) return;

        result.total_updates += count;

        // State Aggregation
        result.by_state[state] = (result.by_state[state] || 0) + count;

        // District Aggregation
        if (!result.by_district[state]) {
          result.by_district[state] = {};
        }
        result.by_district[state][district] = (result.by_district[state][district] || 0) + count;

        // Age Aggregation
        Object.keys(ageCounts).forEach((g) => {
          result.by_age_group[g] = (result.by_age_group[g] || 0) + ageCounts[g];
        });

        // Month Aggregation
        if (month !== 'Unknown') {
          result.by_month[month] = (result.by_month[month] || 0) + count;
        }
      },
      complete: () => {
        redis.set(cacheKey, result, { ex: 86400 }).catch((err) => logger.error('Redis set error', err));
        resolve(result);
      },
      error: (err) => {
        reject(err);
      },
    });
  });
};
