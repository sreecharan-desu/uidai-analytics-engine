
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import { redis } from '../utils/redis';
import logger from '../utils/logger';

// Standardized State Mapping (from Notebook)
const STATE_STANDARD_MAP: Record<string, string> = {
    'andhra pradesh': 'Andhra Pradesh',
    'arunachal pradesh': 'Arunachal Pradesh',
    'assam': 'Assam',
    'bihar': 'Bihar',
    'chhattisgarh': 'Chhattisgarh',
    'chhatisgarh': 'Chhattisgarh',
    'goa': 'Goa',
    'gujarat': 'Gujarat',
    'haryana': 'Haryana',
    'himachal pradesh': 'Himachal Pradesh',
    'jharkhand': 'Jharkhand',
    'karnataka': 'Karnataka',
    'kerala': 'Kerala',
    'madhya pradesh': 'Madhya Pradesh',
    'maharashtra': 'Maharashtra',
    'manipur': 'Manipur',
    'meghalaya': 'Meghalaya',
    'mizoram': 'Mizoram',
    'nagaland': 'Nagaland',
    'odisha': 'Odisha',
    'orissa': 'Odisha',
    'punjab': 'Punjab',
    'rajasthan': 'Rajasthan',
    'sikkim': 'Sikkim',
    'tamil nadu': 'Tamil Nadu',
    'tamilnadu': 'Tamil Nadu',
    'telangana': 'Telangana',
    'tripura': 'Tripura',
    'uttar pradesh': 'Uttar Pradesh',
    'uttarakhand': 'Uttarakhand',
    'uttaranchal': 'Uttarakhand',
    'west bengal': 'West Bengal',
    'westbengal': 'West Bengal',
    'west bangal': 'West Bengal',
    'delhi': 'Delhi',
    'new delhi': 'Delhi',
    'jammu and kashmir': 'Jammu and Kashmir',
    'puducherry': 'Puducherry',
    'pondicherry': 'Puducherry',
    'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'dadra nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'andaman and nicobar islands': 'Andaman and Nicobar Islands',
    'andaman nicobar islands': 'Andaman and Nicobar Islands',
    'chandigarh': 'Chandigarh',
    'ladakh': 'Ladakh',
    'lakshadweep': 'Lakshadweep'
};

const normalizeState = (raw: string) => {
    if (!raw) return 'Unknown';
    const clean = raw.toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
    return STATE_STANDARD_MAP[clean] || raw; // Fallback to raw if not found
};

const getDataPath = (dataset: string) => path.join(process.cwd(), 'public', 'datasets', `${dataset}_full.csv`);

interface AggregationResult {
    total_updates: number;
    by_state: Record<string, number>;
    by_age_group: Record<string, number>;
}

// NOTE: This runs in-memory. For massive datasets, consider streaming or a DB.
// Since we have ~100MB files, node can handle this in memory (with ~1GB heap).
export const getAggregateInsights = async (dataset: string, year?: string): Promise<AggregationResult> => {
    const cacheKey = `agg_v1:${dataset}:${year || 'all'}`;
    const cached = await redis.get(cacheKey);
    if (cached) return cached as AggregationResult;

    const fileName = year ? `${dataset}_${year}.csv` : `${dataset}_full.csv`;
    // Look in split_data if year is provided, else root datasets
    const subdir = year ? 'split_data' : '';
    const filePath = path.join(process.cwd(), 'public', 'datasets', subdir, fileName);

    if (!fs.existsSync(filePath)) {
        throw new Error(`Dataset file not found: ${fileName}`);
    }

    logger.info(`Processing aggregation for ${fileName}...`);

    return new Promise((resolve, reject) => {
        const result: AggregationResult = {
            total_updates: 0,
            by_state: {},
            by_age_group: {}
        };

        Papa.parse(fs.createReadStream(filePath), {
            header: true,
            worker: false, // Node standard stream
            step: (results) => {
                const row: any = results.data;
                // row keys depends on dataset. 
                // Biometric: bio_age_5_17, bio_age_17_
                // Enrolment: age_0_5, age_5_17, age_18_greater
                // Demographic: demo_age_5_17, demo_age_17_

                if (!row.state) return;

                const state = normalizeState(row.state);
                
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

                // Age Aggregation
                Object.keys(ageCounts).forEach(g => {
                   result.by_age_group[g] = (result.by_age_group[g] || 0) + ageCounts[g];
                });
            },
            complete: () => {
                // Cache result for 24 hours
                redis.set(cacheKey, result, { ex: 86400 }).catch(err => logger.error("Redis set error", err));
                resolve(result);
            },
            error: (err) => {
                reject(err);
            }
        });
    });
};
