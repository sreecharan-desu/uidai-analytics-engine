
import { Router } from 'express';
import { getAggregateInsights } from '../services/aggregationService';
import logger from '../utils/logger';

const router = Router();

// GET /api/analytics/:dataset?year=2025
router.get('/:dataset', async (req, res) => {
    try {
        const { dataset } = req.params;
        const { year } = req.query;
        
        const validDatasets = ['biometric', 'enrolment', 'demographic'];
        if (!validDatasets.includes(dataset)) {
            return res.status(400).json({ error: 'Invalid dataset' });
        }

        const data = await getAggregateInsights(dataset, year as string);
        
        res.json({
            dataset,
            year: year || 'all',
            generated_at: new Date().toISOString(),
            data
        });

    } catch (err: any) {
        logger.error('Analytics Error', err);
        res.status(500).json({ error: err.message });
    }
});

export default router;
