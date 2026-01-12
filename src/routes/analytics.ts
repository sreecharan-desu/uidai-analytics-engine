import { Router } from 'express';
import { getAggregateInsights } from '../services/aggregationService';
import logger from '../utils/logger';
import Papa from 'papaparse';

const router = Router();

// GET /api/analytics/:dataset?year=2025&format=csv
router.get('/:dataset', async (req, res) => {
  try {
    const { dataset } = req.params;
    const { year, format } = req.query;

    const validDatasets = ['biometric', 'enrolment', 'demographic'];
    if (!validDatasets.includes(dataset)) {
      return res.status(400).json({ error: 'Invalid dataset' });
    }

    const data = await getAggregateInsights(dataset, year as string);

    if (format === 'csv') {
      // Flatten logic for Looker Studio
      // We'll create a "Category, Label, Count" schema for maximum flexibility
      // OR simply "State, Age Group, Count" (but they are separate aggregates in our service).

      // Better: Return "State Wise" first as it's the main map visual
      // Looker typically needs one table. Since we have TWO insights (State vs Age),
      // maybe we default to STATE here, or allow ?view=age

      const view = req.query.view || 'state';
      let rows: any[] = [];

      if (view === 'state') {
        rows = Object.entries(data.by_state).map(([state, count]) => ({
          State: state,
          Updates: count,
        }));
      } else if (view === 'age') {
        rows = Object.entries(data.by_age_group).map(([age, count]) => ({
          AgeGroup: age,
          Updates: count,
        }));
      }

      const csv = Papa.unparse(rows);
      res.header('Content-Type', 'text/csv');
      res.attachment(`${dataset}_${view}_${year || 'all'}.csv`);
      return res.send(csv);
    }

    res.json({
      dataset,
      year: year || 'all',
      generated_at: new Date().toISOString(),
      data,
    });
  } catch (err: any) {
    logger.error('Analytics Error', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
