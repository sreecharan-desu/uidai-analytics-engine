import { Router } from 'express';
import { ingestMonthlyData } from '../controllers/ingestController';

const router = Router();

router.post('/ingest/monthly', ingestMonthlyData);

export default router;
