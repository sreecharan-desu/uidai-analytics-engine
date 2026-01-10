import { Router } from 'express';
import { getInsights } from '../controllers/insightsController';

const router = Router();

router.post('/query', getInsights);

export default router;
