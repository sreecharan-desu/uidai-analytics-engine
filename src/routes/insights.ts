import { Router } from 'express';
import { validateApiKey } from '../middleware/auth';
import { getInsights } from '../controllers/insightsController';

const router = Router();

router.use(validateApiKey);

router.post('/query', getInsights);

export default router;
