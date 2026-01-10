import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import ingestRoutes from './routes/ingest';
import insightsRoutes from './routes/insights';
import { connectDB } from './db';
import logger from './utils/logger';

const app = express();

app.use(helmet());
app.use(cors());
app.use(express.json());

// Routes
app.use('/api', ingestRoutes);
app.use('/api/insights', insightsRoutes);

// Swagger Documentation
import swaggerUi from 'swagger-ui-express';
import yaml from 'yamljs';
import path from 'path';

const swaggerDocument = yaml.load(path.join(__dirname, '../swagger.yaml'));
app.use('/docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

app.get('/', (req, res) => {
  res.redirect('/docs');
});

// Global error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error('Unhandled Error', err);
  res.status(500).json({ error: 'Internal Server Error' });
});

// Connect to DB immediately if not in Vercel (local dev server), 
// OR let the function handle it.
// For Vercel, we might want to connect inside the handler or middleware to ensure connection is alive.
// We'll wrap the route handling to ensure DB connection.

// Middleware to ensure DB is connected
app.use(async (req, res, next) => {
    // Basic check, usually connectDB handles idempotency
    await connectDB();
    next();
});

export default app;
