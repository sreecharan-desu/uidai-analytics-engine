import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import ingestRoutes from './routes/ingest';
import insightsRoutes from './routes/insights';
import { connectDB } from './db';
import logger from './utils/logger';

const app = express();

app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json());

// Routes
app.use('/api', ingestRoutes);
app.use('/api/insights', insightsRoutes);

// Swagger Documentation
import swaggerUi from 'swagger-ui-express';
import { swaggerDocument } from './swagger';
import path from 'path';

// Serve static files
app.use(express.static(path.join(__dirname, '../public')));

const CSS_URL = "https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.1.0/swagger-ui.min.css";

app.use('/docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument, {
  customCssUrl: CSS_URL
}));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
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

// Middleware to ensure DB is connected before handling requests
app.use(async (req, res, next) => {
    try {
        await connectDB();
    } catch (error) {
        logger.warn('DB Connection failed in middleware. Proceeding without DB.', error);
        // Do NOT fail the request. Proceed. The route handler handles missing DB.
    }
    next();
});

export default app;
