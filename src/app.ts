import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import insightsRoutes from './routes/insights';
import logger from './utils/logger';
import path from 'path';
import datasetsRoutes from './routes/datasets';

const app = express();

// Security and Performance Middleware
app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        ...helmet.contentSecurityPolicy.getDefaultDirectives(),
        'script-src': ["'self'", "'unsafe-inline'", 'cdnjs.cloudflare.com'],
        'style-src': ["'self'", "'unsafe-inline'", 'fonts.googleapis.com', 'cdnjs.cloudflare.com'],
        'font-src': ["'self'", 'fonts.gstatic.com'],
        'img-src': ["'self'", 'data:', 'https:'],
      },
    },
  }),
);
app.use(cors());
app.use(compression());
app.use(express.json());

// Routes
app.use('/api/insights', insightsRoutes);
app.use('/api/datasets', datasetsRoutes);

import analyticsRoutes from './routes/analytics';
app.use('/api/analytics', analyticsRoutes);

// Serve static files from public directory
app.use(express.static(path.join(__dirname, '../public')));

// Documentation Page (Custom Swagger)
app.get('/docs', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/docs.html'));
});

// Root Landing Page - Simplest JSON
app.get('/', (req, res) => {
  res.json({
    status: 'healthy',
    message: 'UIDAI Insights API',
    docs: 'https://uidai.sreecharandesu.in/docs',
  });
});

// Global error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error('Unhandled Error', err);
  res.status(500).json({ error: 'Internal Server Error' });
});

export default app;
