import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';

import insightsRoutes from './routes/insights';

import logger from './utils/logger';

const app = express();

app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json());

// Routes

app.use('/api/insights', insightsRoutes);

// Swagger Documentation
import swaggerUi from 'swagger-ui-express';
import { swaggerDocument } from './swagger';
import path from 'path';

// Serve static files
app.use(express.static(path.join(__dirname, '../public')));

const SWAGGER_OPTS = {
  customCssUrl: "https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.min.css",
  customJs: [
    "https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-bundle.js",
    "https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-standalone-preset.js"
  ]
};

app.use('/docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument, SWAGGER_OPTS));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// Global error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error('Unhandled Error', err);
  res.status(500).json({ error: 'Internal Server Error' });
});



export default app;
