import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import insightsRoutes from './routes/insights';
import logger from './utils/logger';
import path from 'path';

const app = express();

// Security and Performance Middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      ...helmet.contentSecurityPolicy.getDefaultDirectives(),
      "script-src": ["'self'", "'unsafe-inline'"],
      "style-src": ["'self'", "'unsafe-inline'", "fonts.googleapis.com", "cdnjs.cloudflare.com"],
      "font-src": ["'self'", "fonts.gstatic.com"],
    },
  },
}));
app.use(cors());
app.use(compression());
app.use(express.json());

// Routes
app.use('/api/insights', insightsRoutes);

// Serve static files from public directory
app.use(express.static(path.join(__dirname, '../public')));

// Documentation Page (Custom Swagger)
app.get('/docs', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/docs.html'));
});

// Root Landing Page - Smart Handler (JSON + OG)
app.get('/', (req, res) => {
  const isBrowser = req.headers.accept && req.headers.accept.includes('text/html');
  const responseData = { message: "Hello from server", status: "healthy" };

  if (isBrowser) {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta property="og:type" content="website">
    <meta property="og:url" content="https://uidai.sreecharandesu.in/">
    <meta property="og:title" content="UIDAI Aadhaar Insights API">
    <meta property="og:description" content="A secure, high-performance API for fetching real-time Aadhaar insights.">
    <meta property="og:image" content="https://uidai.sreecharandesu.in/og-image.png">
    <meta name="twitter:card" content="summary_large_image">
    <title>UIDAI API</title>
    <style>
        body { background: #0f172a; color: #38bdf8; font-family: monospace; padding: 2rem; }
        pre { white-space: pre-wrap; word-wrap: break-word; }
    </style>
</head>
<body>
    <pre>${JSON.stringify(responseData, null, 2)}</pre>
</body>
</html>
    `);
  } else {
    res.json(responseData);
  }
});

// Global error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error('Unhandled Error', err);
  res.status(500).json({ error: 'Internal Server Error' });
});

export default app;
