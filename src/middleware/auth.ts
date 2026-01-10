import { Request, Response, NextFunction } from 'express';
import { config } from '../config';
import logger from '../utils/logger';

export const validateApiKey = (req: Request, res: Response, next: NextFunction) => {
  const apiKey = req.headers['x-api-key'] || req.headers['X-API-KEY'];

  if (!apiKey || apiKey !== config.clientApiKey) {
    logger.warn('Unauthorized API access attempt', {
      receivedHeader: !!apiKey,
      receivedLength: typeof apiKey === 'string' ? apiKey.length : 0,
      expectedLength: config.clientApiKey.length,
      match: apiKey === config.clientApiKey
    });
    return res.status(401).json({ error: 'Unauthorized: Invalid or missing API Key' });
  }

  next();
};
