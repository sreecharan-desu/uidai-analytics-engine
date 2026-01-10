import { Request, Response, NextFunction } from 'express';
import { config } from '../config';
import logger from '../utils/logger';

export const validateApiKey = (req: Request, res: Response, next: NextFunction) => {
  const apiKey = req.headers['x-api-key'] || req.headers['X-API-KEY'];

  if (!apiKey || apiKey !== config.clientApiKey) {
    const received = typeof apiKey === 'string' ? apiKey : '';
    const expected = config.clientApiKey;

    logger.warn('Unauthorized API access attempt', {
      receivedHeader: !!apiKey,
      receivedLength: received.length,
      expectedLength: expected.length,
      receivedStart: received.substring(0, 3) + '...',
      receivedEnd: '...' + received.substring(received.length - 3),
      expectedStart: expected.substring(0, 3) + '...',
      expectedEnd: '...' + expected.substring(expected.length - 3),
      match: apiKey === config.clientApiKey
    });
    return res.status(401).json({ error: 'Unauthorized: Invalid or missing API Key' });
  }

  next();
};
