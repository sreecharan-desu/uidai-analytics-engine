
import express from 'express';
import path from 'path';
import fs from 'fs';

const router = express.Router();

router.get('/:dataset', (req, res) => {
  const { dataset } = req.params;
  const { year } = req.query;

  const validDatasets = ['biometric', 'enrolment', 'demographic'];
  
  if (!validDatasets.includes(dataset)) {
     res.status(404).json({ error: 'Dataset not found. Available: biometric, enrolment, demographic' });
     return;
  }

  // Construct path relative to public/datasets
  let relativePath = '';
  let fileName = '';

  if (year) {
      if (!/^\d{4}$/.test(String(year))) {
          res.status(400).json({ error: 'Invalid year format. Use YYYY.' });
          return;
      }
      fileName = `${dataset}_${year}.csv`;
      relativePath = `split_data/${fileName}`;
  } else {
      fileName = `${dataset}_full.csv`;
      relativePath = fileName;
  }

  // Check existence in public/datasets
  // NOTE: In Vercel, __dirname logic matches distribution. 
  // But safer to rely on process.cwd() + public for checking existence?
  // Actually, for static assets, we just redirect. 
  // Checking fs.existsSync might fail in Lambda if public isn't bundled in the function layer.
  // We will assume if it's meant to be there, we redirect. 404 will be handled by Vercel static serving if missing.
  
  // Update: Let's just redirect 302 Found to /datasets/...
  const staticUrl = `/datasets/${relativePath}`;
  
  // Optional: We could try to verify existence if we want to give a nice JSON error instead of 404 page.
  // But Vercel static hosting is robust.
  
  res.redirect(staticUrl);
});

export default router;
