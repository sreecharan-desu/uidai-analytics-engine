
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

  let fileName = '';
  let filePath = '';

  if (year) {
      // Serve split file from split_data directory
      // Sanitize year to basically ensure it's a number/simple string to avoid directory traversal
      if (!/^\d{4}$/.test(String(year))) {
          res.status(400).json({ error: 'Invalid year format. Use YYYY.' });
          return;
      }
      fileName = `${dataset}_${year}.csv`;
      filePath = path.resolve(process.cwd(), 'split_data', fileName);
  } else {
      // Serve full file from root
      fileName = `${dataset}_full.csv`;
      filePath = path.resolve(process.cwd(), fileName);
  }

  console.log(`[API] Resolving file path: ${filePath}`);

  if (!fs.existsSync(filePath)) {
     console.error(`[API] File not found. Checked: ${filePath}`);
     res.status(404).json({ error: `File not found: ${fileName}` });
     return;
  }

  // Stream the file
  res.download(filePath, fileName, (err) => {
    if (err) {
      if (!res.headersSent) {
         res.status(500).json({ error: 'Error downloading file' });
      }
    }
  });
});

export default router;
