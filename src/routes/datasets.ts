import express from 'express';

const router = express.Router();

const GITHUB_REPO = 'sreecharan-desu/uidai-data-sync';
const RELEASE_TAG = 'dataset-latest';

router.get('/:dataset', (req, res) => {
  const { dataset } = req.params;
  const { year } = req.query;

  const validDatasets = ['biometric', 'enrolment', 'demographic'];

  if (!validDatasets.includes(dataset)) {
    res.status(404).json({ error: 'Dataset not found. Available: biometric, enrolment, demographic' });
    return;
  }

  let fileName = '';

  if (year) {
    if (!/^\d{4}$/.test(String(year))) {
      res.status(400).json({ error: 'Invalid year format. Use YYYY.' });
      return;
    }
    fileName = `${dataset}_${year}.csv`;
  } else {
    fileName = `${dataset}_full.csv`;
  }

  // Redirect to GitHub Release
  const downloadUrl = `https://github.com/${GITHUB_REPO}/releases/download/${RELEASE_TAG}/${fileName}`;

  res.redirect(downloadUrl);
});

export default router;
