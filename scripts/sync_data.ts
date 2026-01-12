
import axios from 'axios';
import { config } from '../src/config';
import fs from 'fs';
import readline from 'readline';
import { splitByYear } from './split_by_year';
import path from 'path';

async function getLastDateRaw(filename: string): Promise<Date | null> {
    if (!fs.existsSync(filename)) return null;

    // Fast way to get last valid date from file without reading everything into memory
    // We read chunks from the end.
    // For now, simpler approach: Use `tail` via execSync is easiest on Mac/Linux, but let's do Node way for portability?
    // Actually, user is on Mac.
    
    try {
        const { execSync } = require('child_process');
        // Get last 5 lines to be safe against empty lines
        const lastLines = execSync(`tail -n 5 "${filename}"`).toString().trim().split('\n');
        
        for (let i = lastLines.length - 1; i >= 0; i--) {
            const line = lastLines[i];
            const dateMatch = line.match(/^"?(\d{2}-\d{2}-\d{4})"?/);
            if (dateMatch) {
                const [d, m, y] = dateMatch[1].split('-').map(Number);
                return new Date(y, m - 1, d);
            }
        }
    } catch (e) {
        return null; // File might be empty or issues
    }
    return null;
}

// Append-Only Sync Logic
async function syncDataset(datasetName: string, resourceId: string, filename: string) {
  const apiUrl = `https://api.data.gov.in/resource/${resourceId}`;
  const apiKey = config.dataGovApiKey;

  console.log(`\n=== Syncing ${datasetName.toUpperCase()} ===`);
  
  let startDate = new Date('2025-01-01');
  const lastDate = await getLastDateRaw(filename);
  let isAppendMode = false;

  if (lastDate) {
      console.log(`Found existing data up to: ${lastDate.toDateString()}`);
      // Start checking from the last date (inclusive, to ensure it's complete)
      startDate = new Date(lastDate);
      isAppendMode = true;
  } else {
      console.log("No existing data found. Starting fresh from Jan 1, 2025.");
      // Write header if new
      if (fs.existsSync(filename)) fs.unlinkSync(filename); // Clean start
      try {
        const res = await axios.get(apiUrl, { params: { 'api-key': apiKey, format: 'json', limit: 1 } });
        const header = Object.keys(res.data.records[0]).join(',');
        fs.writeFileSync(filename, header + '\n');
      } catch (err: any) {
        console.error("Failed to fetch header.");
        return;
      }
  }

  // End Date: Today
  const endDate = new Date();
  // Loop
  let currentDate = new Date(startDate);
  let totalAdded = 0;

  while (currentDate <= endDate) {
    const d = currentDate.getDate().toString().padStart(2, '0');
    const m = (currentDate.getMonth() + 1).toString().padStart(2, '0');
    const y = currentDate.getFullYear();
    const dateStr = `${d}-${m}-${y}`;
    
    // Check API Total
    let apiCount = -1;
    let retries = 3;
    while (retries > 0 && apiCount === -1) {
        try {
           const res = await axios.get(apiUrl, {
               params: { 'api-key': apiKey, format: 'json', limit: 0, 'filters[date]': dateStr }
           });
           apiCount = res.data.total;
        } catch (e: any) {
            retries--;
             if (retries === 0) apiCount = 0; // Assume 0 on fail to avoid blocking, or error out?
             else await new Promise(r => setTimeout(r, 1000));
        }
    }

    if (apiCount > 0) {
        // We have data on API.
        // If append mode and date == lastDate, checks if we need to remove old entries or just skip?
        // To be safe: If we are on the exact last day, we might have partial data.
        // Strategy: For the *exact last day*, delete it from file and re-sync it?
        // OR: Count local lines for that day.
        
        let localCount = 0;
        if (isAppendMode) {
             const { execSync } = require('child_process');
             const grepCount = parseInt(execSync(`grep -c "^\\"${dateStr}\\"" "${filename}" || echo 0`).toString().trim());
             localCount = grepCount;
        }

        if (localCount < apiCount) {
             if (localCount > 0) {
                 // Partial data! Remove that day's data to avoid dups before appending
                 // This is tricky with append-only.
                 // Easier: Just warn and append *difference*? No, API pagination doesn't support "get last N".
                 // Robust way: Use 'sed' to remove lines matching dateStr.
                 const { execSync } = require('child_process');
                 // Only do this for the *current* iteration date if it clashes
                 // Escape quotes for sed
                 // Mac sed requires '' after -i
                 execSync(`sed -i '' '/^"${dateStr}"/d' "${filename}"`);
                 console.log(`  [${dateStr}] Removing partial local data (${localCount}) to re-sync full day.`);
             }
             
             process.stdout.write(`  [${dateStr}] Downloading ${apiCount} records... `);
             
             let offset = 0;
             while (offset < apiCount) {
                 try {
                     const res = await axios.get(apiUrl, {
                       params: { 'api-key': apiKey, format: 'json', limit: 5000, offset, 'filters[date]': dateStr }
                     });
                     
                     const records = res.data.records;
                     if (!records || records.length === 0) break;

                     const csvRows = records.map((row: any) => 
                       Object.values(row).map(val => `"${val}"`).join(',')
                     ).join('\n');
                     
                     fs.appendFileSync(filename, csvRows + '\n');
                     offset += records.length;
                     totalAdded += records.length;
                 } catch (err: any) {
                     await new Promise(r => setTimeout(r, 2000));
                 }
             }
             process.stdout.write("Done.\n");
        } else {
            // Check
           // console.log(`  [${dateStr}] Local ${localCount} matches API ${apiCount}. Skipped.`);
        }
    }

    currentDate.setDate(currentDate.getDate() + 1);
    // After first iteration (lastDate), we are definitely in append mode for fresh days
    isAppendMode = true; 
  }

  console.log(`Finished ${datasetName}. Added ${totalAdded} new records.`);
}

async function runSync() {
    const publicDir = path.join(process.cwd(), 'public', 'datasets');
    if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir, { recursive: true });

    const datasets = [
        { name: 'enrolment', id: config.resources.enrolment, file: path.join(publicDir, 'enrolment_full.csv') },
        { name: 'demographic', id: config.resources.demographic, file: path.join(publicDir, 'demographic_full.csv') },
        { name: 'biometric', id: config.resources.biometric, file: path.join(publicDir, 'biometric_full.csv') }
    ];

    for (const ds of datasets) {
        await syncDataset(ds.name, ds.id, ds.file);
    }
    
    console.log("\nAll datasets synced.");
    console.log("Updating split data files...");
    await splitByYear();
    console.log("Sync Complete!");
}

runSync();
