import axios from 'axios';
import { config } from './src/config';
import fs from 'fs';
import readline from 'readline';
import { splitByYear } from './split_by_year';
import path from 'path';

async function syncDataset(datasetName: string, resourceId: string, filename: string) {
  const apiUrl = `https://api.data.gov.in/resource/${resourceId}`;
  const apiKey = config.dataGovApiKey;
  const tempFile = filename.replace('_full.csv', '_fresh.csv');

  console.log(`\n=== Syncing ${datasetName.toUpperCase()} ===`);
  console.log(`Checking ${filename} against API...`);
  
  const localData = new Map<string, string[]>();
  let header = '';
  
  if (fs.existsSync(filename)) {
      const fileStream = fs.createReadStream(filename);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      let lineCount = 0;
      for await (const line of rl) {
        if (lineCount === 0) {
          header = line;
        } else {
          // Line format: "01-01-2025",...
          const dateMatch = line.match(/^"?(\d{2}-\d{2}-\d{4})"?/);
          if (dateMatch) {
             const date = dateMatch[1];
             if (!localData.has(date)) {
                 localData.set(date, []);
             }
             localData.get(date)!.push(line);
          }
        }
        lineCount++;
        if (lineCount % 1000000 === 0) process.stdout.write(`\rLoaded ${lineCount} lines...`);
      }
      console.log(`\nLoaded ${lineCount} lines.`);
  } else {
      console.log("Local file not found, starting fresh.");
      try {
        const res = await axios.get(apiUrl, { params: { 'api-key': apiKey, format: 'json', limit: 1 } });
        header = Object.keys(res.data.records[0]).join(',');
      } catch (err: any) {
        console.error("Failed to fetch header. Aborting.");
        return;
      }
  }

  // Define Range: 2025-01-01 to Present + 10 days
  const startDate = new Date('2025-01-01');
  const endDate = new Date();
  endDate.setDate(endDate.getDate() + 10);
  
  fs.writeFileSync(tempFile, header + '\n');
  
  let currentDate = new Date(startDate);
  let totalSaved = 0;
  let hasUpdates = false;

  while (currentDate <= endDate) {
    const d = currentDate.getDate().toString().padStart(2, '0');
    const m = (currentDate.getMonth() + 1).toString().padStart(2, '0');
    const y = currentDate.getFullYear();
    const dateStr = `${d}-${m}-${y}`;
    
    // Skip future dates if obviously far in future (though +10 days handles near future)
    if (currentDate > new Date() && currentDate.getFullYear() > new Date().getFullYear() + 1) break;

    const localLines = localData.get(dateStr) || [];
    const localCount = localLines.length;

    // We only check API if we suspect missing data or if we want to be absolutely sure.
    // For specific known empty months (Jan-May 2025 typically empty for valid data), checking API is safe but slow.
    // Optimization: If localCount > 0, assume it's good? NO, user asked to compare.
    
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
             if (retries === 0) {
                 // network error, assume local is OK if widely failed?
                 // or skip day? defaulting to local count prevents dataloss but might miss updates.
                 apiCount = localCount; 
             } else {
                 await new Promise(r => setTimeout(r, 1000));
             }
        }
    }

    if (localCount === apiCount && apiCount > 0) {
        // Match! Use local.
        const chunk = localLines.join('\n');
        if (chunk) fs.appendFileSync(tempFile, chunk + '\n');
        totalSaved += localCount;
    } else if (apiCount === 0) {
        // No data
    } else {
        // Mismatch (could be Local < API, or Local > API, or Local=0 API>0)
        // Redownload fresh to ensure correctness
        hasUpdates = true;
        process.stdout.write(`\r[${dateStr}] Local: ${localCount} | API: ${apiCount} -> Syncing... `);
        
        let offset = 0;
        let dayRecords = 0;
        while (offset < apiCount) {
             try {
                 const res = await axios.get(apiUrl, {
                   params: { 
                     'api-key': apiKey, 
                     format: 'json', 
                     limit: 5000, 
                     offset, 
                     'filters[date]': dateStr 
                   }
                 });
                 
                 const records = res.data.records;
                 if (!records || records.length === 0) break;

                 const csvRows = records.map((row: any) => 
                   Object.values(row).map(val => `"${val}"`).join(',')
                 ).join('\n');
                 
                 fs.appendFileSync(tempFile, csvRows + '\n');
                 offset += records.length;
                 dayRecords += records.length;
             } catch (err: any) {
                 await new Promise(r => setTimeout(r, 2000));
             }
        }
        totalSaved += dayRecords;
        process.stdout.write("Done.\n");
    }

    currentDate.setDate(currentDate.getDate() + 1);
  }

  console.log(`\nSync finished for ${datasetName}. Total records: ${totalSaved}`);
  
  // Replace file
  fs.renameSync(tempFile, filename);
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
