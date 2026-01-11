import fs from 'fs';
import readline from 'readline';
import path from 'path';

export async function splitByYear() {
  const publicDir = path.join(process.cwd(), 'public', 'datasets');
  const files = [
    path.join(publicDir, 'enrolment_full.csv'),
    path.join(publicDir, 'demographic_full.csv'),
    path.join(publicDir, 'biometric_full.csv')
  ];

  for (const filename of files) {
    if (!fs.existsSync(filename)) {
      console.log(`Skipping ${filename} (not found).`);
      continue;
    }

    console.log(`\nProcessing ${filename}...`);
    
    // BaseName needs to strip the directory path
    const baseName = path.basename(filename).replace('_full.csv', '');
    const outputDir = path.join(publicDir, 'split_data');
    
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const fileStream = fs.createReadStream(filename);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    let header = '';
    let lineCount = 0;
    const yearFileHandles: Record<string, fs.WriteStream> = {};

    for await (const line of rl) {
      if (!line.trim()) continue;

      if (lineCount === 0) {
        header = line;
        lineCount++;
        continue;
      }

      // Extract year from the first column correctly
      // Format is usually "DD-MM-YYYY" (with or without quotes)
      const cols = line.split(',');
      const dateVal = cols[0].replace(/"/g, ''); // Remove quotes
      const yearMatch = dateVal.match(/-(\d{4})$/); // Match year at the end of hyphenated date
      const year = yearMatch ? yearMatch[1] : 'unknown';

      if (!yearFileHandles[year]) {
        const yearPath = path.join(outputDir, `${baseName}_${year}.csv`);
        yearFileHandles[year] = fs.createWriteStream(yearPath);
        yearFileHandles[year].write(header + '\n');
        // console.log(`  Initialized file: ${baseName}_${year}.csv`);
      }

      yearFileHandles[year].write(line + '\n');
      lineCount++;

      if (lineCount % 250000 === 0) {
        process.stdout.write(`  Processed ${lineCount} lines...\r`);
      }
    }

    // Close all handles
    for (const year in yearFileHandles) {
      yearFileHandles[year].end();
    }
    
    console.log(`\nFinished ${filename}. Total lines: ${lineCount}`);
  }
}

if (require.main === module) {
  splitByYear();
}
