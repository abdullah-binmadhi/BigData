console.log("Debug script starting...");
const fs = require('fs');
const csv = require('csv-parser');

let rowCount = 0;
let itemTypes = new Set();

console.log("🚀 Testing CSV parser...");

fs.createReadStream('dataset.csv')
    .pipe(csv())
    .on('data', (row) => {
        rowCount++;
        if (row['ITEM TYPE']) {
            itemTypes.add(row['ITEM TYPE']);
        }
        
        // Log first few rows for debugging
        if (rowCount <= 3) {
            console.log(`Row ${rowCount}:`, row);
        }
    })
    .on('end', () => {
        console.log(`✅ Processed ${rowCount} rows`);
        console.log(`📊 Item types found:`, Array.from(itemTypes));
        console.log("Debug script completed.");
    })
    .on('error', (error) => {
        console.error("❌ Error:", error);
    });