const fs = require('fs');

// MapReduce implementation for total sales by supplier
class TotalSalesBySupplierMapReduce {
    constructor() {
        this.mapResults = new Map();
    }

    // Map phase: Emit (SUPPLIER, RETAIL SALES + WAREHOUSE SALES)
    map(record) {
        const supplier = record['SUPPLIER'];
        const retailSales = parseFloat(record['RETAIL SALES']) || 0;
        const warehouseSales = parseFloat(record['WAREHOUSE SALES']) || 0;
        const totalSales = retailSales + warehouseSales;

        // Only process records with sales > 0
        if (totalSales > 0 && supplier && supplier.trim() !== '') {
            if (!this.mapResults.has(supplier)) {
                this.mapResults.set(supplier, []);
            }
            this.mapResults.get(supplier).push(totalSales);
        }
    }

    // Reduce phase: Sum by supplier
    reduce() {
        const reducedResults = new Map();
        
        for (const [supplier, salesArray] of this.mapResults) {
            const totalSales = salesArray.reduce((sum, sales) => sum + sales, 0);
            reducedResults.set(supplier, totalSales);
        }
        
        return reducedResults;
    }

    // Get top N suppliers by total sales
    getTopSuppliers(reducedResults, n = 10) {
        return Array.from(reducedResults.entries())
            .sort((a, b) => b[1] - a[1])
            .slice(0, n);
    }
}

// CSV Parser
function parseCSV(csvContent) {
    const lines = csvContent.trim().split('\n');
    const headers = lines[0].split(',');
    const records = [];

    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',');
        const record = {};
        
        headers.forEach((header, index) => {
            record[header] = values[index] || '';
        });
        
        records.push(record);
    }
    
    return records;
}

// Main execution
function main() {
    try {
        console.log('Starting MapReduce analysis for Total Sales by Supplier...\n');
        
        // Read the dataset
        const csvContent = fs.readFileSync('dataset.csv', 'utf8');
        console.log('Dataset loaded successfully');
        
        // Parse CSV
        const records = parseCSV(csvContent);
        console.log(`Total records: ${records.length}`);
        
        // Initialize MapReduce
        const mapReduce = new TotalSalesBySupplierMapReduce();
        
        // Map phase
        console.log('\nExecuting Map phase...');
        let processedRecords = 0;
        records.forEach(record => {
            mapReduce.map(record);
            processedRecords++;
            if (processedRecords % 10000 === 0) {
                console.log(`Processed ${processedRecords} records`);
            }
        });
        
        console.log(`Map phase completed. Unique suppliers found: ${mapReduce.mapResults.size}`);
        
        // Reduce phase
        console.log('\nExecuting Reduce phase...');
        const reducedResults = mapReduce.reduce();
        console.log('Reduce phase completed');
        
        // Get top suppliers
        const topSuppliers = mapReduce.getTopSuppliers(reducedResults, 15);
        
        // Display results
        console.log('\n=== TOP SUPPLIERS BY TOTAL SALES ===');
        console.log('Rank | Supplier Name                           | Total Sales');
        console.log('-----|----------------------------------------|-------------');
        
        topSuppliers.forEach((supplier, index) => {
            const [supplierName, totalSales] = supplier;
            const formattedSales = `$${totalSales.toFixed(2)}`;
            console.log(`${(index + 1).toString().padStart(4)} | ${supplierName.padEnd(39)} | ${formattedSales}`);
        });
        
        // Alternative format as requested in the problem
        console.log('\n=== FORMATTED OUTPUT ===');
        topSuppliers.slice(0, 10).forEach(supplier => {
            const [supplierName, totalSales] = supplier;
            // Convert to units (assuming $1 = 1 unit for display purposes)
            const units = Math.round(totalSales);
            console.log(`${supplierName} → ${units.toLocaleString()} units`);
        });
        
        // Additional statistics
        console.log('\n=== SUMMARY STATISTICS ===');
        const allSales = Array.from(reducedResults.values());
        const totalRevenue = allSales.reduce((sum, sales) => sum + sales, 0);
        const averageSales = totalRevenue / allSales.length;
        
        console.log(`Total suppliers: ${reducedResults.size}`);
        console.log(`Total revenue across all suppliers: $${totalRevenue.toFixed(2)}`);
        console.log(`Average sales per supplier: $${averageSales.toFixed(2)}`);
        
        // Top 10 contribution to total revenue
        const top10Revenue = topSuppliers.slice(0, 10).reduce((sum, supplier) => sum + supplier[1], 0);
        const top10Percentage = (top10Revenue / totalRevenue) * 100;
        console.log(`Top 10 suppliers contribute: ${top10Percentage.toFixed(2)}% of total revenue`);
        
        // Market concentration analysis
        const top5Revenue = topSuppliers.slice(0, 5).reduce((sum, supplier) => sum + supplier[1], 0);
        const top5Percentage = (top5Revenue / totalRevenue) * 100;
        console.log(`Top 5 suppliers contribute: ${top5Percentage.toFixed(2)}% of total revenue`);
        
    } catch (error) {
        console.error('Error processing data:', error.message);
    }
}

// Run the analysis
main();