const fs = require('fs');

// MapReduce implementation for most popular product type
class MostPopularProductTypeMapReduce {
    constructor() {
        this.mapResults = new Map();
    }

    // Map phase: Emit (ITEM TYPE, RETAIL SALES + WAREHOUSE SALES)
    map(record) {
        const itemType = record['ITEM TYPE'];
        const retailSales = parseFloat(record['RETAIL SALES']) || 0;
        const warehouseSales = parseFloat(record['WAREHOUSE SALES']) || 0;
        const totalSales = retailSales + warehouseSales;

        // Only process records with sales > 0 and valid item type
        if (totalSales > 0 && itemType && itemType.trim() !== '') {
            if (!this.mapResults.has(itemType)) {
                this.mapResults.set(itemType, []);
            }
            this.mapResults.get(itemType).push(totalSales);
        }
    }

    // Reduce phase: Sum by item type
    reduce() {
        const reducedResults = new Map();
        
        for (const [itemType, salesArray] of this.mapResults) {
            const totalSales = salesArray.reduce((sum, sales) => sum + sales, 0);
            const transactionCount = salesArray.length;
            reducedResults.set(itemType, {
                totalSales: totalSales,
                transactionCount: transactionCount,
                averageSale: totalSales / transactionCount
            });
        }
        
        return reducedResults;
    }

    // Get product types sorted by total sales
    getProductTypesByPopularity(reducedResults) {
        return Array.from(reducedResults.entries())
            .sort((a, b) => b[1].totalSales - a[1].totalSales);
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
        console.log('Starting MapReduce analysis for Most Popular Product Type...\n');
        
        // Read the dataset
        const csvContent = fs.readFileSync('dataset.csv', 'utf8');
        console.log('Dataset loaded successfully');
        
        // Parse CSV
        const records = parseCSV(csvContent);
        console.log(`Total records: ${records.length}`);
        
        // Initialize MapReduce
        const mapReduce = new MostPopularProductTypeMapReduce();
        
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
        
        console.log(`Map phase completed. Unique item types found: ${mapReduce.mapResults.size}`);
        
        // Reduce phase
        console.log('\nExecuting Reduce phase...');
        const reducedResults = mapReduce.reduce();
        console.log('Reduce phase completed');
        
        // Get product types by popularity
        const productTypesByPopularity = mapReduce.getProductTypesByPopularity(reducedResults);
        
        // Display results
        console.log('\n=== PRODUCT TYPES BY TOTAL SALES ===');
        console.log('Rank | Item Type      | Total Sales    | Transactions | Avg Sale');
        console.log('-----|----------------|----------------|--------------|----------');
        
        productTypesByPopularity.forEach((productType, index) => {
            const [itemType, data] = productType;
            const formattedSales = `$${data.totalSales.toFixed(2)}`.padEnd(14);
            const formattedTransactions = data.transactionCount.toString().padEnd(12);
            const formattedAvgSale = `$${data.averageSale.toFixed(2)}`;
            console.log(`${(index + 1).toString().padStart(4)} | ${itemType.padEnd(14)} | ${formattedSales} | ${formattedTransactions} | ${formattedAvgSale}`);
        });
        
        // Formatted output as requested in the problem
        console.log('\n=== FORMATTED OUTPUT ===');
        productTypesByPopularity.forEach(productType => {
            const [itemType, data] = productType;
            // Convert to units (assuming $1 = 1 unit for display purposes)
            const units = Math.round(data.totalSales);
            console.log(`${itemType} → ${units.toLocaleString()} units`);
        });
        
        // Additional statistics
        console.log('\n=== SUMMARY STATISTICS ===');
        const totalRevenue = productTypesByPopularity.reduce((sum, productType) => sum + productType[1].totalSales, 0);
        const totalTransactions = productTypesByPopularity.reduce((sum, productType) => sum + productType[1].transactionCount, 0);
        
        console.log(`Total product types: ${productTypesByPopularity.length}`);
        console.log(`Total revenue across all types: $${totalRevenue.toFixed(2)}`);
        console.log(`Total transactions: ${totalTransactions.toLocaleString()}`);
        console.log(`Overall average sale: $${(totalRevenue / totalTransactions).toFixed(2)}`);
        
        // Market share analysis
        console.log('\n=== MARKET SHARE ANALYSIS ===');
        productTypesByPopularity.forEach(productType => {
            const [itemType, data] = productType;
            const marketShare = (data.totalSales / totalRevenue) * 100;
            const transactionShare = (data.transactionCount / totalTransactions) * 100;
            console.log(`${itemType}: ${marketShare.toFixed(1)}% of revenue, ${transactionShare.toFixed(1)}% of transactions`);
        });
        
        // Winner announcement
        if (productTypesByPopularity.length > 0) {
            const winner = productTypesByPopularity[0];
            const [winnerType, winnerData] = winner;
            const winnerUnits = Math.round(winnerData.totalSales);
            console.log(`\n🏆 WINNER: ${winnerType} is the most popular product type with ${winnerUnits.toLocaleString()} units sold!`);
        }
        
    } catch (error) {
        console.error('Error processing data:', error.message);
    }
}

// Run the analysis
main();