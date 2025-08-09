const fs = require('fs');
const path = require('path');

// MapReduce implementation for finding top selling products
class TopSellingProductsMapReduce {
    constructor() {
        this.mapResults = new Map();
    }

    // Map phase: Emit (ITEM CODE, RETAIL SALES + WAREHOUSE SALES)
    map(record) {
        const itemCode = record['ITEM CODE'];
        const itemDescription = record['ITEM DESCRIPTION'];
        const retailSales = parseFloat(record['RETAIL SALES']) || 0;
        const warehouseSales = parseFloat(record['WAREHOUSE SALES']) || 0;
        const totalSales = retailSales + warehouseSales;

        // Only process records with sales > 0
        if (totalSales > 0) {
            if (!this.mapResults.has(itemCode)) {
                this.mapResults.set(itemCode, {
                    sales: [],
                    description: itemDescription
                });
            }
            this.mapResults.get(itemCode).sales.push(totalSales);
        }
    }

    // Reduce phase: Sum total sales per item
    reduce() {
        const reducedResults = new Map();
        
        for (const [itemCode, data] of this.mapResults) {
            const totalSales = data.sales.reduce((sum, sales) => sum + sales, 0);
            reducedResults.set(itemCode, {
                totalSales: totalSales,
                description: data.description
            });
        }
        
        return reducedResults;
    }

    // Get top N products by total sales
    getTopProducts(reducedResults, n = 10) {
        return Array.from(reducedResults.entries())
            .sort((a, b) => b[1].totalSales - a[1].totalSales)
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
        console.log('Starting MapReduce analysis for Top Selling Products...\n');
        
        // Read the dataset
        const csvContent = fs.readFileSync('dataset.csv', 'utf8');
        console.log('Dataset loaded successfully');
        
        // Parse CSV
        const records = parseCSV(csvContent);
        console.log(`Total records: ${records.length}`);
        
        // Initialize MapReduce
        const mapReduce = new TopSellingProductsMapReduce();
        
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
        
        console.log(`Map phase completed. Unique items found: ${mapReduce.mapResults.size}`);
        
        // Reduce phase
        console.log('\nExecuting Reduce phase...');
        const reducedResults = mapReduce.reduce();
        console.log('Reduce phase completed');
        
        // Get top 10 products
        const topProducts = mapReduce.getTopProducts(reducedResults, 10);
        
        // Display results
        console.log('\n=== TOP 10 SELLING PRODUCTS ===');
        console.log('Rank | Item Code | Total Sales (Retail + Warehouse)');
        console.log('-----|-----------|--------------------------------');
        
        topProducts.forEach((product, index) => {
            const [itemCode, totalSales] = product;
            console.log(`${(index + 1).toString().padStart(4)} | ${itemCode.padEnd(9)} | $${totalSales.toFixed(2)}`);
        });
        
        // Additional statistics
        console.log('\n=== SUMMARY STATISTICS ===');
        const allSales = Array.from(reducedResults.values());
        const totalRevenue = allSales.reduce((sum, sales) => sum + sales, 0);
        const averageSales = totalRevenue / allSales.length;
        
        console.log(`Total unique products: ${reducedResults.size}`);
        console.log(`Total revenue: $${totalRevenue.toFixed(2)}`);
        console.log(`Average sales per product: $${averageSales.toFixed(2)}`);
        
        // Top 10 contribution to total revenue
        const top10Revenue = topProducts.reduce((sum, product) => sum + product[1], 0);
        const top10Percentage = (top10Revenue / totalRevenue) * 100;
        console.log(`Top 10 products contribute: ${top10Percentage.toFixed(2)}% of total revenue`);
        
    } catch (error) {
        console.error('Error processing data:', error.message);
    }
}

// Run the analysis
main();