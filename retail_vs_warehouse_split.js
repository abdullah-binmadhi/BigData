const fs = require('fs');

// MapReduce implementation for retail vs warehouse sales split
class RetailVsWarehouseSplitMapReduce {
    constructor() {
        this.mapResults = new Map();
    }

    // Map phase: Emit ("retail", RETAIL SALES), ("warehouse", WAREHOUSE SALES)
    map(record) {
        const retailSales = parseFloat(record['RETAIL SALES']) || 0;
        const warehouseSales = parseFloat(record['WAREHOUSE SALES']) || 0;

        // Emit retail sales
        if (retailSales > 0) {
            if (!this.mapResults.has('retail')) {
                this.mapResults.set('retail', []);
            }
            this.mapResults.get('retail').push(retailSales);
        }

        // Emit warehouse sales
        if (warehouseSales > 0) {
            if (!this.mapResults.has('warehouse')) {
                this.mapResults.set('warehouse', []);
            }
            this.mapResults.get('warehouse').push(warehouseSales);
        }
    }

    // Reduce phase: Sum both
    reduce() {
        const reducedResults = new Map();
        
        for (const [salesType, salesArray] of this.mapResults) {
            const totalSales = salesArray.reduce((sum, sales) => sum + sales, 0);
            const transactionCount = salesArray.length;
            reducedResults.set(salesType, {
                totalSales: totalSales,
                transactionCount: transactionCount,
                averageSale: totalSales / transactionCount
            });
        }
        
        return reducedResults;
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
        console.log('Starting MapReduce analysis for Retail vs Warehouse Sales Split...\n');
        
        // Read the dataset
        const csvContent = fs.readFileSync('dataset.csv', 'utf8');
        console.log('Dataset loaded successfully');
        
        // Parse CSV
        const records = parseCSV(csvContent);
        console.log(`Total records: ${records.length}`);
        
        // Initialize MapReduce
        const mapReduce = new RetailVsWarehouseSplitMapReduce();
        
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
        
        console.log(`Map phase completed. Sales channels found: ${mapReduce.mapResults.size}`);
        
        // Reduce phase
        console.log('\nExecuting Reduce phase...');
        const reducedResults = mapReduce.reduce();
        console.log('Reduce phase completed');
        
        // Display results
        console.log('\n=== RETAIL VS WAREHOUSE SALES SPLIT ===');
        console.log('Channel   | Total Sales    | Transactions | Avg Sale   | Market Share');
        console.log('----------|----------------|--------------|------------|-------------');
        
        // Calculate total for market share
        const totalRevenue = Array.from(reducedResults.values())
            .reduce((sum, data) => sum + data.totalSales, 0);
        const totalTransactions = Array.from(reducedResults.values())
            .reduce((sum, data) => sum + data.transactionCount, 0);
        
        // Sort by total sales (descending)
        const sortedResults = Array.from(reducedResults.entries())
            .sort((a, b) => b[1].totalSales - a[1].totalSales);
        
        sortedResults.forEach(channelData => {
            const [channel, data] = channelData;
            const formattedSales = `$${data.totalSales.toFixed(2)}`.padEnd(14);
            const formattedTransactions = data.transactionCount.toString().padEnd(12);
            const formattedAvgSale = `$${data.averageSale.toFixed(2)}`.padEnd(10);
            const marketShare = ((data.totalSales / totalRevenue) * 100).toFixed(1);
            
            console.log(`${channel.padEnd(9)} | ${formattedSales} | ${formattedTransactions} | ${formattedAvgSale} | ${marketShare}%`);
        });
        
        // Formatted output as requested in the problem
        console.log('\n=== FORMATTED OUTPUT ===');
        const retailData = reducedResults.get('retail');
        const warehouseData = reducedResults.get('warehouse');
        
        if (retailData) {
            const retailUnits = Math.round(retailData.totalSales);
            console.log(`Retail = ${retailUnits.toLocaleString()} units`);
        } else {
            console.log('Retail = 0 units');
        }
        
        if (warehouseData) {
            const warehouseUnits = Math.round(warehouseData.totalSales);
            console.log(`Warehouse = ${warehouseUnits.toLocaleString()} units`);
        } else {
            console.log('Warehouse = 0 units');
        }
        
        // Additional statistics and insights
        console.log('\n=== DETAILED ANALYSIS ===');
        console.log(`Total combined revenue: $${totalRevenue.toFixed(2)}`);
        console.log(`Total combined transactions: ${totalTransactions.toLocaleString()}`);
        console.log(`Overall average sale: $${(totalRevenue / totalTransactions).toFixed(2)}`);
        
        if (retailData && warehouseData) {
            const retailShare = (retailData.totalSales / totalRevenue) * 100;
            const warehouseShare = (warehouseData.totalSales / totalRevenue) * 100;
            const retailTransactionShare = (retailData.transactionCount / totalTransactions) * 100;
            const warehouseTransactionShare = (warehouseData.transactionCount / totalTransactions) * 100;
            
            console.log('\n=== COMPARATIVE INSIGHTS ===');
            console.log(`Retail dominance: ${retailShare > warehouseShare ? 'YES' : 'NO'} (${retailShare.toFixed(1)}% vs ${warehouseShare.toFixed(1)}%)`);
            console.log(`Retail avg sale vs Warehouse: $${retailData.averageSale.toFixed(2)} vs $${warehouseData.averageSale.toFixed(2)}`);
            console.log(`Transaction distribution: Retail ${retailTransactionShare.toFixed(1)}%, Warehouse ${warehouseTransactionShare.toFixed(1)}%`);
            
            // Determine which channel is more efficient
            const retailEfficiency = retailData.totalSales / retailData.transactionCount;
            const warehouseEfficiency = warehouseData.totalSales / warehouseData.transactionCount;
            const moreEfficientChannel = retailEfficiency > warehouseEfficiency ? 'Retail' : 'Warehouse';
            const efficiencyDifference = Math.abs(retailEfficiency - warehouseEfficiency);
            
            console.log(`More efficient channel: ${moreEfficientChannel} (${efficiencyDifference.toFixed(2)} higher avg sale)`);
            
            // Business insights
            console.log('\n=== BUSINESS INSIGHTS ===');
            if (retailShare > 70) {
                console.log('📊 Retail-dominant business model - focus on consumer sales');
            } else if (warehouseShare > 70) {
                console.log('📊 Wholesale-dominant business model - focus on B2B sales');
            } else {
                console.log('📊 Balanced distribution model - diversified sales channels');
            }
            
            if (retailData.averageSale > warehouseData.averageSale * 2) {
                console.log('💰 Retail commands premium pricing');
            } else if (warehouseData.averageSale > retailData.averageSale * 2) {
                console.log('💰 Warehouse sales involve larger volume transactions');
            } else {
                console.log('💰 Similar pricing structure across channels');
            }
        }
        
    } catch (error) {
        console.error('Error processing data:', error.message);
    }
}

// Run the analysis
main();