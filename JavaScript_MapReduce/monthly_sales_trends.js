const fs = require('fs');

// MapReduce implementation for monthly sales trends
class MonthlySalesTrendsMapReduce {
    constructor() {
        this.mapResults = new Map();
    }

    // Map phase: Emit ((YEAR, MONTH), RETAIL SALES + WAREHOUSE SALES)
    map(record) {
        const year = record['YEAR'];
        const month = record['MONTH'];
        const retailSales = parseFloat(record['RETAIL SALES']) || 0;
        const warehouseSales = parseFloat(record['WAREHOUSE SALES']) || 0;
        const totalSales = retailSales + warehouseSales;

        // Only process records with sales > 0 and valid year/month
        if (totalSales > 0 && year && month) {
            // Create a composite key (YEAR, MONTH)
            const monthKey = `${year}-${month.toString().padStart(2, '0')}`;
            
            if (!this.mapResults.has(monthKey)) {
                this.mapResults.set(monthKey, []);
            }
            this.mapResults.get(monthKey).push(totalSales);
        }
    }

    // Reduce phase: Sum by month
    reduce() {
        const reducedResults = new Map();
        
        for (const [monthKey, salesArray] of this.mapResults) {
            const totalSales = salesArray.reduce((sum, sales) => sum + sales, 0);
            const transactionCount = salesArray.length;
            reducedResults.set(monthKey, {
                totalSales: totalSales,
                transactionCount: transactionCount,
                averageSale: totalSales / transactionCount
            });
        }
        
        return reducedResults;
    }

    // Get months sorted chronologically
    getMonthsSorted(reducedResults) {
        return Array.from(reducedResults.entries())
            .sort((a, b) => a[0].localeCompare(b[0])); // Sort by YYYY-MM format
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

// Helper function to get month name
function getMonthName(monthNum) {
    const months = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ];
    return months[parseInt(monthNum) - 1] || monthNum;
}

// Main execution
function main() {
    try {
        console.log('Starting MapReduce analysis for Monthly Sales Trends...\n');
        
        // Read the dataset
        const csvContent = fs.readFileSync('dataset.csv', 'utf8');
        console.log('Dataset loaded successfully');
        
        // Parse CSV
        const records = parseCSV(csvContent);
        console.log(`Total records: ${records.length}`);
        
        // Initialize MapReduce
        const mapReduce = new MonthlySalesTrendsMapReduce();
        
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
        
        console.log(`Map phase completed. Unique months found: ${mapReduce.mapResults.size}`);
        
        // Reduce phase
        console.log('\nExecuting Reduce phase...');
        const reducedResults = mapReduce.reduce();
        console.log('Reduce phase completed');
        
        // Get months sorted chronologically
        const monthsSorted = mapReduce.getMonthsSorted(reducedResults);
        
        // Display results
        console.log('\n=== MONTHLY SALES TRENDS ===');
        console.log('Month    | Total Sales    | Transactions | Avg Sale | Growth');
        console.log('---------|----------------|--------------|----------|--------');
        
        let previousSales = null;
        monthsSorted.forEach((monthData, index) => {
            const [monthKey, data] = monthData;
            const [year, month] = monthKey.split('-');
            const monthName = getMonthName(month);
            const formattedSales = `$${data.totalSales.toFixed(2)}`.padEnd(14);
            const formattedTransactions = data.transactionCount.toString().padEnd(12);
            const formattedAvgSale = `$${data.averageSale.toFixed(2)}`.padEnd(8);
            
            let growthStr = 'N/A';
            if (previousSales !== null) {
                const growth = ((data.totalSales - previousSales) / previousSales) * 100;
                growthStr = `${growth >= 0 ? '+' : ''}${growth.toFixed(1)}%`;
            }
            
            console.log(`${year}-${monthName.padEnd(3)} | ${formattedSales} | ${formattedTransactions} | ${formattedAvgSale} | ${growthStr}`);
            previousSales = data.totalSales;
        });
        
        // Formatted output as requested in the problem
        console.log('\n=== FORMATTED OUTPUT ===');
        monthsSorted.forEach(monthData => {
            const [monthKey, data] = monthData;
            // Convert to units (assuming $1 = 1 unit for display purposes)
            const units = Math.round(data.totalSales);
            console.log(`${monthKey} → ${units.toLocaleString()} units`);
        });
        
        // Additional statistics
        console.log('\n=== SUMMARY STATISTICS ===');
        const totalRevenue = monthsSorted.reduce((sum, monthData) => sum + monthData[1].totalSales, 0);
        const totalTransactions = monthsSorted.reduce((sum, monthData) => sum + monthData[1].transactionCount, 0);
        const averageMonthlyRevenue = totalRevenue / monthsSorted.length;
        
        console.log(`Total months analyzed: ${monthsSorted.length}`);
        console.log(`Total revenue across all months: $${totalRevenue.toFixed(2)}`);
        console.log(`Average monthly revenue: $${averageMonthlyRevenue.toFixed(2)}`);
        console.log(`Total transactions: ${totalTransactions.toLocaleString()}`);
        
        // Find best and worst performing months
        const bestMonth = monthsSorted.reduce((best, current) => 
            current[1].totalSales > best[1].totalSales ? current : best
        );
        const worstMonth = monthsSorted.reduce((worst, current) => 
            current[1].totalSales < worst[1].totalSales ? current : worst
        );
        
        console.log(`\n🏆 Best month: ${bestMonth[0]} with $${bestMonth[1].totalSales.toFixed(2)}`);
        console.log(`📉 Worst month: ${worstMonth[0]} with $${worstMonth[1].totalSales.toFixed(2)}`);
        
        // Seasonal analysis
        console.log('\n=== SEASONAL ANALYSIS ===');
        const seasonalData = new Map();
        monthsSorted.forEach(monthData => {
            const [monthKey, data] = monthData;
            const month = parseInt(monthKey.split('-')[1]);
            let season;
            if (month >= 3 && month <= 5) season = 'Spring';
            else if (month >= 6 && month <= 8) season = 'Summer';
            else if (month >= 9 && month <= 11) season = 'Fall';
            else season = 'Winter';
            
            if (!seasonalData.has(season)) {
                seasonalData.set(season, { totalSales: 0, months: 0 });
            }
            seasonalData.get(season).totalSales += data.totalSales;
            seasonalData.get(season).months += 1;
        });
        
        Array.from(seasonalData.entries())
            .sort((a, b) => b[1].totalSales - a[1].totalSales)
            .forEach(seasonData => {
                const [season, data] = seasonData;
                const avgMonthlySales = data.totalSales / data.months;
                console.log(`${season}: $${data.totalSales.toFixed(2)} total, $${avgMonthlySales.toFixed(2)} avg/month`);
            });
        
    } catch (error) {
        console.error('Error processing data:', error.message);
    }
}

// Run the analysis
main();