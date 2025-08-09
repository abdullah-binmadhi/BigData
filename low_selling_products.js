const fs = require('fs');

// MapReduce implementation for low-selling products
class LowSellingProductsMapReduce {
    constructor(threshold = 100) {
        this.mapResults = new Map();
        this.threshold = threshold; // Sales threshold for low-selling products
    }

    // Map phase: Emit (ITEM CODE, RETAIL + WAREHOUSE)
    map(record) {
        const itemCode = record['ITEM CODE'];
        const itemDescription = record['ITEM DESCRIPTION'];
        const itemType = record['ITEM TYPE'];
        const supplier = record['SUPPLIER'];
        const retailSales = parseFloat(record['RETAIL SALES']) || 0;
        const warehouseSales = parseFloat(record['WAREHOUSE SALES']) || 0;
        const totalSales = retailSales + warehouseSales;

        // Process all records with valid item codes (including zero sales)
        if (itemCode && itemCode.trim() !== '') {
            if (!this.mapResults.has(itemCode)) {
                this.mapResults.set(itemCode, {
                    sales: [],
                    description: itemDescription,
                    itemType: itemType,
                    supplier: supplier
                });
            }
            this.mapResults.get(itemCode).sales.push(totalSales);
        }
    }

    // Reduce phase: Sum and filter in reducer
    reduce() {
        const reducedResults = new Map();
        const lowSellingProducts = new Map();

        for (const [itemCode, data] of this.mapResults) {
            const totalSales = data.sales.reduce((sum, sales) => sum + sales, 0);
            const transactionCount = data.sales.filter(sale => sale > 0).length;

            const productData = {
                totalSales: totalSales,
                transactionCount: transactionCount,
                averageSale: transactionCount > 0 ? totalSales / transactionCount : 0,
                description: data.description,
                itemType: data.itemType,
                supplier: data.supplier
            };

            reducedResults.set(itemCode, productData);

            // Filter low-selling products in reducer
            if (totalSales < this.threshold) {
                lowSellingProducts.set(itemCode, productData);
            }
        }

        return { allProducts: reducedResults, lowSellingProducts: lowSellingProducts };
    }

    // Get low-selling products sorted by sales (ascending)
    getLowSellingProductsSorted(lowSellingProducts) {
        return Array.from(lowSellingProducts.entries())
            .sort((a, b) => a[1].totalSales - b[1].totalSales);
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
        // Allow threshold to be passed as command line argument
        const threshold = process.argv[2] ? parseFloat(process.argv[2]) : 100;

        console.log(`Starting MapReduce analysis for Low-Selling Products (threshold: $${threshold})...\n`);

        // Read the dataset
        const csvContent = fs.readFileSync('dataset.csv', 'utf8');
        console.log('Dataset loaded successfully');

        // Parse CSV
        const records = parseCSV(csvContent);
        console.log(`Total records: ${records.length}`);

        // Initialize MapReduce
        const mapReduce = new LowSellingProductsMapReduce(threshold);

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

        console.log(`Map phase completed. Unique products found: ${mapReduce.mapResults.size}`);

        // Reduce phase
        console.log('\nExecuting Reduce phase...');
        const { allProducts, lowSellingProducts } = mapReduce.reduce();
        console.log('Reduce phase completed');

        // Get low-selling products sorted
        const lowSellingProductsSorted = mapReduce.getLowSellingProductsSorted(lowSellingProducts);

        // Display results
        console.log(`\n=== LOW-SELLING PRODUCTS (< $${threshold}) ===`);
        console.log(`Found ${lowSellingProductsSorted.length} products that need marketing attention`);
        console.log('\nRank | Item Code | Total Sales | Transactions | Item Type | Description');
        console.log('-----|-----------|-------------|--------------|-----------|-------------');

        // Show top 20 lowest selling products
        const displayCount = Math.min(20, lowSellingProductsSorted.length);
        lowSellingProductsSorted.slice(0, displayCount).forEach((product, index) => {
            const [itemCode, data] = product;
            const formattedSales = `$${data.totalSales.toFixed(2)}`.padEnd(11);
            const formattedTransactions = data.transactionCount.toString().padEnd(12);
            const itemType = (data.itemType || 'N/A').padEnd(9);
            const description = data.description || 'No description';

            console.log(`${(index + 1).toString().padStart(4)} | ${itemCode.padEnd(9)} | ${formattedSales} | ${formattedTransactions} | ${itemType} | ${description}`);
        });

        if (lowSellingProductsSorted.length > displayCount) {
            console.log(`... and ${lowSellingProductsSorted.length - displayCount} more products`);
        }

        // Zero sales products (critical attention needed)
        const zeroSalesProducts = lowSellingProductsSorted.filter(product => product[1].totalSales === 0);
        console.log(`\n🚨 CRITICAL: ${zeroSalesProducts.length} products with ZERO sales need immediate attention!`);

        // Category breakdown of low-selling products
        console.log('\n=== LOW-SELLING PRODUCTS BY CATEGORY ===');
        const categoryBreakdown = new Map();
        lowSellingProductsSorted.forEach(product => {
            const [itemCode, data] = product;
            const category = data.itemType || 'Unknown';
            if (!categoryBreakdown.has(category)) {
                categoryBreakdown.set(category, { count: 0, totalSales: 0 });
            }
            categoryBreakdown.get(category).count += 1;
            categoryBreakdown.get(category).totalSales += data.totalSales;
        });

        Array.from(categoryBreakdown.entries())
            .sort((a, b) => b[1].count - a[1].count)
            .forEach(category => {
                const [categoryName, data] = category;
                const avgSales = data.totalSales / data.count;
                console.log(`${categoryName}: ${data.count} products, avg $${avgSales.toFixed(2)} sales`);
            });

        // Supplier analysis for low-selling products
        console.log('\n=== SUPPLIERS WITH MOST LOW-SELLING PRODUCTS ===');
        const supplierBreakdown = new Map();
        lowSellingProductsSorted.forEach(product => {
            const [itemCode, data] = product;
            const supplier = data.supplier || 'Unknown';
            if (!supplierBreakdown.has(supplier)) {
                supplierBreakdown.set(supplier, { count: 0, totalSales: 0 });
            }
            supplierBreakdown.get(supplier).count += 1;
            supplierBreakdown.get(supplier).totalSales += data.totalSales;
        });

        Array.from(supplierBreakdown.entries())
            .sort((a, b) => b[1].count - a[1].count)
            .slice(0, 10)
            .forEach((supplier, index) => {
                const [supplierName, data] = supplier;
                const avgSales = data.totalSales / data.count;
                console.log(`${index + 1}. ${supplierName}: ${data.count} low-selling products, avg $${avgSales.toFixed(2)}`);
            });

        // Marketing recommendations
        console.log('\n=== MARKETING RECOMMENDATIONS ===');
        console.log('🎯 IMMEDIATE ACTION NEEDED:');

        if (zeroSalesProducts.length > 0) {
            console.log(`   • ${zeroSalesProducts.length} products with zero sales - consider discontinuation or aggressive promotion`);
        }

        const veryLowSales = lowSellingProductsSorted.filter(p => p[1].totalSales > 0 && p[1].totalSales < threshold * 0.1);
        if (veryLowSales.length > 0) {
            console.log(`   • ${veryLowSales.length} products with sales < $${(threshold * 0.1).toFixed(0)} - urgent marketing push needed`);
        }

        const lowButNotZero = lowSellingProductsSorted.filter(p => p[1].totalSales >= threshold * 0.1 && p[1].totalSales < threshold);
        if (lowButNotZero.length > 0) {
            console.log(`   • ${lowButNotZero.length} products with moderate low sales - targeted campaigns recommended`);
        }

        console.log('\n📊 STRATEGIC INSIGHTS:');
        const totalProducts = allProducts.size;
        const lowSellingPercentage = (lowSellingProducts.size / totalProducts) * 100;
        console.log(`   • ${lowSellingPercentage.toFixed(1)}% of products are underperforming`);

        const totalRevenue = Array.from(allProducts.values()).reduce((sum, data) => sum + data.totalSales, 0);
        const lowSellingRevenue = Array.from(lowSellingProducts.values()).reduce((sum, data) => sum + data.totalSales, 0);
        const revenueImpact = (lowSellingRevenue / totalRevenue) * 100;
        console.log(`   • Low-selling products represent ${revenueImpact.toFixed(2)}% of total revenue`);

        // Output format as requested
        console.log('\n=== PRODUCTS THAT NEED A MARKETING PUSH ===');
        console.log(`${lowSellingProductsSorted.length} products identified for marketing intervention:`);

        // Group by urgency level
        const urgent = lowSellingProductsSorted.filter(p => p[1].totalSales === 0);
        const high = lowSellingProductsSorted.filter(p => p[1].totalSales > 0 && p[1].totalSales < threshold * 0.2);
        const medium = lowSellingProductsSorted.filter(p => p[1].totalSales >= threshold * 0.2 && p[1].totalSales < threshold * 0.6);
        const low = lowSellingProductsSorted.filter(p => p[1].totalSales >= threshold * 0.6);

        console.log(`🔴 URGENT (${urgent.length}): Zero sales products`);
        console.log(`🟠 HIGH (${high.length}): Very low sales (< $${(threshold * 0.2).toFixed(0)})`);
        console.log(`🟡 MEDIUM (${medium.length}): Low sales ($${(threshold * 0.2).toFixed(0)} - $${(threshold * 0.6).toFixed(0)})`);
        console.log(`🟢 LOW (${low.length}): Below threshold but showing activity ($${(threshold * 0.6).toFixed(0)} - $${threshold})`);

    } catch (error) {
        console.error('Error processing data:', error.message);
    }
}

// Run the analysis
main();