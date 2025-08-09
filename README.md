# BigData

A collection of data analysis scripts using MapReduce patterns to analyze sales data from a CSV dataset.

## Installation

Install the required dependencies:

```bash
npm install
```

## Scripts

### debug.js
Tests the CSV parser and provides basic dataset information including row count and item types.

### top_selling_products.js
Identifies the top-selling products by analyzing total sales (retail + warehouse) for each item code.

### total_sales_by_supplier.js
Calculates total sales figures grouped by supplier using MapReduce aggregation.

### most_popular_product_type.js
Determines the most popular product types based on total sales volume.

### low_selling_products.js
Identifies products with sales below a specified threshold (default: 100 units).

### retail_vs_warehouse_split.js
Analyzes the distribution of sales between retail and warehouse channels.

### monthly_sales_trends.js
Tracks sales trends over time by aggregating data by year and month.

## File Structure

```
.
├── README.md
├── package.json
├── package-lock.json
├── dataset.csv
├── debug.js
├── top_selling_products.js
├── total_sales_by_supplier.js
├── most_popular_product_type.js
├── low_selling_products.js
├── retail_vs_warehouse_split.js
├── monthly_sales_trends.js
└── node_modules/
```

## Usage

Run any script using Node.js:

```bash
node debug.js
node top_selling_products.js
# ... etc
```