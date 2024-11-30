# Moving from Transactional to Analytical Data Warehouse: A PostgreSQL Guide

## 1. Understanding the Differences

### Transactional (OLTP) vs Analytical (OLAP)

| Feature | OLTP (Transactional) | OLAP (Analytical) |
|---------|---------------------|-------------------|
| Purpose | Day-to-day operations | Analysis and decision making |
| Data Model | Normalized (3NF) | Denormalized (Star/Snowflake) |
| Query Type | Simple, few joins | Complex, many joins |
| Data Volume | Current data | Historical data |
| Updates | Frequent, real-time | Batch updates |

## 2. Dimensional Modeling Concepts

### Facts
- Numerical measurements of business processes
- Examples: sales amount, quantity sold, profit
- Located in fact tables
- Connected to dimensions via foreign keys

### Dimensions
- Descriptive attributes that provide context
- Examples: customer details, product information
- Used for filtering and grouping
- Support different types of changes (SCD)

## 3. PostgreSQL Features Used in Our Model

### 1. Serial Data Type
```sql
product_key SERIAL PRIMARY KEY
```
- Auto-incrementing integer
- Automatically generates unique identifiers
- Used for surrogate keys in dimension tables

### 2. Date/Time Functions
```sql
EXTRACT(YEAR FROM date_column)
TO_CHAR(date_column, 'Month')
```
- Extract parts of dates
- Format dates for display
- Used in date dimension population

### 3. Indexes
```sql
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
```
- Improve query performance
- Essential for large fact tables
- Created on frequently used join columns

### 4. Views
```sql
CREATE VIEW vw_daily_sales AS
SELECT ...
```
- Simplify complex queries
- Provide data abstraction
- Reusable analysis templates

### 5. Stored Procedures
```sql
CREATE OR REPLACE FUNCTION function_name()
RETURNS void AS $$
BEGIN
    -- Function body
END;
$$ LANGUAGE plpgsql;
```
- Encapsulate business logic
- Batch processing
- Data population and maintenance

## 4. ETL Process Steps

1. **Extract**
   - Identify source tables
   - Create staging tables
   - Pull data from OLTP system

2. **Transform**
   - Clean data
   - Apply business rules
   - Generate surrogate keys
   - Handle slowly changing dimensions

3. **Load**
   - Populate dimension tables first
   - Load fact tables
   - Verify referential integrity

## 5. Example: Converting OLTP to OLAP

### OLTP Schema (Before):
```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
);

CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
```

### OLAP Schema (After):
```sql
-- Dimension table
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT,  -- business key
    name VARCHAR(100),
    email VARCHAR(100),
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN
);

-- Fact table
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT,
    customer_key INT,
    product_key INT,
    quantity INT,
    sales_amount DECIMAL(10,2),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
);
```

## 6. Best Practices

1. **Naming Conventions**
   - Prefix fact tables with 'fact_'
   - Prefix dimension tables with 'dim_'
   - Use descriptive column names

2. **Performance Optimization**
   - Create appropriate indexes
   - Partition large fact tables
   - Regular statistics updates

3. **Data Quality**
   - Implement constraints
   - Validate data during ETL
   - Monitor data consistency

4. **Maintenance**
   - Regular vacuum and analyze
   - Archive historical data
   - Update statistics

## 7. Common PostgreSQL Commands for DW Management

```sql
-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('fact_sales'));

-- Find missing indexes
SELECT schemaname, tablename, indexdef 
FROM pg_indexes 
WHERE tablename = 'fact_sales';

-- Analyze query performance
EXPLAIN ANALYZE 
SELECT * FROM fact_sales 
JOIN dim_customer ON fact_sales.customer_key = dim_customer.customer_key;

-- Vacuum and analyze
VACUUM ANALYZE fact_sales;
```

## 8. Monitoring and Maintenance

### Key Metrics to Monitor
- Fact table growth rate
- Query performance
- ETL duration
- Index usage statistics

### Regular Maintenance Tasks
1. Update table statistics
2. Rebuild indexes
3. Archive old data
4. Validate data quality

## 9. Common Analytical Queries

```sql
-- Sales by Category and Month
SELECT 
    d.month_name,
    p.category,
    SUM(f.sales_amount) as total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY d.month_name, p.category
ORDER BY d.month_name, p.category;

-- Customer Purchase Analysis
SELECT 
    c.customer_id,
    COUNT(DISTINCT f.sales_key) as number_of_purchases,
    SUM(f.sales_amount) as total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_id
ORDER BY total_spent DESC;
```
