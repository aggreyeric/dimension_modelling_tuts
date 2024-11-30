-- Example ETL Process for migrating from OLTP to OLAP

-- Step 1: Create staging tables
CREATE TABLE stg_orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
);

CREATE TABLE stg_order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
);

-- Step 2: ETL Functions

-- Function to load customer dimension
CREATE OR REPLACE FUNCTION load_customer_dimension()
RETURNS void AS $$
BEGIN
    -- Insert new customers
    INSERT INTO dim_customer (
        customer_id,
        first_name,
        last_name,
        email,
        effective_date,
        expiry_date,
        is_current
    )
    SELECT DISTINCT
        c.customer_id,
        split_part(c.name, ' ', 1),
        split_part(c.name, ' ', 2),
        c.email,
        CURRENT_DATE,
        '9999-12-31'::DATE,
        TRUE
    FROM customers c
    LEFT JOIN dim_customer dc ON c.customer_id = dc.customer_id
    WHERE dc.customer_id IS NULL;

    -- Handle changes (SCD Type 2)
    UPDATE dim_customer
    SET expiry_date = CURRENT_DATE - 1,
        is_current = FALSE
    WHERE customer_id IN (
        SELECT c.customer_id
        FROM customers c
        JOIN dim_customer dc ON c.customer_id = dc.customer_id
        WHERE dc.is_current = TRUE
        AND (
            c.name != (dc.first_name || ' ' || dc.last_name)
            OR c.email != dc.email
        )
    );
END;
$$ LANGUAGE plpgsql;

-- Function to load fact sales
CREATE OR REPLACE FUNCTION load_fact_sales()
RETURNS void AS $$
BEGIN
    INSERT INTO fact_sales (
        date_key,
        customer_key,
        product_key,
        quantity,
        unit_price,
        sales_amount,
        profit_amount
    )
    SELECT 
        d.date_key,
        dc.customer_key,
        dp.product_key,
        oi.quantity,
        oi.unit_price,
        (oi.quantity * oi.unit_price) as sales_amount,
        ((oi.quantity * oi.unit_price) * 0.2) as profit_amount -- Example profit calculation
    FROM stg_orders o
    JOIN stg_order_items oi ON o.order_id = oi.order_id
    JOIN dim_date d ON o.order_date = d.full_date
    JOIN dim_customer dc ON o.customer_id = dc.customer_id
    JOIN dim_product dp ON oi.product_id = dp.product_id
    WHERE dc.is_current = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Example of running the ETL process
DO $$
BEGIN
    -- 1. Truncate staging tables
    TRUNCATE TABLE stg_orders, stg_order_items;
    
    -- 2. Load staging tables (example)
    INSERT INTO stg_orders 
    SELECT * FROM orders WHERE order_date >= (SELECT COALESCE(MAX(transaction_time::date), '1900-01-01') FROM fact_sales);
    
    INSERT INTO stg_order_items 
    SELECT oi.* 
    FROM order_items oi 
    JOIN stg_orders o ON oi.order_id = o.order_id;
    
    -- 3. Load dimensions
    PERFORM load_customer_dimension();
    
    -- 4. Load facts
    PERFORM load_fact_sales();
    
    -- 5. Clean up
    TRUNCATE TABLE stg_orders, stg_order_items;
END $$;
