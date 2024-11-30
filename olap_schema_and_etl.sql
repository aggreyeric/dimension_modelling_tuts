-- OLAP Schema and ETL Process

-- First, create the dimension tables
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT NOT NULL, -- business key
    product_name VARCHAR(100) NOT NULL,
    category_name VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL
);

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL, -- business key
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL
);

CREATE TABLE dim_employee (
    employee_key SERIAL PRIMARY KEY,
    employee_id INT NOT NULL, -- business key
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    title VARCHAR(50),
    hire_date DATE NOT NULL,
    supervisor_key INT REFERENCES dim_employee(employee_key),
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL
);

CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id INT NOT NULL, -- business key
    store_name VARCHAR(100) NOT NULL,
    manager_key INT REFERENCES dim_employee(employee_key),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL
);

-- Create fact tables
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    product_key INT REFERENCES dim_product(product_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    employee_key INT REFERENCES dim_employee(employee_key),
    store_key INT REFERENCES dim_store(store_key),
    order_id INT NOT NULL, -- business key
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL,
    sales_amount DECIMAL(10,2) NOT NULL,
    cost_amount DECIMAL(10,2) NOT NULL,
    profit_amount DECIMAL(10,2) NOT NULL
);

CREATE TABLE fact_inventory (
    inventory_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    product_key INT REFERENCES dim_product(product_key),
    store_key INT REFERENCES dim_store(store_key),
    quantity_on_hand INT NOT NULL,
    quantity_received INT NOT NULL,
    quantity_sold INT NOT NULL,
    quantity_on_order INT NOT NULL,
    stock_value DECIMAL(10,2) NOT NULL
);

-- Create indexes for the fact tables
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_employee ON fact_sales(employee_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);

CREATE INDEX idx_fact_inventory_date ON fact_inventory(date_key);
CREATE INDEX idx_fact_inventory_product ON fact_inventory(product_key);
CREATE INDEX idx_fact_inventory_store ON fact_inventory(store_key);

-- ETL Functions

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS void AS $$
DECLARE
    loop_date DATE;
BEGIN
    loop_date := start_date;
    WHILE loop_date <= end_date LOOP
        INSERT INTO dim_date (
            full_date,
            year,
            quarter,
            month,
            month_name,
            week,
            day_of_week,
            day_name,
            is_weekend
        )
        VALUES (
            loop_date,
            EXTRACT(YEAR FROM loop_date),
            EXTRACT(QUARTER FROM loop_date),
            EXTRACT(MONTH FROM loop_date),
            TO_CHAR(loop_date, 'Month'),
            EXTRACT(WEEK FROM loop_date),
            EXTRACT(DOW FROM loop_date),
            TO_CHAR(loop_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM loop_date) IN (0, 6) THEN TRUE ELSE FALSE END
        );
        loop_date := loop_date + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to load product dimension (SCD Type 2)
CREATE OR REPLACE FUNCTION load_dim_product()
RETURNS void AS $$
BEGIN
    -- Insert new products
    INSERT INTO dim_product (
        product_id,
        product_name,
        category_name,
        unit_price,
        effective_date,
        expiry_date,
        is_current,
        version
    )
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        p.unit_price,
        CURRENT_DATE,
        '9999-12-31'::DATE,
        TRUE,
        1
    FROM products p
    JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN dim_product dp ON p.product_id = dp.product_id AND dp.is_current = TRUE
    WHERE dp.product_id IS NULL;

    -- Handle changes (SCD Type 2)
    WITH changed_products AS (
        SELECT 
            p.product_id,
            p.product_name,
            c.category_name,
            p.unit_price,
            dp.product_key,
            dp.version
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN dim_product dp ON p.product_id = dp.product_id AND dp.is_current = TRUE
        WHERE p.product_name != dp.product_name 
           OR c.category_name != dp.category_name 
           OR p.unit_price != dp.unit_price
    )
    UPDATE dim_product dp
    SET expiry_date = CURRENT_DATE - 1,
        is_current = FALSE
    FROM changed_products cp
    WHERE dp.product_key = cp.product_key;

    INSERT INTO dim_product (
        product_id,
        product_name,
        category_name,
        unit_price,
        effective_date,
        expiry_date,
        is_current,
        version
    )
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        p.unit_price,
        CURRENT_DATE,
        '9999-12-31'::DATE,
        TRUE,
        dp.version + 1
    FROM products p
    JOIN categories c ON p.category_id = c.category_id
    JOIN dim_product dp ON p.product_id = dp.product_id AND dp.is_current = FALSE
    WHERE EXISTS (
        SELECT 1 
        FROM changed_products cp 
        WHERE cp.product_id = p.product_id
    );
END;
$$ LANGUAGE plpgsql;

-- Function to load fact sales
CREATE OR REPLACE FUNCTION load_fact_sales()
RETURNS void AS $$
BEGIN
    INSERT INTO fact_sales (
        date_key,
        product_key,
        customer_key,
        employee_key,
        store_key,
        order_id,
        quantity,
        unit_price,
        discount_amount,
        sales_amount,
        cost_amount,
        profit_amount
    )
    SELECT 
        dd.date_key,
        dp.product_key,
        dc.customer_key,
        de.employee_key,
        ds.store_key,
        o.order_id,
        od.quantity,
        od.unit_price,
        od.unit_price * od.quantity * od.discount,
        od.unit_price * od.quantity * (1 - od.discount),
        od.unit_price * od.quantity * 0.7, -- Assuming 70% cost
        od.unit_price * od.quantity * (1 - od.discount) * 0.3 -- Assuming 30% profit margin
    FROM orders o
    JOIN order_details od ON o.order_id = od.order_id
    JOIN dim_date dd ON DATE(o.order_date) = dd.full_date
    JOIN dim_product dp ON od.product_id = dp.product_id AND dp.is_current = TRUE
    JOIN dim_customer dc ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
    JOIN dim_employee de ON o.employee_id = de.employee_id AND de.is_current = TRUE
    JOIN dim_store ds ON o.store_id = ds.store_id AND ds.is_current = TRUE
    LEFT JOIN fact_sales fs ON o.order_id = fs.order_id
    WHERE fs.order_id IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to load fact inventory
CREATE OR REPLACE FUNCTION load_fact_inventory()
RETURNS void AS $$
BEGIN
    INSERT INTO fact_inventory (
        date_key,
        product_key,
        store_key,
        quantity_on_hand,
        quantity_received,
        quantity_sold,
        quantity_on_order,
        stock_value
    )
    SELECT 
        dd.date_key,
        dp.product_key,
        ds.store_key,
        COALESCE(SUM(CASE WHEN it.transaction_type = 'IN' THEN it.quantity ELSE -it.quantity END), 0),
        COALESCE(SUM(CASE WHEN it.transaction_type = 'IN' THEN it.quantity ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN it.transaction_type = 'OUT' THEN it.quantity ELSE 0 END), 0),
        0, -- Quantity on order would come from a purchase order table
        COALESCE(SUM(CASE WHEN it.transaction_type = 'IN' THEN it.quantity ELSE -it.quantity END), 0) * dp.unit_price
    FROM inventory_transactions it
    JOIN dim_date dd ON DATE(it.transaction_date) = dd.full_date
    JOIN dim_product dp ON it.product_id = dp.product_id AND dp.is_current = TRUE
    JOIN dim_store ds ON it.store_id = ds.store_id AND ds.is_current = TRUE
    GROUP BY dd.date_key, dp.product_key, ds.store_key, dp.unit_price;
END;
$$ LANGUAGE plpgsql;

-- Main ETL procedure
CREATE OR REPLACE PROCEDURE run_etl()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Ensure date dimension is populated
    PERFORM populate_dim_date('2020-01-01'::DATE, '2025-12-31'::DATE);
    
    -- Load dimensions
    PERFORM load_dim_product();
    -- Add similar calls for other dimensions
    
    -- Load facts
    PERFORM load_fact_sales();
    PERFORM load_fact_inventory();
    
    COMMIT;
END;
$$;

-- Create some useful views for analysis
CREATE VIEW vw_sales_analysis AS
SELECT 
    dd.year,
    dd.month_name,
    dp.product_name,
    dp.category_name,
    dc.first_name || ' ' || dc.last_name as customer_name,
    ds.store_name,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.sales_amount) as total_sales,
    SUM(fs.profit_amount) as total_profit
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_store ds ON fs.store_key = ds.store_key
GROUP BY 
    dd.year,
    dd.month_name,
    dp.product_name,
    dp.category_name,
    customer_name,
    ds.store_name;
