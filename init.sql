-- Create Dimension Tables

-- Date Dimension
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    brand VARCHAR(50) NOT NULL,
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL
);

-- Customer Dimension
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL
);

-- Store Dimension
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(100) NOT NULL,
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    manager VARCHAR(100),
    effective_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL
);

-- Fact Tables

-- Sales Fact Table
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    product_key INT REFERENCES dim_product(product_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    store_key INT REFERENCES dim_store(store_key),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL,
    sales_amount DECIMAL(10,2) NOT NULL,
    profit_amount DECIMAL(10,2) NOT NULL,
    transaction_id VARCHAR(50) NOT NULL,
    transaction_time TIMESTAMP NOT NULL
);

-- Inventory Fact Table
CREATE TABLE fact_inventory (
    inventory_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    product_key INT REFERENCES dim_product(product_key),
    store_key INT REFERENCES dim_store(store_key),
    quantity_on_hand INT NOT NULL,
    quantity_received INT NOT NULL,
    quantity_sold INT NOT NULL,
    stock_value DECIMAL(10,2) NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);

CREATE INDEX idx_fact_inventory_date ON fact_inventory(date_key);
CREATE INDEX idx_fact_inventory_product ON fact_inventory(product_key);
CREATE INDEX idx_fact_inventory_store ON fact_inventory(store_key);

-- Create a function to populate date dimension
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
            day,
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
            EXTRACT(DAY FROM loop_date),
            EXTRACT(DOW FROM loop_date),
            TO_CHAR(loop_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM loop_date) IN (0, 6) THEN TRUE ELSE FALSE END
        );
        loop_date := loop_date + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension for 5 years
SELECT populate_dim_date('2020-01-01'::DATE, '2024-12-31'::DATE);

-- Sample data insertion procedures
CREATE OR REPLACE FUNCTION insert_sample_data()
RETURNS void AS $$
BEGIN
    -- Insert sample product data
    INSERT INTO dim_product (product_id, product_name, category, subcategory, unit_price, brand, effective_date, expiry_date, is_current)
    VALUES 
    ('P001', 'Laptop Pro', 'Electronics', 'Computers', 999.99, 'TechBrand', '2023-01-01', '9999-12-31', true),
    ('P002', 'Smart Watch', 'Electronics', 'Wearables', 199.99, 'TechBrand', '2023-01-01', '9999-12-31', true);

    -- Insert sample customer data
    INSERT INTO dim_customer (customer_id, first_name, last_name, email, phone, address, city, state, country, postal_code, effective_date, expiry_date, is_current)
    VALUES 
    ('C001', 'John', 'Doe', 'john@example.com', '123-456-7890', '123 Main St', 'New York', 'NY', 'USA', '10001', '2023-01-01', '9999-12-31', true),
    ('C002', 'Jane', 'Smith', 'jane@example.com', '098-765-4321', '456 Oak St', 'Los Angeles', 'CA', 'USA', '90001', '2023-01-01', '9999-12-31', true);

    -- Insert sample store data
    INSERT INTO dim_store (store_id, store_name, address, city, state, country, postal_code, manager, effective_date, expiry_date, is_current)
    VALUES 
    ('S001', 'Downtown Store', '789 Market St', 'San Francisco', 'CA', 'USA', '94105', 'Mike Johnson', '2023-01-01', '9999-12-31', true),
    ('S002', 'Mall Store', '321 Shopping Ave', 'Chicago', 'IL', 'USA', '60601', 'Sarah Williams', '2023-01-01', '9999-12-31', true);

    -- Insert sample sales data
    INSERT INTO fact_sales (date_key, product_key, customer_key, store_key, quantity, unit_price, discount_amount, sales_amount, profit_amount, transaction_id, transaction_time)
    SELECT 
        d.date_key,
        1,
        1,
        1,
        2,
        999.99,
        0,
        1999.98,
        400.00,
        'T001',
        '2023-01-01 10:00:00'::TIMESTAMP
    FROM dim_date d
    WHERE d.full_date = '2023-01-01';

    -- Insert sample inventory data
    INSERT INTO fact_inventory (date_key, product_key, store_key, quantity_on_hand, quantity_received, quantity_sold, stock_value)
    SELECT 
        d.date_key,
        1,
        1,
        100,
        50,
        2,
        99999.00
    FROM dim_date d
    WHERE d.full_date = '2023-01-01';
END;
$$ LANGUAGE plpgsql;

-- Execute sample data insertion
SELECT insert_sample_data();

-- Create some useful views for analysis
CREATE VIEW vw_daily_sales AS
SELECT 
    d.full_date,
    p.product_name,
    c.first_name || ' ' || c.last_name as customer_name,
    s.store_name,
    fs.quantity,
    fs.sales_amount,
    fs.profit_amount
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
JOIN dim_product p ON fs.product_key = p.product_key
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_store s ON fs.store_key = s.store_key;

CREATE VIEW vw_inventory_status AS
SELECT 
    d.full_date,
    p.product_name,
    s.store_name,
    fi.quantity_on_hand,
    fi.stock_value
FROM fact_inventory fi
JOIN dim_date d ON fi.date_key = d.date_key
JOIN dim_product p ON fi.product_key = p.product_key
JOIN dim_store s ON fi.store_key = s.store_key;
