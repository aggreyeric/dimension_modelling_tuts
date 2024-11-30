-- OLTP Schema for a Retail System

-- Create OLTP Tables
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description TEXT
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INT REFERENCES categories(category_id),
    supplier_id INT,
    unit_price DECIMAL(10,2) NOT NULL,
    units_in_stock INT NOT NULL,
    discontinued BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    title VARCHAR(50),
    hire_date DATE NOT NULL,
    reports_to INT REFERENCES employees(employee_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    manager_id INT REFERENCES employees(employee_id),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    employee_id INT REFERENCES employees(employee_id),
    store_id INT REFERENCES stores(store_id),
    order_date TIMESTAMP NOT NULL,
    required_date TIMESTAMP,
    shipped_date TIMESTAMP,
    status VARCHAR(20) DEFAULT 'Pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_details (
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    unit_price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    discount DECIMAL(4,2) DEFAULT 0,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE inventory_transactions (
    transaction_id SERIAL PRIMARY KEY,
    store_id INT REFERENCES stores(store_id),
    product_id INT REFERENCES products(product_id),
    transaction_type VARCHAR(20) NOT NULL, -- 'IN' or 'OUT'
    quantity INT NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_id INT, -- Can be order_id for outbound transactions
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better OLTP performance
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_employee ON orders(employee_id);
CREATE INDEX idx_orders_store ON orders(store_id);
CREATE INDEX idx_order_details_product ON order_details(product_id);
CREATE INDEX idx_inventory_store_product ON inventory_transactions(store_id, product_id);

-- Insert sample data
INSERT INTO categories (category_name, description) VALUES
('Electronics', 'Electronic devices and accessories'),
('Clothing', 'Apparel and fashion items'),
('Books', 'Books and publications'),
('Home & Garden', 'Home improvement and garden supplies');

INSERT INTO products (product_name, category_id, supplier_id, unit_price, units_in_stock) VALUES
('Smartphone X', 1, 1, 699.99, 100),
('Laptop Pro', 1, 1, 1299.99, 50),
('T-Shirt Basic', 2, 2, 19.99, 200),
('Garden Tools Set', 4, 3, 89.99, 30),
('Programming Book', 3, 4, 49.99, 75);

INSERT INTO customers (first_name, last_name, email, phone, address, city, state, country, postal_code) VALUES
('John', 'Doe', 'john.doe@email.com', '123-456-7890', '123 Main St', 'New York', 'NY', 'USA', '10001'),
('Jane', 'Smith', 'jane.smith@email.com', '098-765-4321', '456 Oak Ave', 'Los Angeles', 'CA', 'USA', '90001'),
('Bob', 'Johnson', 'bob.johnson@email.com', '555-555-5555', '789 Pine Rd', 'Chicago', 'IL', 'USA', '60601');

INSERT INTO employees (first_name, last_name, title, hire_date) VALUES
('Michael', 'Scott', 'Store Manager', '2020-01-01'),
('Jim', 'Halpert', 'Sales Representative', '2020-02-15'),
('Pam', 'Beesly', 'Customer Service', '2020-03-01');

INSERT INTO stores (store_name, manager_id, address, city, state, country, postal_code) VALUES
('Downtown Store', 1, '100 Retail Ave', 'New York', 'NY', 'USA', '10001'),
('Mall Location', 1, '200 Shopping Ctr', 'Los Angeles', 'CA', 'USA', '90001');

-- Insert sample orders
WITH new_order AS (
    INSERT INTO orders (customer_id, employee_id, store_id, order_date, required_date, status)
    VALUES (1, 2, 1, '2023-01-01 10:00:00', '2023-01-03 10:00:00', 'Completed')
    RETURNING order_id
)
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
SELECT 
    new_order.order_id,
    1,
    699.99,
    1,
    0
FROM new_order;

WITH new_order AS (
    INSERT INTO orders (customer_id, employee_id, store_id, order_date, required_date, status)
    VALUES (2, 2, 1, '2023-01-02 11:00:00', '2023-01-04 11:00:00', 'Completed')
    RETURNING order_id
)
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
SELECT 
    new_order.order_id,
    2,
    1299.99,
    1,
    0.1
FROM new_order;

-- Insert sample inventory transactions
INSERT INTO inventory_transactions (store_id, product_id, transaction_type, quantity)
VALUES
(1, 1, 'IN', 50),
(1, 2, 'IN', 25),
(1, 1, 'OUT', 1),
(1, 2, 'OUT', 1);
