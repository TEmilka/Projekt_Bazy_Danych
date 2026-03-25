-- ======================
-- CUSTOMERS
-- ======================
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    pesel VARCHAR(11) UNIQUE NOT NULL,
    phone VARCHAR(20)
);

-- ======================
-- PRODUCTS
-- ======================
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    is_prescription_required BOOLEAN NOT NULL DEFAULT FALSE,
    barcode VARCHAR(50) UNIQUE,
    price NUMERIC(10,2) NOT NULL,
    category VARCHAR(100)
);

-- ======================
-- EMPLOYEES
-- ======================
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    job_position VARCHAR(100) NOT NULL,
    salary NUMERIC(10,2) NOT NULL
);

-- ======================
-- SALES
-- ======================
CREATE TABLE sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    employee_id INTEGER REFERENCES employees(employee_id) NOT NULL,
    sale_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_value NUMERIC(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- ======================
-- SALE ITEMS
-- ======================
CREATE TABLE sale_items (
    sale_item_id SERIAL PRIMARY KEY,
    sale_id INTEGER NOT NULL REFERENCES sales(sale_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    product_quantity INTEGER NOT NULL CHECK (product_quantity > 0)
);

-- ======================
-- PRESCRIPTIONS
-- ======================
CREATE TABLE prescriptions (
    prescription_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    issue_date TIMESTAMP NOT NULL,
    expiry_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL
);