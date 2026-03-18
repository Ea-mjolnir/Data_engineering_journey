CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    quantity INTEGER,
    price DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (customer_name, product, quantity, price) VALUES
    ('John Doe', 'Laptop', 1, 999.99),
    ('Jane Smith', 'Mouse', 2, 29.99),
    ('Bob Johnson', 'Keyboard', 1, 79.99);
