CREATE TABLE customers (
    invoice_no VARCHAR(20),
    customer_id VARCHAR(20),
    gender ENUM('Male', 'Female'),
    age INT,
    category VARCHAR(50),
    quantity INT,    price DECIMAL(10,2),
    payment_method VARCHAR(20),
    invoice_date VARCHAR(20), 
    shopping_mall VARCHAR(100)
);
