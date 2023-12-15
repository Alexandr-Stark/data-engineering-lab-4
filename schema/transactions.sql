CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(27) PRIMARY KEY,
    transaction_date DATE,
    product_id INT,
    product_code VARCHAR(5),
    product_description TEXT,
    quantity INT,
    account_id INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
);

CREATE INDEX IF NOT EXISTS idx_transaction_date ON transactions(transaction_date);