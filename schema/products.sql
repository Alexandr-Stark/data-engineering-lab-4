CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(5) UNIQUE,
    product_description TEXT
);

CREATE INDEX IF NOT EXISTS idx_product_code ON products(product_code);