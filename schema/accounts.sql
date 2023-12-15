CREATE TABLE IF NOT EXISTS accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address_1 VARCHAR(62),
    address_2 VARCHAR(62),
    city VARCHAR(25),
    state VARCHAR(25),
    zip_code VARCHAR(5),
    join_date DATE
);