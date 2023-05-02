
-- Create the Transactions table
CREATE TABLE IF NOT EXISTS Transactions (
    unique_transaction_id INTEGER PRIMARY KEY,
    timestamp TIMESTAMP,
    drink TEXT,
    cost REAL,
    location TEXT,
    drink_id INTEGER
);
DELETE FROM Transactions;


-- Create the Bar_Data table
CREATE TABLE IF NOT EXISTS Bar_Data (
    glass_id INTEGER PRIMARY KEY,
    glass_type TEXT,
    stock INTEGER,
    location TEXT
);
DELETE FROM Bar_Data;

-- Create the DIM_Glasses table
CREATE TABLE IF NOT EXISTS DIM_Glasses (
    drink_id INTEGER PRIMARY KEY,
    drink TEXT,
    api_id INTEGER,
    glass TEXT,
    glass_id INTEGER
);
DELETE FROM DIM_Glasses;