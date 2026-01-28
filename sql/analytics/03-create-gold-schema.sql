CREATE SCHEMA IF NOT EXISTS gold;

-- Dimension: Customer
DROP TABLE IF EXISTS gold.dim_customer CASCADE;
CREATE TABLE gold.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id_anonymized VARCHAR(64) UNIQUE NOT NULL,
    age INTEGER,
    gender VARCHAR(20),
    tenure_months INTEGER,
    tenure_category VARCHAR(20),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Dimension: Service
DROP TABLE IF EXISTS gold.dim_service CASCADE;
CREATE TABLE gold.dim_service (
    service_sk SERIAL PRIMARY KEY,
    internet_service VARCHAR(50),
    tech_support VARCHAR(50),
    service_bundle_type VARCHAR(50)
);

-- Dimension: Contract
DROP TABLE IF EXISTS gold.dim_contract CASCADE;
CREATE TABLE gold.dim_contract (
    contract_sk SERIAL PRIMARY KEY,
    contract_type VARCHAR(50),
    contract_duration_months INTEGER
);

-- Fact Table
DROP TABLE IF EXISTS gold.fact_customer_churn CASCADE;
CREATE TABLE gold.fact_customer_churn (
    fact_sk SERIAL PRIMARY KEY,
    customer_sk INTEGER REFERENCES gold.dim_customer(customer_sk),
    service_sk INTEGER REFERENCES gold.dim_service(service_sk),
    contract_sk INTEGER REFERENCES gold.dim_contract(contract_sk),
    date_sk INTEGER,
    monthly_charges NUMERIC(10,2),
    total_charges NUMERIC(10,2),
    avg_monthly_spend NUMERIC(10,2),
    total_revenue NUMERIC(12,2),
    is_churned BOOLEAN,
    churn_risk_score NUMERIC(3,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_customer ON gold.fact_customer_churn(customer_sk);
CREATE INDEX idx_fact_service ON gold.fact_customer_churn(service_sk);
CREATE INDEX idx_fact_contract ON gold.fact_customer_churn(contract_sk);
CREATE INDEX idx_fact_churned ON gold.fact_customer_churn(is_churned);