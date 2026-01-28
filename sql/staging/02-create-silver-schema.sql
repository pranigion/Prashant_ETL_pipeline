CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.cleansed_telecom_churn CASCADE;

CREATE TABLE silver.cleansed_telecom_churn (
    customer_id_anonymized VARCHAR(64) PRIMARY KEY,
    age INTEGER,
    gender VARCHAR(20),
    tenure INTEGER,
    monthly_charges NUMERIC(10,2),
    contract_type VARCHAR(50),
    internet_service VARCHAR(50),
    total_charges NUMERIC(10,2),
    tech_support VARCHAR(50),
    churn VARCHAR(10),
    avg_monthly_spend NUMERIC(10,2),
    transformation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_pipeline VARCHAR(255),
    data_quality_score NUMERIC(3,2)
);

CREATE INDEX idx_silver_transform ON silver.cleansed_telecom_churn(transformation_timestamp);
CREATE INDEX idx_silver_churn ON silver.cleansed_telecom_churn(churn);