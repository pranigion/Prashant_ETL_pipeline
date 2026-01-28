CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.raw_telecom_churn CASCADE;

CREATE TABLE bronze.raw_telecom_churn (
    CustomerID INTEGER,
    Age INTEGER,
    Gender VARCHAR(20),
    Tenure INTEGER,
    MonthlyCharges NUMERIC(10,2),
    ContractType VARCHAR(50),
    InternetService VARCHAR(50),
    TotalCharges NUMERIC(10,2),
    TechSupport VARCHAR(50),
    Churn VARCHAR(10),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    pipeline_run_id VARCHAR(255)
);

CREATE INDEX idx_bronze_ingestion ON bronze.raw_telecom_churn(ingestion_timestamp);