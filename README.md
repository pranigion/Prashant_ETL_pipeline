## Technology Choices & Justifications

### Architecture Decision Matrix

| Component | Selected Technology | Why This Choice | Alternatives Considered | Why Not Selected |
|-----------|-------------------|-----------------|------------------------|------------------|
| **Orchestration** | **Apache Airflow** | Industry standard, Python-based, similar to ADF concepts (pipelines, scheduling, dependencies) | 1. Prefect<br>2. Dagster<br>3. Luigi | Airflow has largest community, most Azure-like with UI and scheduling |
| **Staging DB** | **PostgreSQL** | ACID compliant, reliable for raw data ingestion, similar to Azure SQL DB | 1. MySQL<br>2. SQLite<br>3. MinIO (file-based) | PostgreSQL offers better JSON support, window functions, and analytics features |
| **Analytics DB** | **PostgreSQL (separate instance)** | Optimized for OLAP queries, supports Star Schema, similar to Azure Synapse patterns | 1. ClickHouse<br>2. DuckDB<br>3. Same DB as staging | Separate DB simulates proper data lake layers, PostgreSQL handles dimensional models well |
| **Processing Engine** | **Python + Pandas/PySpark** |  Can scale to Spark if needed | 1. Pure SQL<br>2. dbt only<br>3. Scala | Python is most versatile, easier to showcase PII logic.
| **Containerization** | **Docker Compose** | Simple orchestration, runs on laptop, production-like | 1. Kubernetes<br>2. Individual containers | Docker Compose balances simplicity with realism for demos |


| Azure Experience   | This Project's Open-Source Equivalent |
|----------------------|--------------------------------------|
| Azure Data Factory | Apache Airflow (DAGs, scheduling, dependencies) |
| Azure SQL Database | PostgreSQL (staging layer) |
| Azure Synapse / Databricks | PostgreSQL Analytics + PySpark scripts |
| Medallion Architecture | Bronze → Silver → Gold layers implemented |
| Delta Lake | Simulated with versioned tables + timestamps |
| Unity Catalog / RLS | PostgreSQL schemas with access control |
| Star Schema Design | Fact & Dimension tables in Gold layer |
| PII Anonymization | SHA-256 hashing in transformation layer |
| Data Quality Checks | Custom Python validators in pipeline |


### Step 1: Verify Your Files

Open your folder in File Explorer.You should see:

This is the folder structure for project :

## 📁 Project Structure

```text
telecom-elt-pipeline/
├── docker-compose.yml
├── Dockerfile
│
├── data/
│   └── raw/
│       └── telecom_churn.csv
│
├── airflow/
│   ├── dags/
│   │   └── telecom_elt_pipeline.py
│   └── plugins/
│
├── sql/
│   ├── staging/
│   │   ├── 01-create-bronze-schema.sql
│   │   └── 02-create-silver-schema.sql
│   │
│   └── analytics/
│       └── 03-create-gold-schema.sql
│
├── spark/
│   └── jobs/
│       ├── silver_transformation.py
│       └── gold_star_schema.py
│
└── README.md
```





### Step 2: Open Command Prompt / Terminal

**Windows:**
- Press `Windows Key`
- Type "cmd"
- Press Enter

### Step 3: Navigate to Project Folder

cd C:\Users\YourName\Documents\telecom-elt-pipeline   # Windows

**Verify you're in the right place:**

dir        # Windows

You should see `docker-compose.yml` in the list.

### Step 4: Start Docker Containers

**Copy-paste this command:**

docker-compose up -d
docker-compose build

**What this does:**
- `docker-compose` = Tool to run multiple containers
- `up` = Start all services
- `-d` = Run in background (detached mode)

**You'll see output like:**
[+] Running 9/9
 ✔ Network telecom-elt-pipeline_pipeline-network   Created
 ✔ Volume "telecom-elt-pipeline_postgres-metadata-data"   Created
 ✔ Volume "telecom-elt-pipeline_postgres-staging-data"    Created
 ✔ Container postgres-metadata   Started
 ✔ Container postgres-staging    Started
 ✔ Container spark-master        Started
 ✔ Container airflow-init        Started
 ✔ Container airflow-webserver   Started
 ✔ Container airflow-scheduler   Started

**This will take 3-5 minutes the first time** (downloading Docker images)



### Step 5: Check if Everything is Running


docker-compose ps


You should see something like:
NAME                    STATUS
postgres-metadata       Up (healthy)
postgres-staging        Up (healthy)
postgres-analytics      Up (healthy)
spark-master            Up
spark-worker            Up
airflow-webserver       Up (healthy)
airflow-scheduler       Up (healthy)

**Wait until all show "healthy" or "Up"** (check every 30 seconds)

# Create Spark Connection

# 1. Delete the incomplete connection if any
docker-compose exec airflow-webserver airflow connections delete spark_default

# 2. Add the correct connection WITH "spark://"
docker-compose exec airflow-webserver airflow connections add spark_default --conn-type spark --conn-host "spark://spark-master" --conn-port 7077

## Configure Airflow (15 minutes)

### Step 6.1: Open Airflow Web UI

1. Open your browser (Chrome/Firefox)
2. Go to: **http://localhost:8080**
3. Login:
   - Username: `admin`
   - Password: `admin`

### Step 6.2: Create Database Connections

1. Click **"Admin"** (top menu)
2. Click **"Connections"**
3. Click **"+"** (plus sign) to add new connection

**Connection 1: postgres_staging**

Fill in these fields:
- Connection Id: `postgres_staging`
- Connection Type: Select **"Postgres"** from dropdown
- Host: `postgres-staging`
- Schema: `telecom_staging`
- Login: `staging_user`
- Password: `staging_pass`
- Port: `5432`
- Click **"Save"**

**Connection 2: postgres_analytics**

Click **"+"** again, fill in:
- Connection Id: `postgres_analytics`
- Connection Type: **"Postgres"**
- Host: `postgres-analytics`
- Schema: `telecom_analytics`
- Login: `analytics_user`
- Password: `analytics_pass`
- Port: `5432`
- Click **"Save"**

# Or Run below docker commands to create connection :

docker-compose exec airflow-webserver airflow connections add postgres_staging --conn-type postgres --conn-host postgres-staging --conn-schema telecom_staging --conn-login staging_user --conn-password staging_pass --conn-port 5432

docker-compose exec airflow-webserver airflow connections add postgres_analytics --conn-type postgres --conn-host postgres-analytics --conn-schema telecom_analytics --conn-login analytics_user --conn-password analytics_pass --conn-port 5432



### Step 6.3: Enable the DAG

1. Go back to **"DAGs"** (top menu)
2. Find: `telecom_elt_pyspark_pipeline`
3. Toggle the switch ON (should turn blue/green)

---

## 7: Run the Pipeline! (10 minutes)

### Step 7.1: Trigger the Pipeline

1. In Airflow UI, find your DAG: `telecom_elt_pyspark_pipeline`
2. Click the **"Play"** button on the right
3. Click **"Trigger DAG"**

### Step 7.2: Watch it Run

1. Click on the DAG name to open details
2. Click **"Graph"** view
3. Watch the boxes turn:
   - **Yellow** = Running
   - **Green** = Success
   - **Red** = Failed

**Expected time:**
- bronze_ingest: 2 minutes
- silver_pyspark: 3 minutes
- gold_pyspark: 3 minutes
- validate_gold: 1 minute
- **Total: ~9 minutes**

Check in DBeaver:-

Here are the exact connection details for both your Gold (Analytics) and Silver (Staging) databases.

1. Connect to Gold Layer (Analytics DB)
This is where your final Star Schema lives.

Open DBeaver.

Click New Database Connection (Plug icon with a +).

Select PostgreSQL and click Next.

Enter these details:

Host: localhost

Port: 5433 (Note: It is 5433, not the default 5432, because we mapped it there)

Database: telecom_analytics

Username: analytics_user

Password: analytics_pass

Click Test Connection (it should say "Connected").

Click Finish.

What to check inside:

Expand Databases -> telecom_analytics -> Schemas -> gold -> Tables.

You should see 4 tables: dim_customer, dim_contract, dim_service, and fact_customer_churn.

2. Connect to Silver Layer (Staging DB)
This is where your cleansed, flat data lives.

Click New Database Connection again.

Select PostgreSQL.

Enter these details:

Host: localhost

Port: 5432 (This uses the default port)

Database: telecom_staging

Username: staging_user

Password: staging_pass

Click Test Connection -> Finish.

What to check inside:

Expand Databases -> telecom_staging -> Schemas -> silver -> Tables.

You should see cleansed_telecom_churn.

3. Run a Test Query in DBeaver
To verify  Star Schema really works, open a SQL Editor on  Gold connection (Port 5433) and paste this query. It joins the Fact table to the Dimensions:

SQL
SELECT 
    c.contract_type,
    s.internet_service,
    COUNT(f.customer_sk) as customer_count,
    CAST(SUM(CASE WHEN f.is_churned THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 as churn_rate_percent
FROM gold.fact_customer_churn f
JOIN gold.dim_contract c ON f.contract_sk = c.contract_sk
JOIN gold.dim_service s ON f.service_sk = s.service_sk
GROUP BY c.contract_type, s.internet_service
ORDER BY churn_rate_percent DESC;








