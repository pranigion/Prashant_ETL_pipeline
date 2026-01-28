"""
Gold Layer: Star Schema Creation using PySpark
Author: Prashant Pathak
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, row_number, round as spark_round
)
from pyspark.sql.window import Window
import sys

def create_spark_session():
    spark = SparkSession.builder \
        .appName("GoldStarSchema") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_jdbc_props(db):
    if db == "staging":
        return {
            "url": "jdbc:postgresql://postgres-staging:5432/telecom_staging",
            "user": "staging_user",     # can be kept in key vault or encrypted
            "password": "staging_pass",    # can be kept in key vault or encrypted
            "driver": "org.postgresql.Driver"
        }
    else:
        return {
            "url": "jdbc:postgresql://postgres-analytics:5432/telecom_analytics",
            "user": "analytics_user",      # can be kept in key vault or encrypted form
            "password": "analytics_pass",    # can be kept in key vault or encrypted form
            "driver": "org.postgresql.Driver"
        }

def create_gold_star_schema():
    print("="*70)
    print("GOLD LAYER - Star Schema Creation")
    print("="*70)
    
    spark = create_spark_session()
    
    try:
        props_staging = get_jdbc_props("staging")
        props_analytics = get_jdbc_props("analytics")
        
        # --- STEP 1: READ DATA ---
        silver_df = spark.read.format("jdbc") \
            .option("url", props_staging["url"]) \
            .option("dbtable", "silver.cleansed_telecom_churn") \
            .option("user", props_staging["user"]) \
            .option("password", props_staging["password"]) \
            .option("driver", props_staging["driver"]) \
            .load()
        
        print(f"Read {silver_df.count():,} rows from Silver")
        
        # --- STEP 2: PREPARE DIMENSIONS (IN MEMORY) ---
        
        # 2a. Dim Customer
        dim_customer = silver_df.select(
            col("customer_id_anonymized"), col("age"), col("gender"), col("tenure").alias("tenure_months")
        ).distinct()
        
        dim_customer = dim_customer.withColumn(
            "tenure_category",
            when(col("tenure_months") <= 12, "0-12 months")
            .when(col("tenure_months") <= 24, "13-24 months")
            .when(col("tenure_months") <= 48, "25-48 months")
            .otherwise("49+ months")
        )
        window_cust = Window.orderBy("customer_id_anonymized")
        dim_customer = dim_customer.withColumn("customer_sk", row_number().over(window_cust)) \
            .withColumn("effective_date", current_timestamp()) \
            .withColumn("is_current", lit(True)) \
            .select("customer_sk", "customer_id_anonymized", "age", "gender", "tenure_months", "tenure_category", "effective_date", "is_current")
        
        # 2b. Dim Service
        dim_service = silver_df.select("internet_service", "tech_support").distinct()
        dim_service = dim_service.withColumn(
            "service_bundle_type",
            when((col("internet_service") != "unknown") & (col("tech_support") == "yes"), "Premium")
            .when(col("internet_service") != "unknown", "Standard")
            .otherwise("Basic")
        )
        window_svc = Window.orderBy("internet_service")
        dim_service = dim_service.withColumn("service_sk", row_number().over(window_svc))

        # 2c. Dim Contract
        dim_contract = silver_df.select("contract_type").distinct()
        dim_contract = dim_contract.withColumn(
            "contract_duration_months",
            when(col("contract_type") == "month-to-month", 1)
            .when(col("contract_type") == "one-year", 12)
            .when(col("contract_type") == "two-year", 24)
            .otherwise(0)
        )
        window_con = Window.orderBy("contract_type")
        dim_contract = dim_contract.withColumn("contract_sk", row_number().over(window_con))

        # --- STEP 3: PREPARE FACT TABLE (IN MEMORY) ---
        fact = silver_df.join(dim_customer, "customer_id_anonymized") \
            .join(dim_service, ["internet_service", "tech_support"]) \
            .join(dim_contract, "contract_type")
            
        fact = fact.withColumn("total_revenue", spark_round(col("tenure") * col("monthly_charges"), 2)) \
            .withColumn("is_churned", when(col("churn") == "yes", True).otherwise(False)) \
            .withColumn("churn_risk_score", 
                        when(col("is_churned"), lit(1.0))
                        .when(col("tenure") < 12, lit(0.7))
                        .otherwise(lit(0.3)))
        
        window_fact = Window.orderBy("customer_sk")
        fact = fact.withColumn("fact_sk", row_number().over(window_fact)) \
            .withColumn("date_sk", lit(1)) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("updated_timestamp", current_timestamp()) \
            .select("fact_sk", "customer_sk", "service_sk", "contract_sk", "date_sk", 
                    "monthly_charges", "total_charges", "avg_monthly_spend", "total_revenue", 
                    "is_churned", "churn_risk_score", "created_timestamp", "updated_timestamp")

        # --- STEP 4: WRITE DATA (CRITICAL ORDERING) ---
        
        #  We MUST drop the Fact table first to release constraints on Dimensions.
        # We do this by overwriting it with an empty dataframe of the same schema.
        print("Resetting Fact Table to release constraints...")
        spark.createDataFrame([], fact.schema).write.format("jdbc") \
            .option("url", props_analytics["url"]) \
            .option("dbtable", "gold.fact_customer_churn") \
            .option("user", props_analytics["user"]) \
            .option("password", props_analytics["password"]) \
            .option("driver", props_analytics["driver"]) \
            .mode("overwrite") \
            .save()
        print("✓ Fact table cleared.")

        # Now we can safely overwrite Dimensions
        def write_to_postgres(df, table):
            df.write.format("jdbc") \
                .option("url", props_analytics["url"]) \
                .option("dbtable", f"gold.{table}") \
                .option("user", props_analytics["user"]) \
                .option("password", props_analytics["password"]) \
                .option("driver", props_analytics["driver"]) \
                .mode("overwrite") \
                .save()
            print(f"Wrote {df.count():,} rows to {table}")

        write_to_postgres(dim_customer, "dim_customer")
        write_to_postgres(dim_service, "dim_service")
        write_to_postgres(dim_contract, "dim_contract")
        
        # Finally, write the real Fact table
        write_to_postgres(fact, "fact_customer_churn")
        
        print("="*70)
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    success = create_gold_star_schema()
    sys.exit(0 if success else 1)