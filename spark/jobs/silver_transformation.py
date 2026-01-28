"""
Silver Layer Transformation using PySpark
Author: Prashant Pathak
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, sha2, trim, lower,
    round as spark_round, sum as spark_sum
)
import sys
from functools import reduce

def create_spark_session():
    spark = SparkSession.builder \
        .appName("SilverLayerTransformation") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_jdbc_props():
    return {
        "url": "jdbc:postgresql://postgres-staging:5432/telecom_staging",
        "driver": "org.postgresql.Driver",
        "user": "staging_user",   # can be kept in key vault or encrypted form
        "password": "staging_pass" # can be kept in key vault or encrypted form
    }

def transform_bronze_to_silver():
    print("="*70)
    print("SILVER LAYER - PySpark Transformation")
    print("="*70)
    
    spark = create_spark_session()
    
    try:
        props = get_jdbc_props()

        bronze_df = spark.read.format("jdbc") \
            .option("url", props["url"]) \
            .option("dbtable", "bronze.raw_telecom_churn") \
            .option("user", props["user"]) \
            .option("password", props["password"]) \
            .option("driver", props["driver"]) \
            .load()
        
        print(f"Read {bronze_df.count():,} rows from Bronze")
        
        # 1. Standardize Column Names (Handle Bronze -> Silver mapping)
        silver_df = bronze_df \
            .withColumnRenamed("monthlycharges", "monthly_charges") \
            .withColumnRenamed("totalcharges", "total_charges") \
            .withColumnRenamed("internetservice", "internet_service") \
            .withColumnRenamed("techsupport", "tech_support") \
            .withColumnRenamed("contract", "contract_type") \
            .withColumnRenamed("contracttype", "contract_type") # Handle variation
        
        # 2. Handle missing 'age' column if it doesn't exist
        if "age" not in silver_df.columns:
            # Check if SeniorCitizen exists, otherwise default to -1 or null
            if "seniorcitizen" in silver_df.columns:
                silver_df = silver_df.withColumnRenamed("seniorcitizen", "age") 
            else:
                silver_df = silver_df.withColumn("age", lit(None).cast("integer"))

        # 3. PII Anonymization
        silver_df = silver_df.withColumn(
                    "customer_id_anonymized",
                    sha2(col("customerid").cast("string"), 256).substr(1, 16)
                )
        print("Anonymized CustomerID")
        
        # 4. Transformations
        # Clean String Columns
        # Ensure these columns exist before trying to clean them
        clean_cols = ["gender", "contract_type", "internet_service", "tech_support", "churn"]
        for c in clean_cols:
            if c in silver_df.columns:
                silver_df = silver_df.withColumn(c, trim(lower(col(c))))

        # Calculate avg_monthly_spend
        silver_df = silver_df.withColumn(
            "avg_monthly_spend",
            when(col("tenure").isNotNull() & (col("tenure") > 0),
                spark_round(col("total_charges") / col("tenure"), 2))
            .otherwise(lit(0.0))
        )

        # 5. Data Quality Score
        # Dynamically calculate score based on valid columns
        score_columns = silver_df.columns
        silver_df = silver_df.withColumn(
            "data_quality_score",
            spark_round(
                reduce(lambda a, b: a + b,
                    [when(col(c).isNotNull(), 1).otherwise(0) for c in score_columns]
                ) / lit(len(score_columns)), 2
            )
        )
        
        # 6. Metadata
        silver_df = silver_df.withColumn("transformation_timestamp", current_timestamp())
        silver_df = silver_df.withColumn("source_pipeline", lit("pyspark_silver"))
        
        # 7. Final Selection
        # Ensure all these columns exist in the DF before selecting
        final_cols_target = [
            "customer_id_anonymized", "age", "gender", "tenure",
            "monthly_charges", "contract_type", "internet_service",
            "total_charges", "tech_support", "churn", "avg_monthly_spend",
            "transformation_timestamp", "source_pipeline", "data_quality_score"
        ]
        
        # Filter explicitly to avoid AnalysisException if a column is missing
        available_cols = [c for c in final_cols_target if c in silver_df.columns]
        silver_df = silver_df.select(available_cols)
        
        # Write to Silver
        silver_df.write.format("jdbc") \
            .option("url", props["url"]) \
            .option("dbtable", "silver.cleansed_telecom_churn") \
            .option("user", props["user"]) \
            .option("password", props["password"]) \
            .option("driver", props["driver"]) \
            .mode("overwrite") \
            .save()
        
        print(f"✓ Wrote {silver_df.count():,} rows to Silver")
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
    success = transform_bronze_to_silver()
    sys.exit(0 if success else 1)