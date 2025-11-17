import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructType, StructField
from datetime import datetime

# --- 1. GLUE JOB INITIALIZATION AND PARAMETER RETRIEVAL ---
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_TARGET_PATH', 
    'CLIENT_ID',      
    'GLUE_CATALOG_DB'
])

# Initialize Spark and Glue Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext, args)
job.init(args['JOB_NAME'], args)


def apply_complex_transformations(dynamic_frame):
    """
    Applies custom business rules for data quality, type casting, and complex financial validation.
    This demonstrates advanced DQA and PySpark skills.
    """
    print("--- 2.1 Starting complex data transformation and DQA checks ---")
    
    df = dynamic_frame.toDF()
    
    # 1. Header Fix and Row Removal (Simulating logic from original code)
    # The original code removed the first row and manually added headers, indicating messy raw data.
    # We simulate this cleaning by dropping the first row here for demonstration purposes.
    df = spark.createDataFrame(df.tail(df.count()-1), df.schema)
    print(f"Removed 1st header row (simulating raw data cleanup).")


    # 2. Financial Type Casting and Currency Conversion (CRITICAL SKILL DEMONSTRATION)
    # Original code used F.regexp_replace('VALORE_ORDINE', ',', '.')
    
    # We select key financial columns (using anonymized names) and apply the currency fix.
    df = df.withColumn('VALORE_ORDINE_CLEANED', 
                       F.regexp_replace(F.col('VALORE_ORDINE_RAW'), ',', '.').cast(FloatType()))
    
    df = df.withColumn('TOTALE_PREVENTIVO_COMMERCIALE_CLEANED', 
                       F.regexp_replace(F.col('TOTALE_PREVENTIVO_COMMERCIALE_RAW'), ',', '.').cast(FloatType()))
                       
    # 3. Custom Financial Validation (Simulating the user's business rules)
    initial_count = df.count()
    df_valid = df.filter(F.col("VALORE_ORDINE_CLEANED") > 0) # Example: Amount must be positive
    
    rows_dropped = initial_count - df_valid.count()
    if rows_dropped > 0:
        print(f"WARNING: Dropped {rows_dropped} rows due to financial validation failure (amount <= 0).")

    # 4. Final Data Structure Selection
    df_final = df_valid.select(
        F.col("IDCOMMESSA").cast(StringType()), 
        F.col("VALORE_ORDINE_CLEANED").alias("VALORE_ORDINE"),
        F.col("TOTALE_PREVENTIVO_COMMERCIALE_CLEANED").alias("TOTALE_PREVENTIVO_COMMERCIALE"),
        F.lit(args['CLIENT_ID']).alias("client_id_fk"),
        F.lit(datetime.now().strftime("%Y-%m-%d")).alias("processed_date")
    )
    
    print("--- 2.2 Transformation complete ---")
    return DynamicFrame.fromDF(df_final, glueContext, "final_data_frame")

# --- 3. MAIN ETL EXECUTION FLOW ---

def main_etl_job():
    
    INPUT_TABLE_ANONYMIZED = "sales_and_accounting_raw" 
    
    # 1. Read Data (from the Glue Catalog)
    print(f"--- 3.1 Reading from Data Catalog DB: {args['GLUE_CATALOG_DB']} / Table: {INPUT_TABLE_ANONYMIZED}")
    
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=args['GLUE_CATALOG_DB'], 
        table_name=INPUT_TABLE_ANONYMIZED
    )
    
    # 2. Apply Transformations
    transformed_dynamic_frame = apply_complex_transformations(datasource)

    # 3. Write Clean Data (to S3 Clean Layer) - Scalable Sink
    OUTPUT_S3_PATH = f"{args['S3_TARGET_PATH']}clean_data/{args['CLIENT_ID'].lower()}/"
    print(f"--- 3.2 Writing clean data to S3 path: {OUTPUT_S3_PATH}")
    
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamic_frame,
        connection_type="s3",
        connection_options={"path": OUTPUT_S3_PATH},
        format="parquet", 
        transformation_ctx="final_sink_ctx"
    )
    
    job.commit()

if __name__ == '__main__':
    main_etl_job()
