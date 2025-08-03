"""
Bronze to Silver ETL Job
Processes raw data from Bronze layer, applies data quality rules,
and stores cleaned data in Silver layer.
"""

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_spark_context():
    """Initialize Spark and Glue contexts"""
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Optimize Spark configuration for data lake workloads
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    return sc, glueContext, spark

def read_bronze_data(glueContext, database_name, table_name):
    """Read data from Bronze layer using Glue catalog"""
    try:
        logger.info(f"Reading data from {database_name}.{table_name}")
        
        # Create dynamic frame from catalog
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="bronze_source"
        )
        
        logger.info(f"Successfully read {dynamic_frame.count()} records from Bronze layer")
        return dynamic_frame
        
    except Exception as e:
        logger.error(f"Error reading Bronze data: {str(e)}")
        raise

def apply_data_quality_rules(df):
    """Apply data quality and cleansing rules"""
    logger.info("Applying data quality rules")
    
    # Remove duplicates
    initial_count = df.count()
    df_deduped = df.dropDuplicates()
    duplicate_count = initial_count - df_deduped.count()
    logger.info(f"Removed {duplicate_count} duplicate records")
    
    # Remove null primary keys (assuming 'id' is primary key)
    if 'id' in df_deduped.columns:
        df_clean = df_deduped.filter(col('id').isNotNull())
        null_id_count = df_deduped.count() - df_clean.count()
        logger.info(f"Removed {null_id_count} records with null IDs")
    else:
        df_clean = df_deduped
    
    # Standardize column names (lowercase, replace spaces with underscores)
    for old_col in df_clean.columns:
        new_col = old_col.lower().replace(' ', '_').replace('-', '_')
        if old_col != new_col:
            df_clean = df_clean.withColumnRenamed(old_col, new_col)
    
    # Add metadata columns
    df_with_metadata = df_clean \
        .withColumn('processed_timestamp', current_timestamp()) \
        .withColumn('data_source', lit('bronze_layer')) \
        .withColumn('processing_date', current_date())
    
    # Data type conversions and validations
    df_typed = apply_schema_corrections(df_with_metadata)
    
    logger.info(f"Data quality processing complete. Final record count: {df_typed.count()}")
    return df_typed

def apply_schema_corrections(df):
    """Apply schema corrections and data type conversions"""
    logger.info("Applying schema corrections")
    
    # Example corrections - adjust based on your data schema
    corrected_df = df
    
    # Convert string dates to timestamp
    date_columns = ['created_date', 'updated_date', 'transaction_date']
    for col_name in date_columns:
        if col_name in df.columns:
            corrected_df = corrected_df.withColumn(
                col_name, 
                to_timestamp(col(col_name), 'yyyy-MM-dd HH:mm:ss')
            )
    
    # Ensure numeric columns are properly typed
    numeric_columns = ['amount', 'quantity', 'price', 'total']
    for col_name in numeric_columns:
        if col_name in df.columns:
            corrected_df = corrected_df.withColumn(
                col_name,
                when(col(col_name).cast('double').isNull(), 0.0)
                .otherwise(col(col_name).cast('double'))
            )
    
    # Clean and validate email addresses
    if 'email' in df.columns:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        corrected_df = corrected_df.withColumn(
            'email_valid',
            when(col('email').rlike(email_pattern), True).otherwise(False)
        )
    
    return corrected_df

def partition_data_for_silver(df):
    """Add partitioning columns for Silver layer storage"""
    logger.info("Adding partition columns")
    
    # Add year, month, day partitions based on processing date
    partitioned_df = df \
        .withColumn('year', year(col('processing_date'))) \
        .withColumn('month', month(col('processing_date'))) \
        .withColumn('day', dayofmonth(col('processing_date')))
    
    return partitioned_df

def write_to_silver(glueContext, df, silver_bucket, table_name):
    """Write processed data to Silver layer"""
    logger.info(f"Writing data to Silver layer: {silver_bucket}")
    
    # Convert DataFrame back to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_output")
    
    # Write to S3 in Parquet format with partitioning
    silver_path = f"s3://{silver_bucket}/{table_name}/"
    
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": silver_path,
                "partitionKeys": ["year", "month", "day"]
            },
            format="glueparquet",
            format_options={
                "compression": "snappy",
                "blockSize": 134217728,  # 128MB
                "pageSize": 1048576      # 1MB
            },
            transformation_ctx="silver_sink"
        )
        
        logger.info(f"Successfully wrote data to Silver layer at {silver_path}")
        
    except Exception as e:
        logger.error(f"Error writing to Silver layer: {str(e)}")
        raise

def log_data_lineage(job_name, source_table, target_table, record_count, processing_time):
    """Log data lineage information"""
    logger.info("Logging data lineage information")
    
    lineage_info = {
        'job_name': job_name,
        'source_table': source_table,
        'target_table': target_table,
        'record_count': record_count,
        'processing_time': processing_time,
        'timestamp': str(datetime.now())
    }
    
    # In a production environment, you might want to store this in a database
    # or send to a lineage tracking service
    logger.info(f"Lineage: {lineage_info}")

def main():
    """Main ETL process"""
    start_time = datetime.now()
    
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'bronze-bucket',
        'silver-bucket'
    ])
    
    job_name = args['JOB_NAME']
    bronze_bucket = args['bronze_bucket']
    silver_bucket = args['silver_bucket']
    
    logger.info(f"Starting job: {job_name}")
    logger.info(f"Bronze bucket: {bronze_bucket}")
    logger.info(f"Silver bucket: {silver_bucket}")
    
    # Initialize contexts
    sc, glueContext, spark = setup_spark_context()
    job = Job(glueContext)
    job.init(job_name, args)
    
    try:
        # Get database name from Glue catalog
        database_name = "medallion_data_platform_catalog"  # Adjust based on your setup
        
        # Get list of tables in Bronze layer
        glue_client = boto3.client('glue')
        tables_response = glue_client.get_tables(DatabaseName=database_name)
        
        bronze_tables = [
            table['Name'] for table in tables_response['TableList']
            if table['StorageDescriptor']['Location'].startswith(f's3://{bronze_bucket}')
        ]
        
        logger.info(f"Found {len(bronze_tables)} tables in Bronze layer")
        
        # Process each table
        for table_name in bronze_tables:
            logger.info(f"Processing table: {table_name}")
            
            # Read data from Bronze
            bronze_dynamic_frame = read_bronze_data(glueContext, database_name, table_name)
            
            # Convert to DataFrame for processing
            bronze_df = bronze_dynamic_frame.toDF()
            
            # Apply data quality rules
            clean_df = apply_data_quality_rules(bronze_df)
            
            # Add partitioning
            partitioned_df = partition_data_for_silver(clean_df)
            
            # Write to Silver layer
            silver_table_name = f"silver_{table_name.replace('bronze_', '')}"
            write_to_silver(glueContext, partitioned_df, silver_bucket, silver_table_name)
            
            # Log lineage information
            processing_time = (datetime.now() - start_time).total_seconds()
            log_data_lineage(
                job_name, 
                table_name, 
                silver_table_name, 
                partitioned_df.count(),
                processing_time
            )
        
        logger.info("Bronze to Silver ETL completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    
    finally:
        job.commit()
        sc.stop()

if __name__ == "__main__":
    from datetime import datetime
    main()