"""
Silver to Gold ETL Job
Aggregates and transforms data from Silver layer to create business-ready
analytical datasets in the Gold layer.
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
from pyspark.sql.window import Window
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_spark_context():
    """Initialize Spark and Glue contexts with optimizations for analytical workloads"""
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Optimize Spark for analytical workloads
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    
    return sc, glueContext, spark

def read_silver_data(glueContext, database_name, table_name):
    """Read data from Silver layer"""
    try:
        logger.info(f"Reading data from Silver layer: {database_name}.{table_name}")
        
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx=f"silver_source_{table_name}"
        )
        
        record_count = dynamic_frame.count()
        logger.info(f"Read {record_count} records from {table_name}")
        
        return dynamic_frame
        
    except Exception as e:
        logger.error(f"Error reading Silver data from {table_name}: {str(e)}")
        raise

def create_customer_analytics(customer_df, transaction_df):
    """Create customer analytics aggregations"""
    logger.info("Creating customer analytics")
    
    # Customer transaction summary
    customer_summary = transaction_df.groupBy("customer_id") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_transaction_amount"),
            max("transaction_date").alias("last_transaction_date"),
            min("transaction_date").alias("first_transaction_date")
        )
    
    # Add customer segmentation based on transaction behavior
    customer_segments = customer_summary \
        .withColumn("customer_segment",
            when(col("total_amount") >= 10000, "Premium")
            .when(col("total_amount") >= 5000, "Gold")
            .when(col("total_amount") >= 1000, "Silver")
            .otherwise("Bronze")
        ) \
        .withColumn("transaction_frequency",
            when(col("total_transactions") >= 50, "High")
            .when(col("total_transactions") >= 20, "Medium")
            .otherwise("Low")
        )
    
    # Join with customer data for complete profile
    if customer_df is not None:
        customer_analytics = customer_df.join(
            customer_segments, 
            "customer_id", 
            "left"
        ).fillna(0, ["total_transactions", "total_amount"])
    else:
        customer_analytics = customer_segments
    
    # Add derived metrics
    customer_analytics = customer_analytics \
        .withColumn("days_since_last_transaction",
            datediff(current_date(), col("last_transaction_date"))
        ) \
        .withColumn("customer_lifetime_value",
            col("total_amount") * 
            when(col("customer_segment") == "Premium", 1.5)
            .when(col("customer_segment") == "Gold", 1.3)
            .when(col("customer_segment") == "Silver", 1.1)
            .otherwise(1.0)
        ) \
        .withColumn("churn_risk",
            when(col("days_since_last_transaction") > 90, "High")
            .when(col("days_since_last_transaction") > 30, "Medium")
            .otherwise("Low")
        )
    
    logger.info(f"Created customer analytics with {customer_analytics.count()} records")
    return customer_analytics

def create_product_analytics(product_df, transaction_df):
    """Create product performance analytics"""
    logger.info("Creating product analytics")
    
    # Product sales summary
    product_summary = transaction_df.groupBy("product_id") \
        .agg(
            count("transaction_id").alias("total_sales"),
            sum("quantity").alias("total_quantity_sold"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_sale_amount"),
            countDistinct("customer_id").alias("unique_customers")
        )
    
    # Calculate product rankings
    window_spec = Window.orderBy(desc("total_revenue"))
    product_rankings = product_summary \
        .withColumn("revenue_rank", row_number().over(window_spec)) \
        .withColumn("revenue_percentile", 
            percent_rank().over(window_spec) * 100
        )
    
    # Add product performance categories
    product_performance = product_rankings \
        .withColumn("performance_category",
            when(col("revenue_percentile") >= 80, "Top Performer")
            .when(col("revenue_percentile") >= 60, "Good Performer")
            .when(col("revenue_percentile") >= 40, "Average Performer")
            .when(col("revenue_percentile") >= 20, "Below Average")
            .otherwise("Poor Performer")
        )
    
    # Join with product master data if available
    if product_df is not None:
        product_analytics = product_df.join(
            product_performance,
            "product_id",
            "left"
        ).fillna(0, ["total_sales", "total_revenue"])
    else:
        product_analytics = product_performance
    
    logger.info(f"Created product analytics with {product_analytics.count()} records")
    return product_analytics

def create_time_series_analytics(transaction_df):
    """Create time-based analytics and trends"""
    logger.info("Creating time series analytics")
    
    # Daily aggregations
    daily_metrics = transaction_df \
        .withColumn("transaction_date", to_date(col("transaction_date"))) \
        .groupBy("transaction_date") \
        .agg(
            count("transaction_id").alias("daily_transaction_count"),
            sum("amount").alias("daily_revenue"),
            avg("amount").alias("daily_avg_transaction"),
            countDistinct("customer_id").alias("daily_unique_customers"),
            countDistinct("product_id").alias("daily_unique_products")
        )
    
    # Add rolling averages and trends
    window_7d = Window.orderBy("transaction_date").rowsBetween(-6, 0)
    window_30d = Window.orderBy("transaction_date").rowsBetween(-29, 0)
    
    daily_with_trends = daily_metrics \
        .withColumn("revenue_7d_avg", avg("daily_revenue").over(window_7d)) \
        .withColumn("revenue_30d_avg", avg("daily_revenue").over(window_30d)) \
        .withColumn("transaction_count_7d_avg", avg("daily_transaction_count").over(window_7d)) \
        .withColumn("transaction_count_30d_avg", avg("daily_transaction_count").over(window_30d))
    
    # Monthly aggregations
    monthly_metrics = transaction_df \
        .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM")) \
        .groupBy("year_month") \
        .agg(
            count("transaction_id").alias("monthly_transaction_count"),
            sum("amount").alias("monthly_revenue"),
            avg("amount").alias("monthly_avg_transaction"),
            countDistinct("customer_id").alias("monthly_unique_customers"),
            countDistinct("product_id").alias("monthly_unique_products")
        ) \
        .withColumn("year", substring(col("year_month"), 1, 4).cast("int")) \
        .withColumn("month", substring(col("year_month"), 6, 2).cast("int"))
    
    # Calculate month-over-month growth
    window_monthly = Window.orderBy("year_month")
    monthly_with_growth = monthly_metrics \
        .withColumn("prev_month_revenue", lag("monthly_revenue").over(window_monthly)) \
        .withColumn("revenue_mom_growth",
            when(col("prev_month_revenue").isNotNull(),
                ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100)
            ).otherwise(0)
        )
    
    logger.info(f"Created daily analytics with {daily_with_trends.count()} records")
    logger.info(f"Created monthly analytics with {monthly_with_growth.count()} records")
    
    return daily_with_trends, monthly_with_growth

def create_cohort_analysis(transaction_df):
    """Create customer cohort analysis"""
    logger.info("Creating cohort analysis")
    
    # Identify first purchase date for each customer
    customer_first_purchase = transaction_df \
        .groupBy("customer_id") \
        .agg(min("transaction_date").alias("first_purchase_date"))
    
    # Add cohort month to transactions
    transactions_with_cohort = transaction_df \
        .join(customer_first_purchase, "customer_id") \
        .withColumn("cohort_month", date_format(col("first_purchase_date"), "yyyy-MM")) \
        .withColumn("transaction_month", date_format(col("transaction_date"), "yyyy-MM"))
    
    # Calculate months since first purchase
    transactions_with_periods = transactions_with_cohort \
        .withColumn("months_since_first_purchase",
            months_between(col("transaction_date"), col("first_purchase_date")).cast("int")
        )
    
    # Create cohort table
    cohort_table = transactions_with_periods \
        .groupBy("cohort_month", "months_since_first_purchase") \
        .agg(
            countDistinct("customer_id").alias("customers"),
            sum("amount").alias("revenue"),
            count("transaction_id").alias("transactions")
        )
    
    # Calculate cohort sizes
    cohort_sizes = customer_first_purchase \
        .withColumn("cohort_month", date_format(col("first_purchase_date"), "yyyy-MM")) \
        .groupBy("cohort_month") \
        .agg(countDistinct("customer_id").alias("cohort_size"))
    
    # Add retention rates
    cohort_analysis = cohort_table \
        .join(cohort_sizes, "cohort_month") \
        .withColumn("retention_rate", 
            (col("customers") / col("cohort_size") * 100).cast("decimal(5,2)")
        )
    
    logger.info(f"Created cohort analysis with {cohort_analysis.count()} records")
    return cohort_analysis

def create_advanced_metrics(transaction_df):
    """Create advanced business metrics"""
    logger.info("Creating advanced metrics")
    
    # RFM Analysis (Recency, Frequency, Monetary)
    reference_date = transaction_df.agg(max("transaction_date")).collect()[0][0]
    
    rfm_analysis = transaction_df \
        .groupBy("customer_id") \
        .agg(
            datediff(lit(reference_date), max("transaction_date")).alias("recency"),
            count("transaction_id").alias("frequency"),
            sum("amount").alias("monetary")
        )
    
    # Calculate RFM quintiles
    recency_quintiles = rfm_analysis.approxQuantile("recency", [0.2, 0.4, 0.6, 0.8], 0.05)
    frequency_quintiles = rfm_analysis.approxQuantile("frequency", [0.2, 0.4, 0.6, 0.8], 0.05)
    monetary_quintiles = rfm_analysis.approxQuantile("monetary", [0.2, 0.4, 0.6, 0.8], 0.05)
    
    def assign_rfm_score(col_name, quintiles, reverse=False):
        """Assign RFM scores based on quintiles"""
        conditions = []
        scores = [5, 4, 3, 2, 1] if not reverse else [1, 2, 3, 4, 5]
        
        for i, score in enumerate(scores):
            if i == 0:
                if reverse:
                    conditions.append(when(col(col_name) <= quintiles[0], score))
                else:
                    conditions.append(when(col(col_name) >= quintiles[-1], score))
            elif i == len(scores) - 1:
                if reverse:
                    conditions.append(score)  # Default case
                else:
                    conditions.append(score)  # Default case
            else:
                if reverse:
                    conditions.append(when(col(col_name) <= quintiles[i-1], score))
                else:
                    conditions.append(when(col(col_name) >= quintiles[-(i+1)], score))
        
        # Build the complete when-otherwise chain
        result = conditions[0]
        for condition in conditions[1:-1]:
            result = result.otherwise(condition)
        return result.otherwise(conditions[-1])
    
    rfm_scores = rfm_analysis \
        .withColumn("r_score", assign_rfm_score("recency", recency_quintiles, reverse=True)) \
        .withColumn("f_score", assign_rfm_score("frequency", frequency_quintiles)) \
        .withColumn("m_score", assign_rfm_score("monetary", monetary_quintiles)) \
        .withColumn("rfm_score", concat(col("r_score"), col("f_score"), col("m_score")))
    
    # Customer segmentation based on RFM
    customer_segments = rfm_scores \
        .withColumn("customer_segment",
            when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "Champions")
            .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "Loyal Customers")
            .when((col("r_score") >= 4) & (col("f_score") <= 2), "New Customers")
            .when((col("r_score") <= 2) & (col("f_score") >= 3) & (col("m_score") >= 3), "At Risk")
            .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") >= 3), "Cannot Lose Them")
            .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") <= 2), "Lost Customers")
            .otherwise("Others")
        )
    
    logger.info(f"Created RFM analysis with {customer_segments.count()} records")
    return customer_segments

def write_to_gold(glueContext, df, gold_bucket, dataset_name):
    """Write analytical dataset to Gold layer"""
    logger.info(f"Writing {dataset_name} to Gold layer")
    
    # Add metadata
    df_with_metadata = df \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("dataset_name", lit(dataset_name)) \
        .withColumn("year", year(current_date())) \
        .withColumn("month", month(current_date())) \
        .withColumn("day", dayofmonth(current_date()))
    
    # Convert to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df_with_metadata, glueContext, f"gold_{dataset_name}")
    
    # Write to S3 with partitioning
    gold_path = f"s3://{gold_bucket}/{dataset_name}/"
    
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": gold_path,
                "partitionKeys": ["year", "month", "day"]
            },
            format="glueparquet",
            format_options={
                "compression": "snappy",
                "blockSize": 134217728,  # 128MB blocks for analytical queries
                "pageSize": 1048576      # 1MB pages
            },
            transformation_ctx=f"gold_sink_{dataset_name}"
        )
        
        logger.info(f"Successfully wrote {dataset_name} to {gold_path}")
        
    except Exception as e:
        logger.error(f"Error writing {dataset_name} to Gold layer: {str(e)}")
        raise

def main():
    """Main ETL process for Silver to Gold transformation"""
    start_time = datetime.now()
    
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'silver-bucket',
        'gold-bucket'
    ])
    
    job_name = args['JOB_NAME']
    silver_bucket = args['silver_bucket']
    gold_bucket = args['gold_bucket']
    
    logger.info(f"Starting job: {job_name}")
    logger.info(f"Silver bucket: {silver_bucket}")
    logger.info(f"Gold bucket: {gold_bucket}")
    
    # Initialize contexts
    sc, glueContext, spark = setup_spark_context()
    job = Job(glueContext)
    job.init(job_name, args)
    
    try:
        database_name = "medallion_data_platform_catalog"
        
        # Get Silver layer tables
        glue_client = boto3.client('glue')
        tables_response = glue_client.get_tables(DatabaseName=database_name)
        
        silver_tables = [
            table['Name'] for table in tables_response['TableList']
            if table['StorageDescriptor']['Location'].startswith(f's3://{silver_bucket}')
        ]
        
        logger.info(f"Found {len(silver_tables)} tables in Silver layer")
        
        # Load core datasets
        customer_df = None
        transaction_df = None
        product_df = None
        
        for table_name in silver_tables:
            silver_data = read_silver_data(glueContext, database_name, table_name).toDF()
            
            if 'customer' in table_name.lower():
                customer_df = silver_data
            elif 'transaction' in table_name.lower():
                transaction_df = silver_data
            elif 'product' in table_name.lower():
                product_df = silver_data
        
        # Ensure we have transaction data (minimum requirement)
        if transaction_df is None:
            raise ValueError("No transaction data found in Silver layer")
        
        # Create analytical datasets
        
        # 1. Customer Analytics
        if customer_df is not None or transaction_df is not None:
            customer_analytics = create_customer_analytics(customer_df, transaction_df)
            write_to_gold(glueContext, customer_analytics, gold_bucket, "customer_analytics")
        
        # 2. Product Analytics
        if transaction_df is not None:
            product_analytics = create_product_analytics(product_df, transaction_df)
            write_to_gold(glueContext, product_analytics, gold_bucket, "product_analytics")
        
        # 3. Time Series Analytics
        daily_metrics, monthly_metrics = create_time_series_analytics(transaction_df)
        write_to_gold(glueContext, daily_metrics, gold_bucket, "daily_metrics")
        write_to_gold(glueContext, monthly_metrics, gold_bucket, "monthly_metrics")
        
        # 4. Cohort Analysis
        cohort_analysis = create_cohort_analysis(transaction_df)
        write_to_gold(glueContext, cohort_analysis, gold_bucket, "cohort_analysis")
        
        # 5. Advanced Metrics (RFM Analysis)
        rfm_analysis = create_advanced_metrics(transaction_df)
        write_to_gold(glueContext, rfm_analysis, gold_bucket, "rfm_customer_segments")
        
        # Create summary report
        processing_time = (datetime.now() - start_time).total_seconds()
        
        summary_data = [
            ("customer_analytics", customer_analytics.count() if 'customer_analytics' in locals() else 0),
            ("product_analytics", product_analytics.count() if 'product_analytics' in locals() else 0),
            ("daily_metrics", daily_metrics.count()),
            ("monthly_metrics", monthly_metrics.count()),
            ("cohort_analysis", cohort_analysis.count()),
            ("rfm_customer_segments", rfm_analysis.count())
        ]
        
        summary_df = spark.createDataFrame(summary_data, ["dataset_name", "record_count"]) \
            .withColumn("job_name", lit(job_name)) \
            .withColumn("processing_time_seconds", lit(processing_time)) \
            .withColumn("created_timestamp", current_timestamp())
        
        write_to_gold(glueContext, summary_df, gold_bucket, "job_summary")
        
        logger.info("Silver to Gold ETL completed successfully")
        logger.info(f"Total processing time: {processing_time} seconds")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    
    finally:
        job.commit()
        sc.stop()

if __name__ == "__main__":
    from datetime import datetime
    main()