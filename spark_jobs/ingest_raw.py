import os
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    logger.info("Initializing Spark Session for Ingestion (Bronze Layer)")
    spark = SparkSession.builder \
        .appName("Manufacturing_Lakehouse_Ingest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    config = load_config()
    raw_path = config['storage']['raw_layer_path']
    bronze_path = config['storage']['bronze_layer_path']

    datasets = ['machine_logs', 'production_records', 'inspection_results', 'material_batches']

    for dataset in datasets:
        logger.info(f"Ingesting raw data for: {dataset}")
        file_path = f"{raw_path}/{dataset}.csv"
        
        # Read CSV with schema inference
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            initial_count = df.count()
            logger.info(f"Read {initial_count} rows from {file_path}")
            
            # Deduplicate rows
            df_dedup = df.dropDuplicates()
            dedup_count = df_dedup.count()
            logger.info(f"Dropped {initial_count - dedup_count} duplicates")
            
            # Add metadata
            df_bronze = df_dedup.withColumn("ingestion_timestamp", current_timestamp())
            
            # Write to Bronze layer (Using Parquet here to avoid requiring delta-core jar in basic setups, 
            # but structurally representing a Bronze layer)
            out_path = f"{bronze_path}/{dataset}"
            df_bronze.write.mode("overwrite").parquet(out_path)
            logger.info(f"Successfully wrote {dataset} to Bronze layer at {out_path}")
            
        except Exception as e:
            logger.error(f"Failed to ingest {dataset}: {str(e)}")
            raise e

    spark.stop()

if __name__ == "__main__":
    main()
