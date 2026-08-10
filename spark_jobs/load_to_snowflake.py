import os
import yaml
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def load_table_to_snowflake(conn, df_pandas, table_name):
    # Ensure uppercase table names for Snowflake defaults
    table_name = table_name.upper()
    logger.info(f"Loading {len(df_pandas)} rows to Snowflake table {table_name}...")
    success, nchunks, nrows, _ = write_pandas(conn, df_pandas, table_name, auto_create_table=True, overwrite=True)
    if success:
        logger.info(f"Successfully inserted {nrows} rows into {table_name}")
    else:
        logger.error(f"Failed to insert into {table_name}")

def main():
    logger.info("Starting Snowflake Load Job")
    spark = SparkSession.builder.appName("Manufacturing_Lakehouse_Snowflake_Load").getOrCreate()
    config = load_config()
    silver_path = config['storage']['silver_layer_path']
    sf_config = config['snowflake']

    try:
        # Note: In a real production system, use Snowflake Spark Connector or Stage+COPY INTO.
        # We are using python snowflake-connector-python pandas_tools for simulation.
        
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema'],
            role=sf_config['role']
        )
        
        # Load Quality Metrics
        quality_df = spark.read.parquet(f"{silver_path}/quality_metrics").toPandas()
        load_table_to_snowflake(conn, quality_df, "FACT_QUALITY_METRICS")
        
        # Load Downtime Metrics
        downtime_df = spark.read.parquet(f"{silver_path}/downtime_metrics").toPandas()
        load_table_to_snowflake(conn, downtime_df, "FACT_DOWNTIME_METRICS")

    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {str(e)}")
        raise e
    finally:
        if 'conn' in locals():
            conn.close()
        spark.stop()

if __name__ == "__main__":
    main()
