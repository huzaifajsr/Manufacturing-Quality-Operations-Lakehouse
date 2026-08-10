import os
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, lit, round as _round

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    logger.info("Initializing Spark Session for Downtime Transformation (Silver Layer)")
    spark = SparkSession.builder.appName("Manufacturing_Lakehouse_Downtime_Silver").getOrCreate()
    
    config = load_config()
    bronze_path = config['storage']['bronze_layer_path']
    silver_path = config['storage']['silver_layer_path']
    
    # Assume 8-hour shifts = 480 minutes
    SHIFT_DURATION_MINS = 480.0

    try:
        # Load Bronze Machine Logs
        machine_df = spark.read.parquet(f"{bronze_path}/machine_logs")
        
        # Categorize downtime and aggregate
        # Assuming event_type defines downtime vs running
        downtime_df = machine_df.filter(col("event_type").isin(["planned_maintenance", "unplanned_breakdown", "changeover", "idle"]))
        
        agg_downtime_df = downtime_df.groupBy("machine_id", "production_line", "shift_id") \
            .agg(
                _sum(when(col("event_type") == "planned_maintenance", col("duration_minutes")).otherwise(0)).alias("planned_maint_mins"),
                _sum(when(col("event_type") == "unplanned_breakdown", col("duration_minutes")).otherwise(0)).alias("unplanned_breakdown_mins"),
                _sum(when(col("event_type") == "changeover", col("duration_minutes")).otherwise(0)).alias("changeover_mins"),
                _sum(when(col("event_type") == "idle", col("duration_minutes")).otherwise(0)).alias("idle_mins"),
                _sum("duration_minutes").alias("total_downtime_mins")
            )
            
        # Compute Machine Availability %
        final_downtime_df = agg_downtime_df.withColumn(
            "machine_availability_pct",
            _round(((lit(SHIFT_DURATION_MINS) - col("total_downtime_mins")) / lit(SHIFT_DURATION_MINS)) * 100, 2)
        )
        
        # Write to Silver Layer
        out_path = f"{silver_path}/downtime_metrics"
        final_downtime_df.write.mode("overwrite").parquet(out_path)
        logger.info(f"Successfully processed Downtime metrics to Silver layer: {out_path} ({final_downtime_df.count()} records)")
        
    except Exception as e:
        logger.error(f"Error during downtime transformation: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
