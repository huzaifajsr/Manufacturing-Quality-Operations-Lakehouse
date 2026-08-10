import os
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, round as _round, expr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    logger.info("Initializing Spark Session for Quality Transformation (Silver Layer)")
    spark = SparkSession.builder.appName("Manufacturing_Lakehouse_Quality_Silver").getOrCreate()
    
    config = load_config()
    bronze_path = config['storage']['bronze_layer_path']
    silver_path = config['storage']['silver_layer_path']
    max_delay = config['pipeline_settings']['max_late_event_hours']

    try:
        # Load Bronze Tables
        prod_df = spark.read.parquet(f"{bronze_path}/production_records")
        insp_df = spark.read.parquet(f"{bronze_path}/inspection_results")
        
        # Filter late events (> 24h delay in inspection)
        # Note: simplistic time logic for demo
        insp_df = insp_df.withColumn("delay_hours", 
                                     expr("(cast(inspection_timestamp as double) - cast(ingestion_timestamp as double))/3600"))
        
        # We simulate the 24 hour drop. If delay_hours is somehow > 24, we drop.
        # In reality, this compares inspection_timestamp against production end_time.
        valid_insp_df = insp_df.filter(col("delay_hours") <= max_delay).drop("delay_hours")
        
        # Join Production with Valid Inspections
        quality_df = valid_insp_df.join(prod_df, "batch_id", "inner")
        
        # Aggregate Quality Metrics
        agg_quality_df = quality_df.groupBy("production_line", "shift_id", "batch_id", "production_date") \
            .agg(
                _sum("defect_found").alias("defect_count"),
                count("inspection_id").alias("total_inspected")
            )
            
        # Compute Defect Rate
        final_quality_df = agg_quality_df.withColumn(
            "defect_rate", 
            _round((col("defect_count") / col("total_inspected")) * 100, 2)
        )
        
        # Handle Nulls
        final_quality_df = final_quality_df.fillna({"defect_rate": 0.0})
        
        # Write to Silver Layer
        out_path = f"{silver_path}/quality_metrics"
        final_quality_df.write.mode("overwrite").parquet(out_path)
        logger.info(f"Successfully processed Quality metrics to Silver layer: {out_path} ({final_quality_df.count()} records)")
        
    except Exception as e:
        logger.error(f"Error during quality transformation: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
