USE DATABASE MFG_LAKEHOUSE;
USE SCHEMA ANALYTICS;

-- KPI: Machine Downtime and Availability
CREATE OR REPLACE VIEW KPI_DOWNTIME AS
SELECT
    d.MACHINE_ID,
    d.PRODUCTION_LINE,
    d.SHIFT_ID,
    -- Fetch shift date from linked production table (simplified representation)
    -- In a real scenario, date would be included in the downtime fact table
    CURRENT_DATE() AS RECORD_DATE, 
    d.PLANNED_MAINT_MINS,
    d.UNPLANNED_BREAKDOWN_MINS,
    d.CHANGEOVER_MINS,
    d.IDLE_MINS,
    d.TOTAL_DOWNTIME_MINS,
    d.MACHINE_AVAILABILITY_PCT,
    -- 30-day cumulative downtime
    SUM(d.TOTAL_DOWNTIME_MINS) OVER (
        PARTITION BY d.MACHINE_ID 
        ORDER BY CURRENT_DATE() -- Replace with actual date column
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS CUMULATIVE_30D_DOWNTIME_MINS
FROM STAGING.FACT_DOWNTIME_METRICS d;
