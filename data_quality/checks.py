import logging
import pandas as pd

logger = logging.getLogger(__name__)

def check_duplicates(df, key_columns):
    """
    Checks for duplicate rows based on key columns.
    Returns True if no duplicates found, else False.
    """
    duplicates = df[df.duplicated(subset=key_columns, keep=False)]
    if not duplicates.empty:
        logger.warning(f"Found {len(duplicates)} duplicate rows based on keys: {key_columns}")
        return False
    logger.info(f"Duplicate check passed for keys: {key_columns}")
    return True

def check_nulls(df, critical_columns, threshold_pct):
    """
    Checks if null percentage in critical columns exceeds threshold.
    """
    passed = True
    total_rows = len(df)
    
    for col in critical_columns:
        null_count = df[col].isnull().sum()
        null_pct = null_count / total_rows
        if null_pct > threshold_pct:
            logger.error(f"Column {col} failed null check. Null pct: {null_pct:.2%}, Threshold: {threshold_pct:.2%}")
            passed = False
        else:
            logger.info(f"Column {col} passed null check. Null pct: {null_pct:.2%}")
            
    return passed

def check_late_events(df, event_timestamp_col, reference_timestamp_col, max_delay_hours=24):
    """
    Checks if events arrive later than the maximum allowed delay.
    """
    df['delay_hours'] = (pd.to_datetime(df[event_timestamp_col]) - pd.to_datetime(df[reference_timestamp_col])).dt.total_seconds() / 3600.0
    late_events = df[df['delay_hours'] > max_delay_hours]
    
    if not late_events.empty:
        logger.warning(f"Found {len(late_events)} late events exceeding {max_delay_hours} hours delay.")
        return False
    
    logger.info(f"Late event check passed.")
    return True

def check_row_count_deviation(current_count, historical_avg, tolerance=0.2):
    """
    Checks if the current row count deviates significantly from the historical average.
    """
    if historical_avg == 0:
        logger.warning("Historical average is 0, skipping deviation check.")
        return True
        
    deviation = abs(current_count - historical_avg) / historical_avg
    if deviation > tolerance:
        logger.error(f"Row count deviation {deviation:.2%} exceeds tolerance {tolerance:.2%}")
        return False
        
    logger.info(f"Row count deviation {deviation:.2%} is within tolerance.")
    return True
