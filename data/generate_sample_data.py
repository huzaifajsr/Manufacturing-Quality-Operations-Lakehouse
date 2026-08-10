import os
import random
import uuid
import pandas as pd
from datetime import datetime, timedelta

def generate_sample_data():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    os.makedirs(base_dir, exist_ok=True)
    
    # Configuration
    NUM_MACHINE_LOGS = 5000
    NUM_PROD_RECORDS = 3000
    NUM_INSP_RESULTS = 3000
    NUM_MAT_BATCHES = 500
    
    lines = ['LINE_A', 'LINE_B', 'LINE_C', 'LINE_D']
    shifts = ['S1', 'S2', 'S3']
    event_types = ['running', 'planned_maintenance', 'unplanned_breakdown', 'changeover', 'idle']
    product_ids = [f'PROD_{i:03d}' for i in range(1, 11)]
    defect_types = ['scratch', 'dent', 'misaligned', 'color_mismatch', 'None']
    
    start_date = datetime.now() - timedelta(days=30)
    
    # 1. Machine Logs
    machine_logs = []
    for _ in range(NUM_MACHINE_LOGS):
        machine_logs.append({
            'machine_id': f"MACH_{random.randint(1, 20):03d}",
            'timestamp': (start_date + timedelta(minutes=random.randint(0, 43200))).strftime("%Y-%m-%d %H:%M:%S"),
            'event_type': random.choices(event_types, weights=[70, 10, 5, 10, 5])[0],
            'duration_minutes': random.randint(5, 120),
            'production_line': random.choice(lines),
            'shift_id': random.choice(shifts)
        })
    pd.DataFrame(machine_logs).to_csv(os.path.join(base_dir, 'machine_logs.csv'), index=False)
    
    # 2. Production Records
    prod_records = []
    batch_ids = [str(uuid.uuid4()) for _ in range(NUM_PROD_RECORDS)]
    for batch_id in batch_ids:
        dt = start_date + timedelta(minutes=random.randint(0, 43200))
        prod_records.append({
            'batch_id': batch_id,
            'product_id': random.choice(product_ids),
            'production_line': random.choice(lines),
            'shift_id': random.choice(shifts),
            'units_produced': random.randint(500, 5000),
            'production_date': dt.strftime("%Y-%m-%d"),
            'start_time': dt.strftime("%Y-%m-%d %H:%M:%S"),
            'end_time': (dt + timedelta(hours=random.randint(4, 8))).strftime("%Y-%m-%d %H:%M:%S")
        })
    pd.DataFrame(prod_records).to_csv(os.path.join(base_dir, 'production_records.csv'), index=False)
    
    # 3. Inspection Results
    insp_results = []
    for _ in range(NUM_INSP_RESULTS):
        batch = random.choice(batch_ids)
        is_defect = random.choices([0, 1], weights=[95, 5])[0]
        insp_results.append({
            'inspection_id': str(uuid.uuid4()),
            'batch_id': batch,
            'defect_found': is_defect,
            'defect_type': random.choice(defect_types[:-1]) if is_defect else 'None',
            'measurement_value': round(random.uniform(9.5, 10.5), 4),
            'inspector_id': f"INSP_{random.randint(1, 50):02d}",
            'inspection_timestamp': (start_date + timedelta(minutes=random.randint(0, 44000))).strftime("%Y-%m-%d %H:%M:%S")
        })
    pd.DataFrame(insp_results).to_csv(os.path.join(base_dir, 'inspection_results.csv'), index=False)
    
    # 4. Material Batches
    mat_batches = []
    for _ in range(NUM_MAT_BATCHES):
        mat_batches.append({
            'batch_id': random.choice(batch_ids),
            'material_id': f"MAT_{random.randint(1, 10):02d}",
            'supplier_id': f"SUP_{random.randint(1, 5):02d}",
            'received_date': (start_date - timedelta(days=random.randint(1, 15))).strftime("%Y-%m-%d"),
            'quality_grade': random.choice(['A', 'B', 'C'])
        })
    pd.DataFrame(mat_batches).to_csv(os.path.join(base_dir, 'material_batches.csv'), index=False)

    print(f"Sample data generated successfully in {base_dir}")

if __name__ == "__main__":
    generate_sample_data()
