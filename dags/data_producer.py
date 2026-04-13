import json
import random
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

def generate_energy_data():
    regions = ['EMEA', 'APAC', 'NORTH_AMERICA', 'LATAM']
    
    # --- Generate Random Date (2021 - 2026) ---
    start_date = datetime(2021, 1, 1)
    end_date = datetime.now()
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + timedelta(days=random_number_of_days)
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "device_id": str(uuid.uuid4()), 
        "region": random.choice(regions),
        "kwh_usage": round(random.uniform(50.0, 500.0), 2),
        "carbon_tax_rate": 0.30, 
        "operational_status": "ACTIVE",
        # New Column: Month-Day-Year format
        "event_date": random_date.strftime('%m-%d-%Y') 
    }

# ... (rest of your run_producer code)

def run_producer():
    # Simulate a batch of 50 sensor readings
    batch = [generate_energy_data() for _ in range(1000)]
    project_root = Path(__file__).resolve().parent
    landing_zone = project_root / 'landing_zone'
    os.makedirs(landing_zone, exist_ok=True)

    file_name = landing_zone / f"energy_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(batch, f, indent=4)
    print(f"Generated: {file_name}")
    return str(file_name)

if __name__ == "__main__":
    run_producer()
