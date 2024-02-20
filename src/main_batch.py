from ga_extractor import fetch_ga4_data, list_properties_by_account
from sql_loader import insert_data_into_sql_server
import logging
from datetime import datetime, timedelta

# Configure logging as previously described
logging.basicConfig(level=logging.INFO, filename='logs/etl_log_batch.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

def generate_date_batches(start_date, end_date, delta):
    """Generate batches of date ranges."""
    current_date = start_date
    while current_date < end_date:
        batch_end_date = min(current_date + delta, end_date)
        yield current_date, batch_end_date
        current_date = batch_end_date + timedelta(days=1)

def run_etl_pipeline_batch(start_date_str, end_date_str, delta, metrics, dimensions):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    try:
        properties = list_properties_by_account()
    except Exception as e:
        logging.error(f"Failed to list GA4 properties: {e}")
        return

    property_ids = [prop['property_id'] for account_props in properties.values() for prop in account_props]
    

    for batch_start_date, batch_end_date in generate_date_batches(start_date, end_date, delta):
        batch_start_str = batch_start_date.strftime("%Y-%m-%d")
        batch_end_str = batch_end_date.strftime("%Y-%m-%d")
        logging.info(f"Processing batch: {batch_start_str} to {batch_end_str}")

        for property_id in property_ids:
            try:
                logging.info(f"Fetching data for property ID: {property_id}, Date Range: {batch_start_str} to {batch_end_str}")
                data = fetch_ga4_data([property_id], batch_start_str, batch_end_str, metrics, dimensions)
                if data:
                    insert_data_into_sql_server(data)
            except Exception as e:
                logging.error(f"Error processing property ID {property_id} for batch {batch_start_str} to {batch_end_str}: {e}")
                continue

if __name__ == "__main__":
    METRICS = ['activeUsers']
    DIMENSIONS = ['country']
    
    start_date_str = '2023-01-01'
    end_date_str = '2023-12-31'
    delta = timedelta(days=30) # Define batch size (e.g., one month)

    run_etl_pipeline_batch(start_date_str, end_date_str, delta, METRICS, DIMENSIONS)
