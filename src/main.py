from ga_extractor import list_properties_by_account, fetch_ga4_data
from sql_loader import insert_data_into_sql_server
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename='logs/etl_log.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')


METRICS = ['activeUsers']
DIMENSIONS = ['country']
START_DATE = '2023-01-01'  # Example start date
END_DATE = '2023-12-31'    # Example end date

def run_etl_pipeline():
    try:
        # Step 1: List all GA4 properties
        properties = list_properties_by_account()
    except Exception as e:
        logging.error(f"Failed to list GA4 properties: {e}")
        return

    property_ids = [prop['property_id'] for account_props in properties.values() for prop in account_props]

    all_data = []
    for property_id in property_ids:
        try:
            logging.info(f"Fetching data for property ID: {property_id}")
            data = fetch_ga4_data([property_id], START_DATE, END_DATE, METRICS, DIMENSIONS)
            all_data.extend(data)
        except Exception as e:
            logging.error(f"Failed to fetch data for property ID {property_id}: {e}")
            # Continue to the next property ID if one fails
            continue

    if all_data:
        try:
            logging.info("Loading data into SQL Server...")
            insert_data_into_sql_server(all_data)
            logging.info("Data loading complete.")
        except Exception as e:
            logging.error(f"Failed to load data into SQL Server: {e}")
    else:
        logging.info("No data to load.")


