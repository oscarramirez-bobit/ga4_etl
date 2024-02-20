import pyodbc
import json

#TODO change the INSERT INTO script to ensure the data we import matches the schema of the table we create.

# Load the configuration
with open('config.json', 'r') as f:
    config = json.load(f)

sql_server_config = config['sql_server']

connection_string = (
    f"SERVER={sql_server_config['server']};"
    f"DATABASE={sql_server_config['database']};"
    f"UID={sql_server_config['username']};"
    f"PWD={sql_server_config['password']}"
)

cnxn = pyodbc.connect(connection_string)
cursor = cnxn.cursor()

def insert_data_into_sql_server(data):
    insert_stmt = """
    INSERT INTO GA4Data (PropertyID, Date, Country, ActiveUsers)
    VALUES (?, ?, ?, ?)
    """
    for record in data:
        # Assuming 'record' is a dictionary with keys matching your data structure
        params = (record['property_id'], record['date'], record['country'], record['activeUsers'])
        cursor.execute(insert_stmt, params)
    cnxn.commit()  # Commit the transaction