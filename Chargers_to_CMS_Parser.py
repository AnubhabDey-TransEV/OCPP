from peewee import Model, AutoField, CharField, DateTimeField, MySQLDatabase
from datetime import datetime, timezone, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Database connection
DATABASE_URI = {
    'host': 'localhost',
    'user': 'ocpphandler',
    'password': 'ocpp2024',
    'database': 'OCPP'
}
db = MySQLDatabase(DATABASE_URI['database'], user=DATABASE_URI['user'], password=DATABASE_URI['password'], host=DATABASE_URI['host'], port=3306)

class BaseModel(Model):
    class Meta:
        database = db

class OCPPMessage(BaseModel):
    id = AutoField()  # Auto-incrementing primary key
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = 'Charger_to_CMS'

# Ensure the table is created
db.connect()
db.create_tables([OCPPMessage])

def get_existing_columns():
    cursor = db.execute_sql('SHOW COLUMNS FROM Charger_to_CMS')
    return [column[0] for column in cursor.fetchall()]

def add_column(column_name, column_type):
    try:
        if column_type == 'string':
            db.execute_sql(f'ALTER TABLE Charger_to_CMS ADD COLUMN {column_name} TEXT')
        elif column_type == 'integer':
            db.execute_sql(f'ALTER TABLE Charger_to_CMS ADD COLUMN {column_name} INTEGER')
        elif column_type == 'float':
            db.execute_sql(f'ALTER TABLE Charger_to_CMS ADD COLUMN {column_name} FLOAT')
        elif column_type == 'datetime':
            db.execute_sql(f'ALTER TABLE Charger_to_CMS ADD COLUMN {column_name} DATETIME')
    except Exception as e:
        logging.error(f"Error adding column {column_name}: {e}")

def insert_data(data):
    existing_columns = get_existing_columns()
    logging.debug(f"Existing columns: {existing_columns}")
    logging.debug(f"Data to insert: {data}")

    # Add columns dynamically if they do not exist
    for key, value in data.items():
        if key not in existing_columns:
            if isinstance(value, str):
                add_column(key, 'string')
            elif isinstance(value, int):
                add_column(key, 'integer')
            elif isinstance(value, float):
                add_column(key, 'float')
            elif isinstance(value, datetime):
                add_column(key, 'datetime')

    # Ensure the columns are ordered correctly
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values = list(data.values())

    # Build the insert query
    query = f"INSERT INTO Charger_to_CMS ({columns}) VALUES ({placeholders})"
    logging.debug(f"Executing query: {query}")
    logging.debug(f"With values: {values}")

    # Execute the insert query
    db.execute_sql(query, values)

def get_ist_time():
    utc_time = datetime.now(timezone.utc)
    ist_time = utc_time + timedelta(hours=5, minutes=30)
    return ist_time

# Function to format and store messages and acknowledgments from Chargers to CMS
def store_ocpp_message(charger_id, message_type, message_category, **kwargs):
    data = {
        "message_type": message_type,
        "charger_id": charger_id,
        "message_category": message_category,
        "original_message_type": kwargs.get('original_message_type', None) if message_category == 'Acknowledgment' else None,
        "original_message_time": kwargs.get('original_message_time', None) if message_category == 'Acknowledgment' else None,
        "timestamp": get_ist_time()
    }
    logging.debug(f"Store OCPP message data: {data}")

    # Add additional columns dynamically
    data.update(kwargs)
    
    insert_data(data)

# Example functions for specific message types
def parse_and_store_boot_notification(charger_id, **kwargs):
    message_type = "BootNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_heartbeat(charger_id, **kwargs):
    message_type = "Heartbeat"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_start_transaction(charger_id, **kwargs):
    message_type = "StartTransaction"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_stop_transaction(charger_id, **kwargs):
    message_type = "StopTransaction"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_meter_values(charger_id, **kwargs):
    message_type = "MeterValues"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_status_notification(charger_id, **kwargs):
    message_type = "StatusNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_diagnostics_status(charger_id, **kwargs):
    message_type = "DiagnosticsStatusNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_firmware_status(charger_id, **kwargs):
    message_type = "FirmwareStatusNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_acknowledgment(charger_id, message_type, original_message_type, original_message_time, **kwargs):
    store_ocpp_message(charger_id, message_type, "Acknowledgment", original_message_type=original_message_type, original_message_time=original_message_time, **kwargs)

# Close the database connection when done
def close_connection():
    db.close()
