from peewee import Model, AutoField, CharField, DateTimeField, ForeignKeyField, MySQLDatabase
from datetime import datetime, timezone
import pytz
import logging
import json

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

class CMS_to_Charger(BaseModel):
    id = AutoField()  # Auto-incrementing primary key
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = 'CMS_to_Charger'

class CMS_to_Charger_IdTagInfo(BaseModel):
    id = AutoField()
    ocpp_message = ForeignKeyField(CMS_to_Charger, backref='id_tag_info')
    status = CharField()
    expiry_date = DateTimeField(null=True)
    parent_id_tag = CharField(null=True)

    class Meta:
        table_name = 'CMS_to_Charger_IdTagInfo'

class CMS_to_Charger_Acknowledgment(BaseModel):
    id = AutoField()
    ocpp_message = ForeignKeyField(CMS_to_Charger, backref='acknowledgments')
    status = CharField()
    file_name = CharField(null=True)

    class Meta:
        table_name = 'CMS_to_Charger_Acknowledgment'

# Ensure the tables are created
db.connect()
db.create_tables([CMS_to_Charger, CMS_to_Charger_IdTagInfo, CMS_to_Charger_Acknowledgment])

IST = pytz.timezone('Asia/Kolkata')

# Function to convert any datetime to IST
def convert_to_ist(dt):
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)

def get_ist_time():
    return datetime.now(timezone.utc).astimezone(IST)

def get_existing_columns(table):
    cursor = db.execute_sql(f'SHOW COLUMNS FROM {table}')
    return [column[0] for column in cursor.fetchall()]

def add_column(table, column_name, column_type):
    try:
        if column_type == 'string':
            db.execute_sql(f'ALTER TABLE {table} ADD COLUMN {column_name} TEXT')
        elif column_type == 'integer':
            db.execute_sql(f'ALTER TABLE {table} ADD COLUMN {column_name} INTEGER')
        elif column_type == 'float':
            db.execute_sql(f'ALTER TABLE {table} ADD COLUMN {column_name} FLOAT')
        elif column_type == 'datetime':
            db.execute_sql(f'ALTER TABLE {table} ADD COLUMN {column_name} DATETIME')
    except Exception as e:
        logging.error(f"Error adding column {column_name} to table {table}: {e}")

def insert_data(table, data):
    existing_columns = get_existing_columns(table)
    logging.debug(f"Existing columns in {table}: {existing_columns}")
    logging.debug(f"Data to insert into {table}: {data}")

    # Add columns dynamically if they do not exist
    for key, value in data.items():
        if key not in existing_columns:
            if isinstance(value, str):
                add_column(table, key, 'string')
            elif isinstance(value, int):
                add_column(table, key, 'integer')
            elif isinstance(value, float):
                add_column(table, key, 'float')
            elif isinstance(value, datetime):
                add_column(table, key, 'datetime')

    # Serialize complex data structures to JSON strings
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            data[key] = json.dumps(value)

    # Ensure the columns are ordered correctly
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values = list(data.values())

    # Build the insert query
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    logging.debug(f"Executing query on {table}: {query}")
    logging.debug(f"With values: {values}")

    # Execute the insert query
    db.execute_sql(query, values)

def store_ocpp_message(charger_id, message_type, message_category, **kwargs):
    original_message_time = kwargs.get('original_message_time', None)
    if original_message_time:
        original_message_time = convert_to_ist(original_message_time)

    data = {
        "message_type": message_type,
        "charger_id": charger_id,
        "message_category": message_category,
        "original_message_type": kwargs.get('original_message_type', None) if message_category == 'Acknowledgment' else None,
        "original_message_time": original_message_time,
        "timestamp": get_ist_time(),
    }
    logging.debug(f"Store OCPP message data: {data}")

    # Add additional columns dynamically
    data.update(kwargs)
    
    insert_data('CMS_to_Charger', data)
    return CMS_to_Charger.create(**data)

def parse_and_store_acknowledgment(charger_id, message_type, original_message_type, original_message_time, **kwargs):
    ocpp_message = store_ocpp_message(charger_id, message_type, "Acknowledgment", original_message_type=original_message_type, original_message_time=convert_to_ist(original_message_time), **kwargs)

    ack_data = {
        "ocpp_message": ocpp_message.id,
        "status": kwargs.get('status'),
        "file_name": kwargs.get('fileName')
    }
    insert_data('CMS_to_Charger_Acknowledgment', ack_data)

# Example functions for specific message types
def parse_and_store_remote_start_transaction(charger_id, **kwargs):
    message_type = "RemoteStartTransaction"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_remote_stop_transaction(charger_id, **kwargs):
    message_type = "RemoteStopTransaction"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_change_availability(charger_id, **kwargs):
    message_type = "ChangeAvailability"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_change_configuration(charger_id, **kwargs):
    message_type = "ChangeConfiguration"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_clear_cache(charger_id, **kwargs):
    message_type = "ClearCache"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_unlock_connector(charger_id, **kwargs):
    message_type = "UnlockConnector"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_get_diagnostics(charger_id, **kwargs):
    message_type = "GetDiagnostics"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_update_firmware(charger_id, **kwargs):
    message_type = "UpdateFirmware"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_reset(charger_id, **kwargs):
    message_type = "Reset"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_acknowledgment(charger_id, message_type, original_message_type, original_message_time, **kwargs):
    store_ocpp_message(charger_id, message_type, "Acknowledgment", original_message_type=original_message_type, original_message_time=convert_to_ist(original_message_time), **kwargs)

# Close the database connection when done
def close_connection():
    db.close()
