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

class Charger_to_CMS(BaseModel):
    id = AutoField()  # Auto-incrementing primary key
    message_type = CharField()
    charger_id = CharField()
    message_category = CharField()
    original_message_type = CharField(null=True)
    original_message_time = DateTimeField(null=True)
    timestamp = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = 'Charger_to_CMS'

class Charger_to_CMS_MeterValue(BaseModel):
    id = AutoField()
    ocpp_message = ForeignKeyField(Charger_to_CMS, backref='meter_values')
    timestamp = DateTimeField()
    connector_id = CharField()
    transaction_id = CharField()

    class Meta:
        table_name = 'Charger_to_CMS_MeterValue'

class Charger_to_CMS_SampledValue(BaseModel):
    id = AutoField()
    meter_value = ForeignKeyField(Charger_to_CMS_MeterValue, backref='sampled_values')
    value = CharField()
    context = CharField(null=True)
    format = CharField(null=True)
    measurand = CharField(null=True)
    phase = CharField(null=True)
    location = CharField(null=True)
    unit = CharField(null=True)

    class Meta:
        table_name = 'Charger_to_CMS_SampledValue'

class Charger_to_CMS_Acknowledgment(BaseModel):
    id = AutoField()
    ocpp_message = ForeignKeyField(Charger_to_CMS, backref='acknowledgments')
    status = CharField()
    file_name = CharField(null=True)

    class Meta:
        table_name = 'Charger_to_CMS_Acknowledgment'

# Ensure the tables are created
db.connect()
db.create_tables([Charger_to_CMS, Charger_to_CMS_MeterValue, Charger_to_CMS_SampledValue, Charger_to_CMS_Acknowledgment])

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
    
    insert_data('Charger_to_CMS', data)
    ocpp_message = Charger_to_CMS.create(**data)
    ocpp_message.save()  # Ensure the main record is saved
    return ocpp_message

def parse_and_store_acknowledgment(charger_id, message_type, original_message_type, original_message_time, **kwargs):
    ocpp_message = store_ocpp_message(charger_id, message_type, "Acknowledgment", original_message_type=original_message_type, original_message_time=convert_to_ist(original_message_time), **kwargs)

    ack_data = {
        "ocpp_message": ocpp_message.id,
        "status": kwargs.get('status'),
        "file_name": kwargs.get('fileName')
    }
    insert_data('Charger_to_CMS_Acknowledgment', ack_data)

def parse_and_store_meter_values(charger_id, connector_id, transaction_id, meter_value, **kwargs):
    message_type = "MeterValues"
    ocpp_message = store_ocpp_message(charger_id, message_type, "Request", connector_id=connector_id, transaction_id=transaction_id)

    for value in meter_value:
        meter_value_data = {
            "ocpp_message": ocpp_message.id,
            "timestamp": convert_to_ist(value['timestamp']),
            "connector_id": connector_id,
            "transaction_id": transaction_id
        }
        ocpp_message.save()  # Ensure the main record is saved
        insert_data('Charger_to_CMS_MeterValue', meter_value_data)
        meter_value_record = Charger_to_CMS_MeterValue.get(Charger_to_CMS_MeterValue.ocpp_message == ocpp_message.id, Charger_to_CMS_MeterValue.timestamp == meter_value_data['timestamp'])

        for sampled in value['sampledValue']:
            sampled_value_data = {
                "meter_value": meter_value_record.id
            }
            # Add dynamic columns for sampled values
            sampled_value_data.update(sampled)
            insert_data('Charger_to_CMS_SampledValue', sampled_value_data)

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
    ocpp_message = store_ocpp_message(charger_id, message_type, "Request", **kwargs)

    transaction_data = kwargs.get('transactionData', [])
    for data in transaction_data:
        meter_value_data = {
            "ocpp_message": ocpp_message.id,
            "timestamp": convert_to_ist(data['timestamp']),
        }
        ocpp_message.save()  # Ensure the main record is saved
        insert_data('Charger_to_CMS_MeterValue', meter_value_data)
        meter_value_record = Charger_to_CMS_MeterValue.get(Charger_to_CMS_MeterValue.ocpp_message == ocpp_message.id, Charger_to_CMS_MeterValue.timestamp == meter_value_data['timestamp'])

        for sampled in data['sampledValue']:
            sampled_value_data = {
                "meter_value": meter_value_record.id
            }
            # Add dynamic columns for sampled values
            sampled_value_data.update(sampled)
            insert_data('Charger_to_CMS_SampledValue', sampled_value_data)

def parse_and_store_status_notification(charger_id, **kwargs):
    message_type = "StatusNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_diagnostics_status(charger_id, **kwargs):
    message_type = "DiagnosticsStatusNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_firmware_status(charger_id, **kwargs):
    message_type = "FirmwareStatusNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

# Close the database connection when done
def close_connection():
    db.close()
