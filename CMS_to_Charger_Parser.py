from sqlalchemy import create_engine, Column, String, Integer, DateTime, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database connection
DATABASE_URI = 'mysql+pymysql://username:password@localhost/OCPP'
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Function to create a table with dynamic columns if it doesn't exist
def create_table_if_not_exists(table_name, columns):
    metadata = MetaData(engine)
    if not engine.dialect.has_table(engine, table_name):
        columns_definition = [
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('Message_Type', String(50)),
            Column('Charger_ID', String(50)),
            Column('Message_Category', String(50)),  # Request or Acknowledgment
            Column('Original_Message_Type', String(50), nullable=True),
            Column('Original_Message_Time', DateTime, nullable=True),
            Column('Timestamp', DateTime)
        ]
        for col in columns:
            columns_definition.append(Column(col, String(255)))
        table = Table(table_name, metadata, *columns_definition)
        metadata.create_all()

# Function to insert data into a table
def insert_data(table_name, data):
    metadata = MetaData(engine)
    table = Table(table_name, metadata, autoload_with=engine)
    insert_stmt = table.insert().values(data)
    conn = engine.connect()
    conn.execute(insert_stmt)
    conn.close()

# Function to format and store messages and acknowledgments from CMS to Charger
def store_ocpp_message(charger_id, message_type, message_category, **kwargs):
    table_name = "CMS_to_Charger"
    common_columns = ["Message_Type", "Charger_ID", "Message_Category", "Original_Message_Type", "Original_Message_Time", "Timestamp"]
    message_columns = list(kwargs.keys())
    all_columns = common_columns + message_columns
    create_table_if_not_exists(table_name, message_columns)

    data = {
        "Message_Type": message_type,
        "Charger_ID": charger_id,
        "Message_Category": message_category,
        "Original_Message_Type": kwargs.get('original_message_type', None) if message_category == 'Acknowledgment' else None,
        "Original_Message_Time": kwargs.get('original_message_time', None) if message_category == 'Acknowledgment' else None,
        "Timestamp": datetime.now(),
        **kwargs
    }

    insert_data(table_name, data)

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
    store_ocpp_message(charger_id, message_type, "Acknowledgment", original_message_type=original_message_type, original_message_time=original_message_time, **kwargs)

# Close the database connection when done
def close_connection():
    session.close()
