from peewee import Model, CharField, IntegerField, DateTimeField, MySQLDatabase
from datetime import datetime

# Database connection
DATABASE_URI = {
    'host': 'localhost',
    'user': 'username',
    'password': 'password',
    'database': 'OCPP'
}
db = MySQLDatabase(DATABASE_URI['database'], user=DATABASE_URI['user'], password=DATABASE_URI['password'], host=DATABASE_URI['host'], port=3306)

class BaseModel(Model):
    class Meta:
        database = db

class OCPPMessage(BaseModel):
    id = IntegerField(primary_key=True)
    Message_Type = CharField()
    Charger_ID = CharField()
    Message_Category = CharField()
    Original_Message_Type = CharField(null=True)
    Original_Message_Time = DateTimeField(null=True)
    Timestamp = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = 'Charger_to_CMS'

# Ensure the table is created
db.connect()
db.create_tables([OCPPMessage])

# Function to insert data into a table
def insert_data(data):
    OCPPMessage.create(**data)

# Function to format and store messages and acknowledgments from Chargers to CMS
def store_ocpp_message(charger_id, message_type, message_category, **kwargs):
    data = {
        "Message_Type": message_type,
        "Charger_ID": charger_id,
        "Message_Category": message_category,
        "Original_Message_Type": kwargs.get('original_message_type', None) if message_category == 'Acknowledgment' else None,
        "Original_Message_Time": kwargs.get('original_message_time', None) if message_category == 'Acknowledgment' else None,
        "Timestamp": datetime.utcnow(),
    }
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
