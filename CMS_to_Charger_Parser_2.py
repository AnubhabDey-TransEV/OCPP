import logging
from datetime import datetime, timezone
import pytz
import uuid
import json
from dbconn import get_database

# Get the database connection
db = get_database()

def get_existing_columns():
    cursor = db.execute_sql('SHOW COLUMNS FROM CMS_to_Charger')
    return [column[0] for column in cursor.fetchall()]

def add_column(column_name, column_type):
    try:
        if column_type == 'string':
            db.execute_sql(f'ALTER TABLE CMS_to_Charger ADD COLUMN `{column_name}` TEXT')
        elif column_type == 'integer':
            db.execute_sql(f'ALTER TABLE CMS_to_Charger ADD COLUMN `{column_name}` INTEGER')
        elif column_type == 'float':
            db.execute_sql(f'ALTER TABLE CMS_to_Charger ADD COLUMN `{column_name}` FLOAT')
        elif column_type == 'datetime':
            db.execute_sql(f'ALTER TABLE CMS_to_Charger ADD COLUMN `{column_name}` DATETIME')
        logging.debug(f"Added column {column_name} of type {column_type}")
    except Exception as e:
        logging.error(f"Error adding column {column_name}: {e}")

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, sub_dict in enumerate(v):
                if isinstance(sub_dict, dict):
                    items.extend(flatten_dict(sub_dict, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", sub_dict))
        else:
            items.append((new_key, v))
    return dict(items)

def convert_to_ist(original_time):
    ist = pytz.timezone('Asia/Kolkata')
    if isinstance(original_time, str):
        try:
            original_time = datetime.fromisoformat(original_time.replace("Z", "+00:00"))
        except ValueError:
            logging.error(f"Failed to parse datetime string: {original_time}")
            return None
    if original_time.tzinfo is None:
        return original_time.astimezone(ist)

def insert_data(data):
    existing_columns = get_existing_columns()
    logging.debug(f"Existing columns: {existing_columns}")
    logging.debug(f"Data to insert: {data}")

    # Flatten the data
    data = flatten_dict(data)

    # Convert all datetime fields to IST
    for key, value in data.items():
        if isinstance(value, str):
            try:
                parsed_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
                data[key] = convert_to_ist(parsed_time)
            except ValueError:
                pass
        elif isinstance(value, datetime):
            data[key] = convert_to_ist(value)

    # Serialize data values that are dicts or lists
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            data[key] = json.dumps(value)

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
    columns = ", ".join([f"`{col}`" for col in data.keys()])
    placeholders = ", ".join(["%s"] * len(data))
    values = list(data.values())

    # Build the insert query
    query = f"INSERT INTO CMS_to_Charger ({columns}) VALUES ({placeholders})"
    logging.debug(f"Executing query: {query}")
    logging.debug(f"With values: {values}")

    # Execute the insert query
    db.execute_sql(query, values)

def get_ist_time():
    return datetime.now()

# Function to format and store messages and acknowledgments from CMS to Chargers
def store_ocpp_message(charger_id, message_type, message_category, **kwargs):
    try:
        original_message_type = kwargs.get('original_message_type', None)
        original_message_time = kwargs.get('original_message_time', None)

        # Log the types and values of kwargs
        logging.debug(f"kwargs received: {kwargs}")
        logging.debug(f"original_message_type: {type(original_message_type)} = {original_message_type}")
        logging.debug(f"original_message_time: {type(original_message_time)} = {original_message_time}")

        data = {
            "uuiddb": str(uuid.uuid4()),
            "message_type": message_type,
            "charger_id": charger_id,
            "message_category": message_category,
            "original_message_type": original_message_type if message_category == 'Acknowledgment' else None,
            "original_message_time": convert_to_ist(original_message_time) if message_category == 'Acknowledgment' and original_message_time else None,
            "timestamp": get_ist_time()
        }

        logging.debug(f"Store OCPP message data before update: {data}")

        # Add additional columns dynamically, ensuring no type issues
        for key, value in kwargs.items():
            if key not in data:
                data[key] = value

        logging.debug(f"Store OCPP message data after update: {data}")

        # Insert the data using the OCPPMessageCMS model
        insert_data(data)
        
    except Exception as e:
        logging.error(f"An error occurred in store_ocpp_message: {e}")
        raise

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
    message_type = "SetVariables"  # OCPP 2.0.1 replaces ChangeConfiguration with SetVariables
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

def parse_and_store_get_configuration(charger_id, **kwargs):
    message_type = "GetVariables"  # OCPP 2.0.1 replaces GetConfiguration with GetVariables
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_acknowledgment(charger_id, message_type, original_message_type, original_message_time, **kwargs):
    store_ocpp_message(charger_id, message_type, "Acknowledgment", original_message_type=original_message_type, original_message_time=original_message_time, **kwargs)

def parse_and_store_trigger_message(charger_id, **kwargs):
    message_type = "TriggerMessage"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_reserve_now(charger_id, **kwargs):
    message_type = "ReserveNow"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

def parse_and_store_cancel_reservation(charger_id, **kwargs):
    message_type = "CancelReservation"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)

# Close the database connection when done
def close_connection():
    db.close()
    logging.debug("Database connection closed successfully.")
