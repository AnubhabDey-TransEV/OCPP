import logging
from datetime import datetime, timezone
import pytz
import json
import uuid
from dbconn import (
    get_database,
)  # Import the get_database function to get the database connection

db = get_database()


def get_existing_columns():
    cursor = db.execute_sql("SHOW COLUMNS FROM Charger_to_CMS")
    return [column[0] for column in cursor.fetchall()]


def add_column(column_name, column_type):
    try:
        if column_type == "string":
            db.execute_sql(
                f"ALTER TABLE Charger_to_CMS ADD COLUMN `{column_name}` TEXT"
            )
        elif column_type == "integer":
            db.execute_sql(
                f"ALTER TABLE Charger_to_CMS ADD COLUMN `{column_name}` INTEGER"
            )
        elif column_type == "float":
            db.execute_sql(
                f"ALTER TABLE Charger_to_CMS ADD COLUMN `{column_name}` FLOAT"
            )
        elif column_type == "datetime":
            db.execute_sql(
                f"ALTER TABLE Charger_to_CMS ADD COLUMN `{column_name}` DATETIME"
            )
        logging.debug(f"Added column {column_name} of type {column_type}")
    except Exception as e:
        logging.error(f"Error adding column {column_name}: {e}")


def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, sub_dict in enumerate(v):
                if isinstance(sub_dict, dict):
                    items.extend(
                        flatten_dict(
                            sub_dict, f"{new_key}{sep}{i}", sep=sep
                        ).items()
                    )
                else:
                    items.append((f"{new_key}{sep}{i}", sub_dict))
        else:
            items.append((new_key, v))
    return dict(items)


def convert_to_ist(original_time):
    ist = pytz.timezone("Asia/Kolkata")
    if isinstance(original_time, str):
        try:
            original_time = datetime.fromisoformat(
                original_time.replace("Z", "+00:00")
            )
        except ValueError:
            logging.error(f"Failed to parse datetime string: {original_time}")
            return None
    if original_time.tzinfo is None:
        return original_time


# def insert_data(data):
#     existing_columns = get_existing_columns()
#     logging.debug(f"Existing columns: {existing_columns}")
#     logging.debug(f"Data to insert: {data}")

#     # Add payload column if necessary
#     if "payload" in data and "payload" not in existing_columns:
#         add_column("payload", "string")

#     # Flatten the data for non-JSON messages
#     if "payload" not in data:
#         data = flatten_dict(data)

#     # Convert all datetime fields to IST
#     for key, value in data.items():
#         if isinstance(value, str):
#             try:
#                 parsed_time = datetime.fromisoformat(
#                     value.replace("Z", "+00:00")
#                 )
#                 data[key] = convert_to_ist(parsed_time)
#             except ValueError:
#                 pass
#         elif isinstance(value, datetime):
#             data[key] = convert_to_ist(value)

#     # Serialize data values that are dicts or lists
#     for key, value in data.items():
#         if isinstance(value, (dict, list)) and key != "payload":
#             data[key] = json.dumps(value)

#     # Add columns dynamically if they do not exist
#     for key, value in data.items():
#         if key not in existing_columns:
#             if isinstance(value, str):
#                 add_column(key, "string")
#             elif isinstance(value, int):
#                 add_column(key, "integer")
#             elif isinstance(value, float):
#                 add_column(key, "float")
#             elif isinstance(value, datetime):
#                 add_column(key, "datetime")

#     existing_columns = get_existing_columns()  # refresh after adding new ones

#     # Ensure the columns are ordered correctly
#     columns = ", ".join([f"`{col}`" for col in data.keys()])
#     placeholders = ", ".join(["%s"] * len(data))
#     values = list(data.values())

#     # Build the insert query
#     query = f"INSERT INTO Charger_to_CMS ({columns}) VALUES ({placeholders})"
#     logging.debug(f"Executing query: {query}")
#     logging.debug(f"With values: {values}")

#     # Execute the insert query
#     db.execute_sql(query, values)

def insert_data(data):
    existing_columns = get_existing_columns()
    print(f"Existing columns: {existing_columns}")
    print(f"Data to insert (pre-flatten): {data}")

    # Flatten if no payload (payloads are raw dumps)
    if "payload" not in data:
        data = flatten_dict(data)

    # Convert datetime strings to IST datetime objects
    for key, value in data.items():
        if isinstance(value, str):
            try:
                parsed_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
                data[key] = convert_to_ist(parsed_time)
            except ValueError:
                pass
        elif isinstance(value, datetime):
            data[key] = convert_to_ist(value)

    # Serialize dicts/lists (unless it's the raw payload)
    for key, value in data.items():
        if isinstance(value, (dict, list)) and key != "payload":
            data[key] = json.dumps(value)

    # Try adding missing columns dynamically
    for key, value in data.items():
        if key not in existing_columns:
            try:
                if isinstance(value, str):
                    add_column(key, "string")
                elif isinstance(value, int):
                    add_column(key, "integer")
                elif isinstance(value, float):
                    add_column(key, "float")
                elif isinstance(value, datetime):
                    add_column(key, "datetime")
                else:
                    print(f"Skipped column '{key}' — unsupported type: {type(value)}")
            except Exception as e:
                print(f"Could not add column '{key}' — Reason: {str(e)}")

    # Refresh after attempting column adds
    existing_columns = get_existing_columns()

    # Filter data to include only columns that actually exist
    filtered_data = {}
    for k, v in data.items():
        if k in existing_columns:
            filtered_data[k] = v
        else:
            print(f"Column '{k}' not found in DB after ALTER attempt — dropping from insert")

    # Prepare insert query
    columns = ", ".join([f"`{col}`" for col in filtered_data.keys()])
    placeholders = ", ".join(["%s"] * len(filtered_data))
    values = list(filtered_data.values())
    query = f"INSERT INTO Charger_to_CMS ({columns}) VALUES ({placeholders})"
    print(f"Final Insert Query: {query}")
    print(f"With Values: {values}")

    # Execute the insert
    try:
        db.execute_sql(query, values)
    except Exception as e:
        print(f"INSERT failed hard: {e}")
        raise



def get_ist_time():
    return datetime.now()


# Function to format and store messages and acknowledgments from Chargers to CMS
def store_ocpp_message(charger_id, message_type, message_category, **kwargs):
    try:
        original_message_type = kwargs.get("original_message_type", None)
        original_message_time = kwargs.get("original_message_time", None)

        # Log the types and values of kwargs
        logging.debug(f"kwargs received: {kwargs}")
        logging.debug(
            f"original_message_type: {type(original_message_type)} = {original_message_type}"
        )
        logging.debug(
            f"original_message_time: {type(original_message_time)} = {original_message_time}"
        )

        data = {
            "uuiddb": str(uuid.uuid4()),
            "message_type": message_type,
            "charger_id": charger_id,
            "message_category": message_category,
            "original_message_type": (
                original_message_type
                if message_category == "Acknowledgment"
                else None
            ),
            "original_message_time": (
                convert_to_ist(original_message_time)
                if message_category == "Acknowledgment"
                and original_message_time
                else None
            ),
            "timestamp": get_ist_time(),
        }

        # Store the entire payload as JSON for specific message types
        if message_type in ["MeterValues", "ConfigurationResponse"]:
            data["payload"] = json.dumps(
                kwargs
            )  # Store kwargs as JSON in payload
        else:
            # Add additional columns dynamically, ensuring no type issues
            for key, value in kwargs.items():
                if key not in data:
                    data[key] = value

        logging.debug(f"Store OCPP message data after update: {data}")

        # Insert data into the database
        insert_data(data)

    except Exception as e:
        logging.error(f"An error occurred in store_ocpp_message: {e}")
        raise


# Example functions for specific message types
def parse_and_store_boot_notification(charger_id, **kwargs):
    message_type = "BootNotification"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)


def parse_and_store_reserve_now_response(charger_id, **kwargs):
    message_type = "ReserveNowResponse"
    store_ocpp_message(charger_id, message_type, "Response", **kwargs)


def parse_and_store_cancel_reservation_response(charger_id, **kwargs):
    message_type = "CancelReservationResponse"
    store_ocpp_message(charger_id, message_type, "Response", **kwargs)


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


def parse_and_store_get_configuration_response(charger_id, configuration_key):
    message_type = "ConfigurationResponse"
    store_ocpp_message(
        charger_id,
        message_type,
        "Response",
        configuration_key=configuration_key,
    )


def parse_and_store_authorize(charger_id, **kwargs):
    message_type = "Authorize"
    store_ocpp_message(charger_id, message_type, "Request", **kwargs)


def parse_and_store_acknowledgment(
    charger_id,
    message_type,
    original_message_type,
    original_message_time,
    **kwargs,
):
    store_ocpp_message(
        charger_id,
        message_type,
        "Acknowledgment",
        original_message_type=original_message_type,
        original_message_time=original_message_time,
        **kwargs,
    )


# Close the database connection when done
def close_connection():
    db.close()
    logging.debug("Database connection closed successfully.")
