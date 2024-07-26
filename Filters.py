import json
from datetime import datetime

def currdatetime():
    """Return the current UTC date-time in ISO format."""
    return datetime.now(datetime.timezone.utc).isoformat()

def format_message(message_type, charger_id, acknowledgment, acknowledgment_from, **kwargs):
    """Format the message for storage with all fields."""
    formatted_message = {
        "Message Type": message_type,
        "Charger ID": charger_id,
        "Acknowledgment": acknowledgment,
        "Acknowledgment From": acknowledgment_from,
        "Timestamp": currdatetime(),
        # Request parameters
        "Vendor": kwargs.get("vendor", None),
        "Model": kwargs.get("model", None),
        "Connector ID (Request)": kwargs.get("connector_id", None),
        "Transaction ID (Request)": kwargs.get("transaction_id", None),
        "ID Tag (Request)": kwargs.get("id_tag", None),
        "Start Timestamp (Request)": kwargs.get("start_timestamp", None),
        "Meter Start (Request)": kwargs.get("meter_start", None),
        "Stop Timestamp (Request)": kwargs.get("stop_timestamp", None),
        "Meter Stop (Request)": kwargs.get("meter_stop", None),
        "Transaction Reason (Request)": kwargs.get("transaction_reason", None),
        "Status (Request)": kwargs.get("status", None),
        "Error Code (Request)": kwargs.get("error_code", None),
        "Info (Request)": kwargs.get("info", None),
        "Meter Values (Request)": kwargs.get("meter_value", None),
        "Diagnostics Status (Request)": kwargs.get("diagnostics_status", None),
        "Firmware Status (Request)": kwargs.get("firmware_status", None),
        "Location (Request)": kwargs.get("location", None),
        "Retrieve Date (Request)": kwargs.get("retrieve_date", None),
        "Retries (Request)": kwargs.get("retries", None),
        "Retry Interval (Request)": kwargs.get("retry_interval", None),
        "Request Type (Request)": kwargs.get("request_type", None),
        "Key (Request)": kwargs.get("key", None),
        "Value (Request)": kwargs.get("value", None),
        # Response parameters
        "Status (Response)": kwargs.get("status_response", None),
        "Error Code (Response)": kwargs.get("error_code_response", None),
        "Info (Response)": kwargs.get("info_response", None),
        "Meter Values (Response)": kwargs.get("meter_value_response", None),
        "Diagnostics Status (Response)": kwargs.get("diagnostics_status_response", None),
        "Firmware Status (Response)": kwargs.get("firmware_status_response", None),
        "Transaction ID (Response)": kwargs.get("transaction_id_response", None),
        "ID Tag (Response)": kwargs.get("id_tag_response", None),
        "Start Timestamp (Response)": kwargs.get("start_timestamp_response", None),
        "Meter Start (Response)": kwargs.get("meter_start_response", None),
        "Stop Timestamp (Response)": kwargs.get("stop_timestamp_response", None),
        "Meter Stop (Response)": kwargs.get("meter_stop_response", None),
        "Transaction Reason (Response)": kwargs.get("transaction_reason_response", None),
        "Remote Start Transaction (Response)": kwargs.get("remote_start_transaction_response", None),
        "Remote Stop Transaction (Response)": kwargs.get("remote_stop_transaction_response", None),
        "Change Availability (Response)": kwargs.get("change_availability_response", None),
        "Change Configuration (Response)": kwargs.get("change_configuration_response", None),
        "Clear Cache (Response)": kwargs.get("clear_cache_response", None),
        "Unlock Connector (Response)": kwargs.get("unlock_connector_response", None),
        "Get Diagnostics (Response)": kwargs.get("get_diagnostics_response", None),
        "Update Firmware (Response)": kwargs.get("update_firmware_response", None),
        "Reset (Response)": kwargs.get("reset_response", None)
    }
    
    return formatted_message

def jsonify_response(response):
    """Convert the formatted response to JSON."""
    return json.dumps(response)

# Example usage functions for specific message types

def format_boot_notification(charger_id, acknowledgment, acknowledgment_from, vendor, model, vendor_status=None, model_status=None):
    return format_message("Boot Notification", charger_id, acknowledgment, acknowledgment_from, vendor=vendor, model=model, status_response=vendor_status)

def format_heartbeat(charger_id, acknowledgment, acknowledgment_from):
    return format_message("Heartbeat", charger_id, acknowledgment, acknowledgment_from)

def format_start_transaction(charger_id, acknowledgment, acknowledgment_from, connector_id, id_tag, timestamp, meter_start, transaction_id_response=None, id_tag_response=None, status_response=None):
    return format_message("Start Transaction", charger_id, acknowledgment, acknowledgment_from, connector_id=connector_id, id_tag=id_tag, start_timestamp=timestamp, meter_start=meter_start, transaction_id_response=transaction_id_response, id_tag_response=id_tag_response, status_response=status_response)

def format_stop_transaction(charger_id, acknowledgment, acknowledgment_from, transaction_id, id_tag, timestamp, meter_stop, reason, transaction_id_response=None, id_tag_response=None, status_response=None):
    return format_message("Stop Transaction", charger_id, acknowledgment, acknowledgment_from, transaction_id=transaction_id, id_tag=id_tag, stop_timestamp=timestamp, meter_stop=meter_stop, transaction_reason=reason, transaction_id_response=transaction_id_response, id_tag_response=id_tag_response, status_response=status_response)

def format_meter_values(charger_id, acknowledgment, acknowledgment_from, connector_id, transaction_id, meter_value, status_response=None):
    return format_message("Meter Values", charger_id, acknowledgment, acknowledgment_from, connector_id=connector_id, transaction_id=transaction_id, meter_value=meter_value, status_response=status_response)

def format_status_notification(charger_id, acknowledgment, acknowledgment_from, connector_id, status, error_code):
    return format_message("Status Notification", charger_id, acknowledgment, acknowledgment_from, connector_id=connector_id, status=status, error_code=error_code)

def format_diagnostics_status(charger_id, acknowledgment, acknowledgment_from, status):
    return format_message("Diagnostics Status Notification", charger_id, acknowledgment, acknowledgment_from, diagnostics_status=status)

def format_firmware_status(charger_id, acknowledgment, acknowledgment_from, status):
    return format_message("Firmware Status Notification", charger_id, acknowledgment, acknowledgment_from, firmware_status=status)

def format_remote_start_transaction(charger_id, acknowledgment, acknowledgment_from, id_tag, connector_id, transaction_id_response=None, status_response=None):
    return format_message("Remote Start Transaction", charger_id, acknowledgment, acknowledgment_from, id_tag=id_tag, connector_id=connector_id, remote_start_transaction_response={"transaction_id": transaction_id_response, "status": status_response})

def format_remote_stop_transaction(charger_id, acknowledgment, acknowledgment_from, transaction_id, status_response=None):
    return format_message("Remote Stop Transaction", charger_id, acknowledgment, acknowledgment_from, transaction_id=transaction_id, remote_stop_transaction_response={"status": status_response})

def format_change_availability(charger_id, acknowledgment, acknowledgment_from, connector_id, type):
    return format_message("Change Availability", charger_id, acknowledgment, acknowledgment_from, connector_id=connector_id, request_type=type)

def format_change_configuration(charger_id, acknowledgment, acknowledgment_from, key, value):
    return format_message("Change Configuration", charger_id, acknowledgment, acknowledgment_from, key=key, value=value)

def format_clear_cache(charger_id, acknowledgment, acknowledgment_from):
    return format_message("Clear Cache", charger_id, acknowledgment, acknowledgment_from)

def format_unlock_connector(charger_id, acknowledgment, acknowledgment_from, connector_id):
    return format_message("Unlock Connector", charger_id, acknowledgment, acknowledgment_from, connector_id=connector_id)

def format_get_diagnostics(charger_id, acknowledgment, acknowledgment_from, location, start_time=None, stop_time=None, retries=None, retry_interval=None, status_response=None, info_response=None):
    return format_message("Get Diagnostics", charger_id, acknowledgment, acknowledgment_from, location=location, start_time=start_time, stop_time=stop_time, retries=retries, retry_interval=retry_interval, status_response=status_response, info_response=info_response)

def format_update_firmware(charger_id, acknowledgment, acknowledgment_from, location, retrieve_date, retries=None, retry_interval=None, status_response=None, info_response=None):
    return format_message("Update Firmware", charger_id, acknowledgment, acknowledgment_from, location=location, retrieve_date=retrieve_date, retries=retries, retry_interval=retry_interval, status_response=status_response, info_response=info_response)

def format_reset(charger_id, acknowledgment, acknowledgment_from, type, status_response=None):
    return format_message("Reset", charger_id, acknowledgment, acknowledgment_from, request_type=type, status_response=status_response)
