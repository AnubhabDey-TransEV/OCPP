import logging

import httpx
from decouple import config

APP_LAYER_URL = config("APP_LAYER_URL")  # 👈 pull from .env


async def post_transaction_to_app(transaction_record):
    payload = {
        "transaction_id": transaction_record.id,  # Unique identifier for the charge session given by the Charging Station
        "charger_id": transaction_record.charger_id,  # Charger ID of the charging station assigned by Application Layer
        "connector_id": transaction_record.connector_id,  # Connector ID of the charger
        "meter_start": transaction_record.meter_start,  # Meter readings in Wh when started
        "meter_stop": transaction_record.meter_stop,  # Meter readings in Wh whe stopped
        "start_time": transaction_record.start_time.isoformat(),  # Start time in ISO format
        "stop_time": transaction_record.stop_time.isoformat()  # Stop time in ISO format
        if transaction_record.stop_time
        else None,
        "total_consumption": transaction_record.total_consumption,  # Total energy consumed in kWh
        "id_tag": transaction_record.id_tag,  # User ID managed by Application Layer
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(APP_LAYER_URL, json=payload)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logging.error(f"Error posting transaction to app layer: {e}")
        return None
