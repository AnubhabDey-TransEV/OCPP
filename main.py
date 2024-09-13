import asyncio
import logging
from datetime import datetime, timezone as dt_timezone
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Header, Depends, Request, Query
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, Response
from pydantic import BaseModel
from typing import Optional, Dict
from peewee import DoesNotExist
from io import BytesIO
import qrcode
import uvicorn
# import uvloop
import requests
import json
from decouple import config
import os
from OCPP_Requests import ChargePoint  # Assuming this is where ChargePoint is implemented
from models import Reservation, QRCodeData, db, Analytics
from Chargers_to_CMS_Parser import parse_and_store_cancel_reservation_response  # Assuming this handles responses
from loggingHandler import setup_logging
from cachetools import TTLCache, cached
import time
import valkey
from contextlib import asynccontextmanager

setup_logging()

CHARGER_DATA_KEY = "charger_data_cache"
CACHE_EXPIRY = 7200  # Cache TTL in seconds (2 hours)  # Cache for 2 hours

API_KEY_NAME = "x-api-key"

app = FastAPI()

valkey_uri = config("VALKEY_URI")
valkey_client = valkey.from_url(valkey_uri)

async def verify_api_key_middleware(request: Request, call_next):
    
    if request.url.path.startswith("/api/"):
        api_key = request.headers.get("x-api-key")
        expected_api_key = config("API_KEY")
        logging.info(f"Received API Key: {api_key}, Expected API Key: {expected_api_key}")
        if api_key != expected_api_key:
            raise HTTPException(status_code=403, detail="Invalid API key")
    response = await call_next(request)
    return response

async def refresh_cache():
    try:
        charger_data = await central_system.get_charger_data()
        valkey_client.setex(CHARGER_DATA_KEY, CACHE_EXPIRY, json.dumps(charger_data))  # serialize as JSON string
        print("Charger data has been cached.")
    except Exception as e:
        print(f"Failed to refresh cache: {e}")

# Startup event calling the global refresh_cache function

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    try:
        await refresh_cache()  # Cache refresh logic at startup
        print("Startup: Cache refreshed.")
    except Exception as e:
        print(f"Failed to refresh cache: {e}")
    yield  # The app runs while it yields here
    # Shutdown actions
    try:
        valkey_client.delete(CHARGER_DATA_KEY)  # Cache cleanup logic at shutdown
        print("Shutdown: Cache cleared.")
    except Exception as e:
        print(f"Failed to clear cache on shutdown: {e}")

app = FastAPI(lifespan=lifespan)

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*']
    )
]

app = FastAPI(middleware=middleware)

class WebSocketAdapter:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket

    async def recv(self):
        return await self.websocket.receive_text()

    async def send(self, message):
        await self.websocket.send_text(message)

    async def close(self):
        await self.websocket.close()

class CentralSystem:  
    def __init__(self):
        self.charge_points = {}
        self.active_connections = {}
        self.verification_failures = {}
        self.frontend_connections = {}

    async def handle_charge_point(self, websocket: WebSocket, charge_point_id: str):
        await websocket.accept()

        # Verify if the charger ID exists
        if not await self.verify_charger_id(charge_point_id):
            await websocket.close(code=1000)
            return

        logging.info(f"Charge point {charge_point_id} connected.")

        # Store the WebSocket connection in Valkey with the charge_point_id as the key
        valkey_client.set(f"active_connections:{charge_point_id}", os.getpid())  # Store worker/process ID with charge_point_id

        # Create a WebSocket adapter for the charge point
        ws_adapter = WebSocketAdapter(websocket)
        charge_point = ChargePoint(charge_point_id, ws_adapter)

        # Store the charge point object in the local in-memory dictionary
        self.charge_points[charge_point_id] = charge_point
        self.charge_points[charge_point_id].online = True
        await self.notify_frontend(charge_point_id, online=True)

        try:
            # Start the charge point in a background task
            start_task = asyncio.create_task(charge_point.start())

            # Wait for a brief moment to ensure the start method has begun
            await asyncio.sleep(5)  # Adjust the sleep time as necessary

            # Now send the ChangeConfiguration request (e.g., changing heartbeat interval)
            await self.send_heartbeat_interval_change(charge_point_id)

            await self.send_sampled_metervalues_interval_change(charge_point_id)

            # Await the completion of the start method
            await start_task

        except WebSocketDisconnect:
            # Handle WebSocket disconnect: remove from Valkey and local memory
            logging.info(f"Charge point {charge_point_id} disconnected.")
            valkey_client.delete(f"active_connections:{charge_point_id}")  # Remove from Valkey
            self.charge_points[charge_point_id].online = False  # Remove from local memory
            await self.notify_frontend(charge_point_id, online=False)

        except Exception as e:
            logging.error(f"Error occurred while handling charge point {charge_point_id}: {e}")
            valkey_client.delete(f"active_connections:{charge_point_id}")  # Remove from Valkey in case of an error
            self.charge_points[charge_point_id].online = False
            await self.notify_frontend(charge_point_id, online=False)
            await websocket.close()

    async def handle_frontend_websocket(self, websocket: WebSocket, uid: str):
        await websocket.accept()

        # Track the frontend connection
        if uid not in self.frontend_connections:
            self.frontend_connections[uid] = []
        self.frontend_connections[uid].append(websocket)

        # Notify the newly connected frontend about the current status of the charger (online/offline)
        if uid in self.charge_points and self.charge_points[uid].online:
            # Charger is online, notify the frontend immediately
            await self.notify_frontend(uid, online=True)
        else:
            # Charger is offline, notify the frontend immediately
            await self.notify_frontend(uid, online=False)

        try:
            while True:
                # Frontend will just be listening, no need to handle incoming messages
                await websocket.receive_text()

        except WebSocketDisconnect:
            logging.info(f"Frontend WebSocket for charger {uid} disconnected.")
            
            # Safely remove the WebSocket from the frontend connections list
            if uid in self.frontend_connections:
                self.frontend_connections[uid].remove(websocket)

                # If no other connections are active, remove the entry entirely
                if not self.frontend_connections[uid]:
                    del self.frontend_connections[uid]

        except Exception as e:
            logging.error(f"Error handling frontend WebSocket for charger {uid}: {e}")

            # Ensure the WebSocket is properly removed in case of an error
            if uid in self.frontend_connections and websocket in self.frontend_connections[uid]:
                self.frontend_connections[uid].remove(websocket)

                if not self.frontend_connections[uid]:
                    del self.frontend_connections[uid]

    async def notify_frontend(self, charge_point_id: str, online: bool):
    # Notify all frontends listening for this specific charger `uid`
        if charge_point_id in self.frontend_connections:
            for ws in list(self.frontend_connections[charge_point_id]):
                try:
                    # Send the charger status update to the frontend
                    await ws.send_json({
                        "charger_id": charge_point_id,
                        "status": "Online" if online else "Offline"
                    })
                except Exception as e:
                    logging.error(f"Error sending WebSocket message to frontend for charger {charge_point_id}: {e}")
                    # If sending fails, close and remove this WebSocket connection
                    await ws.close()
                    self.frontend_connections[charge_point_id].remove(ws)

            # Clean up if no more frontend connections exist for this charger
            if not self.frontend_connections[charge_point_id]:
                del self.frontend_connections[charge_point_id]

    async def send_heartbeat_interval_change(self, charge_point_id: str):
        try:
            logging.info(f"Sending HeartbeatInterval change to {charge_point_id}")
            response = await self.send_request(
                charge_point_id=charge_point_id,
                request_method='change_configuration',
                key='HeartbeatInterval',
                value='60'
            )

            if response.status == 'Accepted':
                logging.info(f"Successfully set HeartbeatInterval to 10 seconds for charger {charge_point_id}")
            else:
                logging.error(f"Failed to set HeartbeatInterval for charger {charge_point_id}: {response}")

        except Exception as e:
            logging.error(f"Exception while sending HeartbeatInterval change: {e}")

    async def send_sampled_metervalues_interval_change(self, charge_point_id: str):
        try:
            logging.info(f"Sending SampledMeterValueInterval change to {charge_point_id}")
            response = await self.send_request(
                charge_point_id=charge_point_id,
                request_method='change_configuration',
                key='MeterValueSampleInterval',
                value='90'
            )

            if response.status == 'Accepted':
                logging.info(f"Successfully set MeterValueSampleInterval to 10 seconds for charger {charge_point_id}")
            else:
                logging.error(f"Failed to set MeterValueSampleInterval for charger {charge_point_id}: {response}")

        except Exception as e:
            logging.error(f"Exception while sending MeterValueSampleInterval change: {e}")


    async def get_charger_data(self):
        """
        Fetch the entire charger data from Valkey cache or API if the cache is not available or expired.
        """
        # Try to get cached charger data from Valkey
        cached_data = valkey_client.get(CHARGER_DATA_KEY)
        
        if cached_data:
            logging.info("Using cached charger data from Valkey.")
            # Deserialize the JSON string back into a Python object (list)
            return json.loads(cached_data.decode('utf-8'))

        logging.info("Cache not found or expired, fetching fresh charger data from API.")
        # Fetch fresh charger data from API
        charger_data = await self.fetch_charger_data_from_api()

        # Serialize the charger data (list) into a JSON string before storing it in Valkey
        charger_data_json = json.dumps(charger_data)

        # Store the data in Valkey with expiration time of 2 hours (7200 seconds)
        valkey_client.setex(CHARGER_DATA_KEY, CACHE_EXPIRY, charger_data_json)

        return charger_data
    
    async def fetch_charger_data_from_api(self):
        """
        Make an API request to get the charger data from the source.
        """
        first_api_url = config("APICHARGERDATA")
        apiauthkey = config("APIAUTHKEY")
        timeout = 120
        
        response = requests.get(first_api_url, headers={"apiauthkey": apiauthkey}, timeout=timeout)
        
        if response.status_code != 200:
            logging.error("Error fetching charger data from API")
            raise HTTPException(status_code=500, detail="Error fetching charger data from API")
        
        charger_data = response.json().get("data", [])
        return charger_data
    
    
    async def verify_charger_id(self, charge_point_id: str) -> bool:
        """
        Verify if the charger ID exists in the system by checking cached data first, then the API if necessary.
        If verification fails 3 times, force an update of the cached data.
        """
        # Get the charger data (either from cache or API)
        charger_data = await self.get_charger_data()
        
        # Check if the charger exists in the cached data
        charger = next((item for item in charger_data if item["uid"] == charge_point_id), None)

        if not charger:
            # Track verification failures
            if charge_point_id not in self.verification_failures:
                self.verification_failures[charge_point_id] = 0
            self.verification_failures[charge_point_id] += 1
            
            logging.error(f"Charger with ID {charge_point_id} not found in the system. Verification failed.")

            # If verification fails 3 times, force cache update
            if self.verification_failures[charge_point_id] >= 3:
                logging.info(f"Verification failed 3 times for {charge_point_id}, forcing cache update.")
                charger_data = await self.fetch_charger_data_from_api()
                valkey_client.setex(CHARGER_DATA_KEY, CACHE_EXPIRY, json.dumps(charger_data))
                self.verification_failures[charge_point_id] = 0  # Reset the failure count

            return False

        return True

    async def send_request(self, charge_point_id, request_method, *args, **kwargs):
        charge_point = self.charge_points.get(charge_point_id)
        if not charge_point:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}

        method = getattr(charge_point, request_method, None)
        if method is None:
            logging.error(f"Request method {request_method} not found on ChargePoint {charge_point_id}.")
            return {"error": f"Request method {request_method} not found"}

        try:
            response = await method(*args, **kwargs)
            logging.info(f"Sent {request_method} to charge point {charge_point_id} with response: {response}")
            return response
        except Exception as e:
            logging.error(f"Error sending {request_method} to charge point {charge_point_id}: {e}")
            return {"error": str(e)}

    async def cancel_reservation(self, charge_point_id, reservation_id):
        try:
            reservation = Reservation.get(Reservation.reservation_id == reservation_id)
        except DoesNotExist:
            logging.info(f"No reservation found with ID {reservation_id}")
            return {"error": "Reservation not found"}

        response = await self.send_request(
            charge_point_id=charge_point_id,
            request_method='cancel_reservation',
            reservation_id=reservation_id
        )

        if response.status == 'Accepted':
            reservation.status = 'Cancelled'
            reservation.save()

            next_reservation = Reservation.select().where(
                Reservation.charger_id == charge_point_id,
                Reservation.status == 'Reserved'
            ).order_by(Reservation.expiry_date.asc()).first()

            if next_reservation:
                logging.info(f"Next reservation for charger {charge_point_id}: {next_reservation.reservation_id}")

            parse_and_store_cancel_reservation_response(charge_point_id, reservation_id=reservation_id, status='Cancelled')
        else:
            parse_and_store_cancel_reservation_response(charge_point_id, reservation_id=reservation_id, status='Failed')

        return response

# Instantiate the central system
central_system = CentralSystem()

# WebSocket endpoint that supports charger_id with slashes
# WebSocket route for connections with both charger_id and serialnumber
@app.websocket("/{charger_id}/{serialnumber}")
async def websocket_with_serialnumber(websocket: WebSocket, charger_id: str, serialnumber: str):
    logging.info(f"Charger {charger_id} with serial number {serialnumber} is connecting.")
    await central_system.handle_charge_point(websocket, charger_id)

# WebSocket route for connections with only charger_id
@app.websocket("/{charger_id}")
async def websocket_without_serialnumber(websocket: WebSocket, charger_id: str):
    logging.info(f"Charger {charger_id} is connecting without serial number.")
    await central_system.handle_charge_point(websocket, charger_id)

# WebSocket route for frontend connections
@app.websocket("/frontend/ws/{uid}")
async def frontend_websocket(websocket: WebSocket, uid: str):
    try:
        await central_system.handle_frontend_websocket(websocket, uid)
    except WebSocketDisconnect:
        logging.info(f"Frontend WebSocket for {uid} disconnected.")
    except Exception as e:
        logging.error(f"Error during WebSocket connection for {uid}: {e}")
        await websocket.close(code=1011, reason=f"Error: {e}")

# FastAPI request models
class ChangeAvailabilityRequest(BaseModel):
    uid: str
    connector_id: int
    type: str

class StartTransactionRequest(BaseModel):
    uid: str
    id_tag: str
    connector_id: int

class StopTransactionRequest(BaseModel):
    uid: str
    transaction_id: int

class ChangeConfigurationRequest(BaseModel):
    uid: str
    key: str
    value: str

class UnlockConnectorRequest(BaseModel):
    uid: str
    connector_id: int

class GetDiagnosticsRequest(BaseModel):
    uid: str
    location: str
    start_time: Optional[str]
    stop_time: Optional[str]
    retries: Optional[int]
    retry_interval: Optional[int]

class UpdateFirmwareRequest(BaseModel):
    uid: str
    location: str
    retrieve_date: str
    retries: Optional[int]
    retry_interval: Optional[int]

class ResetRequest(BaseModel):
    uid: str
    type: str

class TriggerMessageRequest(BaseModel):
    uid: str
    requested_message: str

class ReserveNowRequest(BaseModel):
    uid: str
    connector_id: int
    expiry_date: str
    id_tag: str
    reservation_id: int

class CancelReservationRequest(BaseModel):
    uid: str
    reservation_id: int

class GetConfigurationRequest(BaseModel):
    uid: str

class StatusRequest(BaseModel):
    uid: str

class ChargerToCMSQueryRequest(BaseModel):
    uid: Optional[str] = None  # Optional UID for filtering by charge_point_id
    filters: Optional[Dict[str, str]] = None  # Optional dictionary of column name and value pairs for filtering
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Optional start time for filtering
    end_time: Optional[datetime] = None  # Optional end time for filtering

class CMSToChargerQueryRequest(BaseModel):
    uid: Optional[str] = None  # Optional UID for filtering by charge_point_id
    filters: Optional[Dict[str, str]] = None  # Optional dictionary of column name and value pairs for filtering
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Optional start time for filtering
    end_time: Optional[datetime] = None  # Optional end time for filtering

class ChargerAnalyticsRequest(BaseModel):
    start_time: Optional[datetime] = None  # Optional start time for the analytics period
    end_time: Optional[datetime] = None  # Optional end time for the analytics period
    charger_id: Optional[str] = None  # Optional filter by charger_id
    include_charger_ids: Optional[bool] = False  # Whether to include the list of unique charger IDs

# REST API endpoints

# Handle OPTIONS for /api/change_availability
@app.options("/api/change_availability")
async def options_change_availability():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/change_availability")
async def change_availability(request: ChangeAvailabilityRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='change_availability',
        connector_id=request.connector_id,
        type=request.type
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/start_transaction
@app.options("/api/start_transaction")
async def options_start_transaction():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })
@app.post("/api/start_transaction")
async def start_transaction(request: StartTransactionRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='remote_start_transaction',
        id_tag=request.id_tag,
        connector_id=request.connector_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/stop_transaction
@app.options("/api/stop_transaction")
async def options_stop_transaction():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/stop_transaction")
async def stop_transaction(request: StopTransactionRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='remote_stop_transaction',
        transaction_id=request.transaction_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/change_configuration
@app.options("/api/change_configuration")
async def options_change_configuration():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/change_configuration")
async def change_configuration(request: ChangeConfigurationRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='change_configuration',
        key=request.key,
        value=request.value
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/clear_cache
@app.options("/api/clear_cache")
async def options_clear_cache():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })


@app.post("/api/clear_cache")
async def clear_cache(request: GetConfigurationRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='clear_cache'
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/unlock_connector
@app.options("/api/unlock_connector")
async def options_unlock_connector():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })
@app.post("/api/unlock_connector")
async def unlock_connector(request: UnlockConnectorRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='unlock_connector',
        connector_id=request.connector_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/get_diagnostics
@app.options("/api/get_diagnostics")
async def options_get_diagnostics():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/get_diagnostics")
async def get_diagnostics(request: GetDiagnosticsRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='get_diagnostics',
        location=request.location,
        start_time=request.start_time,
        stop_time=request.stop_time,
        retries=request.retries,
        retry_interval=request.retry_interval
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/update_firmware
@app.options("/api/update_firmware")
async def options_update_firmware():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/update_firmware")
async def update_firmware(request: UpdateFirmwareRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='update_firmware',
        location=request.location,
        retrieve_date=request.retrieve_date,
        retries=request.retries,
        retry_interval=request.retry_interval
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/reset
@app.options("/api/reset")
async def options_reset():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/reset")
async def reset(request: ResetRequest):

    # charger_serialnum = await central_system.getChargerSerialNum(request.uid)
    
    # Form the complete charge_point_id
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='reset',
        type=request.type
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/get_configuration
@app.options("/api/get_configuration")
async def options_get_configuration():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/get_configuration")
async def get_configuration(request: GetConfigurationRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='get_configuration'
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return response  # Return the configuration response as JSON

# Handle OPTIONS for /api/status
@app.options("/api/status")
async def options_status():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/status")
async def get_charge_point_status(request: StatusRequest):
    
    # Fetch the latest charger data from cache or API
    charger_data = await central_system.get_charger_data()
    
    # Create a set of active charger IDs from the charger data (API/cache)
    active_chargers = {f"{item['uid']}" for item in charger_data}

    # Handle specific chargers or all_online case
    if request.uid not in ["all", "all_online"]:
        # Form the complete charge_point_id
        charge_point_id = request.uid
        
        # Ensure the requested charger is still present in the API/cache
        if charge_point_id not in active_chargers:
            raise HTTPException(status_code=404, detail="Charger not found in the system")
    
    else:
        charge_point_id = request.uid

    # Handle "all_online" case
    if charge_point_id == "all_online":
        all_online_statuses = {}
        for cp_id, charge_point in central_system.charge_points.items():
            if charge_point.online and cp_id in active_chargers:  # Only show chargers still present in the API/cache
                online_status = "Online (with error)" if charge_point.has_error else "Online"
                connectors = charge_point.state["connectors"]
                connectors_status = {}

                for conn_id, conn_state in connectors.items():
                    next_reservation = Reservation.select().where(
                        Reservation.charger_id == cp_id,
                        Reservation.connector_id == conn_id,
                        Reservation.status == 'Reserved'
                    ).order_by(Reservation.expiry_date.asc()).dicts().first()

                    if next_reservation:
                        next_reservation_info = {
                            "reservation_id": next_reservation['reservation_id'],
                            "connector_id": next_reservation['connector_id'],
                            "from_time": next_reservation['from_time'],
                            "to_time": next_reservation['to_time']
                        }
                    else:
                        next_reservation_info = None

                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "latest_meter_value": conn_state.get("last_meter_value"),
                        "latest_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "latest_transaction_id": conn_state.get("transaction_id"),
                        "next_reservation": next_reservation_info
                    }

                all_online_statuses[cp_id] = {
                    "status": charge_point.state["status"],
                    "connectors": connectors_status,
                    "online": online_status,
                    "latest_message_received_time": charge_point.last_message_time.isoformat()
                }
        return all_online_statuses

    # Handle "all" case - return all chargers (online and offline) present in the API/cache
    if charge_point_id == "all":
        all_statuses = {}

        # Iterate through the charger data (from API/cache)
        for charger in charger_data:
            # Get the charger ID from the API/cache
            charger_id = charger['uid']  # Use the 'uid' field from the charger data to set charger_id
            charge_point = central_system.charge_points.get(charger_id)
            online_status = "Offline"  # Default to offline unless active

            connectors_status = {}

            # If the charge point is active, use its real-time status and data
            if charge_point:
                online_status = "Online (with error)" if charge_point.online and charge_point.has_error else "Online"
                connectors = charge_point.state["connectors"]

                for conn_id, conn_state in connectors.items():
                    next_reservation = Reservation.select().where(
                        Reservation.charger_id == charger_id,
                        Reservation.connector_id == conn_id,
                        Reservation.status == 'Reserved'
                    ).order_by(Reservation.expiry_date.asc()).dicts().first()

                    if next_reservation:
                        next_reservation_info = {
                            "reservation_id": next_reservation['reservation_id'],
                            "connector_id": next_reservation['connector_id'],
                            "from_time": next_reservation['from_time'],
                            "to_time": next_reservation['to_time']
                        }
                    else:
                        next_reservation_info = None

                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "last_meter_value": conn_state.get("last_meter_value"),
                        "last_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "transaction_id": conn_state.get("transaction_id"),
                        "next_reservation": next_reservation_info
                    }

            # If the charger is offline, use its last known data from charge_points
            elif charger_id in central_system.charge_points:
                charge_point = central_system.charge_points[charger_id]
                online_status = "Offline"
                connectors = charge_point.state["connectors"]

                for conn_id, conn_state in connectors.items():
                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "last_meter_value": conn_state.get("last_meter_value"),
                        "last_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "transaction_id": conn_state.get("transaction_id"),
                    }

            all_statuses[charger_id] = {
                "status": charge_point.state["status"] if charge_point else "Offline",
                "connectors": connectors_status,
                "online": online_status,
                "last_message_received_time": charge_point.last_message_time.isoformat() if charge_point else None
            }

        return all_statuses

    # Handle specific charge point status request (for specific chargers)
    if charge_point_id:
        charge_point = central_system.charge_points.get(charge_point_id)
        if not charge_point:
            raise HTTPException(status_code=404, detail="Charge point not found")

        online_status = "Online (with error)" if charge_point.online and charge_point.has_error else "Online" if charge_point.online else "Offline"
        connectors = charge_point.state["connectors"]
        connectors_status = {}

        for conn_id, conn_state in connectors.items():
            next_reservation = Reservation.select().where(
                Reservation.charger_id == charge_point_id,
                Reservation.connector_id == conn_id,
                Reservation.status == 'Reserved'
            ).order_by(Reservation.expiry_date.asc()).dicts().first()

            if next_reservation:
                next_reservation_info = {
                    "reservation_id": next_reservation['reservation_id'],
                    "connector_id": next_reservation['connector_id'],
                    "from_time": next_reservation['from_time'],
                    "to_time": next_reservation['to_time']
                }
            else:
                next_reservation_info = None

            connectors_status[conn_id] = {
                "status": conn_state["status"],
                "latest_meter_value": conn_state.get("last_meter_value"),
                "latest_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                "error_code": conn_state.get("error_code", "NoError"),
                "latest_transaction_id": conn_state.get("transaction_id"),
                "next_reservation": next_reservation_info
            }

        return {
            "charger_id": charge_point_id,
            "status": charge_point.state["status"],
            "connectors": connectors_status,
            "online": online_status,
            "latest_message_received_time": charge_point.last_message_time.isoformat()
        }


    # Handle specific charge point status request (this block will handle only specific charge points, not "all")
    if charge_point_id and charge_point_id != "all":
        charge_point = central_system.charge_points.get(charge_point_id)
        if not charge_point:
            raise HTTPException(status_code=404, detail="Charge point not found")

        online_status = "Online (with error)" if charge_point.online and charge_point.has_error else "Online" if charge_point.online else "Offline"
        connectors = charge_point.state["connectors"]
        connectors_status = {}

        for conn_id, conn_state in connectors.items():
            next_reservation = Reservation.select().where(
                Reservation.charger_id == charge_point_id,
                Reservation.connector_id == conn_id,
                Reservation.status == 'Reserved'
            ).order_by(Reservation.expiry_date.asc()).dicts().first()

            if next_reservation:
                next_reservation_info = {
                    "reservation_id": next_reservation['reservation_id'],
                    "connector_id": next_reservation['connector_id'],
                    "from_time": next_reservation['from_time'],
                    "to_time": next_reservation['to_time']
                }
            else:
                next_reservation_info = None

            connectors_status[conn_id] = {
                "status": conn_state["status"],
                "latest_meter_value": conn_state.get("last_meter_value"),
                "latest_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                "error_code": conn_state.get("error_code", "NoError"),
                "latest_transaction_id": conn_state.get("transaction_id"),
                "next_reservation": next_reservation_info
            }

        return {
            "charger_id": charge_point_id,
            "status": charge_point.state["status"],
            "connectors": connectors_status,
            "online": online_status,
            "latest_message_received_time": charge_point.last_message_time.isoformat()
        }

# Handle OPTIONS for /api/trigger_message
@app.options("/api/trigger_message")
async def options_trigger_message():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/trigger_message")
async def trigger_message(request: TriggerMessageRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='trigger_message',
        requested_message=request.requested_message
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/reserve_now
@app.options("/api/reserve_now")
async def options_reserve_now():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })


@app.post("/api/reserve_now")
async def reserve_now(request: ReserveNowRequest):

    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method='reserve_now',
        connector_id=request.connector_id,
        expiry_date=request.expiry_date,
        id_tag=request.id_tag,
        reservation_id=request.reservation_id
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/cancel_reservation
@app.options("/api/cancel_reservation")
async def options_cancel_reservation():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/cancel_reservation")
async def cancel_reservation(request: CancelReservationRequest):

    charge_point_id = request.uid

    response = await central_system.cancel_reservation(
        charge_point_id=charge_point_id,
        reservation_id=request.reservation_id
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

# Handle OPTIONS for /api/query_charger_to_cms
@app.options("/api/query_charger_to_cms")
async def options_query_charger_to_cms():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/query_charger_to_cms")
async def query_charger_to_cms(request: ChargerToCMSQueryRequest):
    # Initialize the base query
    base_query = "SELECT * FROM Charger_to_CMS WHERE 1=1"
    params = []

    # Handle the uid filter, if provided
    if request.uid:
        # Fetch the serial number using the uid
        # charger_serialnum = await central_system.getChargerSerialNum(request.uid)
        
        # Form the complete charge_point_id
        charge_point_id = request.uid
        
        # Apply the charge_point_id filter
        base_query += " AND charger_id = %s"
        params.append(charge_point_id)
    
    # Add other filtering conditions if any
    if request.filters:
        for column, value in request.filters.items():
            # Check if the column exists in the table
            column_check_query = """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'Charger_to_CMS' AND COLUMN_NAME = %s AND TABLE_SCHEMA = DATABASE();
            """
            column_exists = db.execute_sql(column_check_query, (column,)).fetchone()[0]

            if not column_exists:
                raise HTTPException(status_code=404, detail=f"Column '{column}' not found in Charger_to_CMS table.")

            # Add condition for the column
            base_query += f" AND {column} = %s"
            params.append(value)

    # Apply the timestamp filter if provided
    if request.start_time:
        base_query += " AND timestamp >= %s"
        params.append(request.start_time)
    if request.end_time:
        base_query += " AND timestamp <= %s"
        params.append(request.end_time)

    # Apply the limit if specified
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
            base_query += " ORDER BY id LIMIT %s, %s"
            params += [start - 1, end - start + 1]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        base_query += " ORDER BY id"

    # Execute the query
    cursor = db.execute_sql(base_query, params)
    rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]

    # Convert the result to a list of dictionaries
    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))

        # Deserialize the payload if it’s a JSON string
        if 'payload' in row_dict and row_dict['payload']:
            try:
                row_dict['payload'] = json.loads(row_dict['payload'])
            except json.JSONDecodeError:
                # Leave the payload as it is if it's not a valid JSON string
                pass

        results.append(row_dict)

    return {"data": results}

# Handle OPTIONS for /api/query_cms_to_charger
@app.options("/api/query_cms_to_charger")
async def options_query_cms_to_charger():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/query_cms_to_charger")
async def query_cms_to_charger(request: CMSToChargerQueryRequest):
    # Initialize the base query
    base_query = "SELECT * FROM CMS_to_Charger WHERE 1=1"
    params = []

    # Handle the uid filter, if provided
    if request.uid:
        # Fetch the serial number using the uid
        # charger_serialnum = await central_system.getChargerSerialNum(request.uid)
        
        # Form the complete charge_point_id
        charge_point_id = request.uid
        
        # Apply the charge_point_id filter
        base_query += " AND charger_id = %s"
        params.append(charge_point_id)
    
    # Add other filtering conditions if any
    if request.filters:
        for column, value in request.filters.items():
            # Check if the column exists in the table
            column_check_query = """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'CMS_to_Charger' AND COLUMN_NAME = %s AND TABLE_SCHEMA = DATABASE();
            """
            column_exists = db.execute_sql(column_check_query, (column,)).fetchone()[0]

            if not column_exists:
                raise HTTPException(status_code=404, detail=f"Column '{column}' not found in CMS_to_Charger table.")

            # Add condition for the column
            base_query += f" AND {column} = %s"
            params.append(value)

    # Apply the timestamp filter if provided
    if request.start_time:
        base_query += " AND timestamp >= %s"
        params.append(request.start_time)
    if request.end_time:
        base_query += " AND timestamp <= %s"
        params.append(request.end_time)

    # Apply the limit if specified
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
            base_query += " ORDER BY id LIMIT %s, %s"
            params += [start - 1, end - start + 1]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        base_query += " ORDER BY id"

    # Execute the query
    cursor = db.execute_sql(base_query, params)
    rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]

    # Convert the result to a list of dictionaries
    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))

        # Deserialize the payload if it’s a JSON string
        if 'payload' in row_dict and row_dict['payload']:
            try:
                row_dict['payload'] = json.loads(row_dict['payload'])
            except json.JSONDecodeError:
                # Leave the payload as it is if it's not a valid JSON string
                pass

        results.append(row_dict)

    return {"data": results}

# Handle OPTIONS for /api/query_transactions
@app.options("/api/query_transactions")
async def options_query_transactions():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/query_transactions")
async def query_transactions(request: ChargerToCMSQueryRequest):
    # Initialize the base query
    base_query = "SELECT * FROM transactions WHERE 1=1"
    params = []

    # Handle the uid filter, if provided
    if request.uid:
        # Form the complete charge_point_id
        charge_point_id = request.uid
        
        # Apply the charge_point_id filter
        base_query += " AND charger_id = %s"
        params.append(charge_point_id)
    
    # Add other filtering conditions if any
    if request.filters:
        for column, value in request.filters.items():
            # Check if the column exists in the table
            column_check_query = """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'transactions' AND COLUMN_NAME = %s AND TABLE_SCHEMA = DATABASE();
            """
            column_exists = db.execute_sql(column_check_query, (column,)).fetchone()[0]

            if not column_exists:
                raise HTTPException(status_code=404, detail=f"Column '{column}' not found in transactions table.")

            # Add condition for the column
            base_query += f" AND {column} = %s"
            params.append(value)

    # Apply the timestamp filter if provided
    if request.start_time:
        base_query += " AND start_time >= %s"
        params.append(request.start_time)
    if request.end_time:
        base_query += " AND stop_time <= %s"
        params.append(request.end_time)

    # Apply the limit if specified
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
            base_query += " ORDER BY id LIMIT %s, %s"
            params += [start - 1, end - start + 1]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        base_query += " ORDER BY id"

    # Execute the query
    cursor = db.execute_sql(base_query, params)
    rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]

    # Convert the result to a list of dictionaries
    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        results.append(row_dict)

    return {"data": results}

# Handle OPTIONS for /api/query_reservations
@app.options("/api/query_reservations")
async def options_query_reservations():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
    })

@app.post("/api/query_reservations")
async def query_reservations(request: ChargerToCMSQueryRequest):
    # Initialize the base query
    base_query = "SELECT * FROM reservations WHERE 1=1"
    params = []

    # Handle the uid filter, if provided
    if request.uid:
        # Fetch the serial number using the uid
        # charger_serialnum = await central_system.getChargerSerialNum(request.uid)
        
        # Form the complete charge_point_id
        charge_point_id = request.uid
        
        # Apply the charge_point_id filter
        base_query += " AND charger_id = %s"
        params.append(charge_point_id)
    
    # Add other filtering conditions if any
    if request.filters:
        for column, value in request.filters.items():
            # Check if the column exists in the table
            column_check_query = """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'reservations' AND COLUMN_NAME = %s AND TABLE_SCHEMA = DATABASE();
            """
            column_exists = db.execute_sql(column_check_query, (column,)).fetchone()[0]

            if not column_exists:
                raise HTTPException(status_code=404, detail=f"Column '{column}' not found in reservations table.")

            # Add condition for the column
            base_query += f" AND {column} = %s"
            params.append(value)

    # Apply the timestamp filter if provided
    if request.start_time:
        base_query += " AND reserved_at >= %s"
        params.append(request.start_time)
    if request.end_time:
        base_query += " AND reserved_at <= %s"
        params.append(request.end_time)

    # Apply the limit if specified
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
            base_query += " ORDER BY id LIMIT %s, %s"
            params += [start - 1, end - start + 1]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        base_query += " ORDER BY id"

    # Execute the query
    cursor = db.execute_sql(base_query, params)
    rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]

    # Convert the result to a list of dictionaries
    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        results.append(row_dict)

    return {"data": results}

def format_duration(seconds):
    """Convert seconds into a human-readable format: years, days, hours, minutes, seconds."""
    years, remainder = divmod(seconds, 31536000)  # 365 * 24 * 60 * 60
    days, remainder = divmod(remainder, 86400)    # 24 * 60 * 60
    hours, remainder = divmod(remainder, 3600)    # 60 * 60
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if years > 0:
        parts.append(f"{int(years)} years")
    if days > 0:
        parts.append(f"{int(days)} days")
    if hours > 0:
        parts.append(f"{int(hours)} hours")
    if minutes > 0:
        parts.append(f"{int(minutes)} minutes")
    if seconds > 0:
        parts.append(f"{int(seconds)} seconds")

    return ", ".join(parts) if parts else "0 seconds"

# Handle OPTIONS for /api/charger_analytics
@app.options("/api/charger_analytics")
async def options_charger_analytics():
    return JSONResponse(content={}, status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key",
    })

@app.post("/api/charger_analytics")
async def charger_analytics(request: ChargerAnalyticsRequest):

    # Initialize variables to store the results
    start_time = request.start_time or datetime.min.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = request.end_time or datetime.max.replace(hour=23, minute=59, second=59, microsecond=0)
    analytics = {}

    # Construct dynamic WHERE clauses based on provided filters
    where_clauses = ["timestamp BETWEEN %s AND %s"]
    params = [start_time, end_time]

    if request.charger_id:
        where_clauses.append("charger_id = %s")
        params.append(charger_id)

    where_clause = " AND ".join(where_clauses)

    # Step 1: Calculate the uptime for each charger
    uptime_query = f"""
    SELECT charger_id, timestamp
    FROM Charger_to_CMS
    WHERE {where_clause}
    ORDER BY charger_id, timestamp
    """
    
    # Execute the query with parameters
    cursor = db.execute_sql(uptime_query, params)
    uptime_results = cursor.fetchall()

    if not uptime_results:
        return {"error": "No data found for the specified time range and charger."}

    charger_uptime_data = {}
    current_charger = None
    last_timestamp = None

    for row in uptime_results:
        charger_id, timestamp = row

        if charger_id != current_charger:
            if current_charger:
                # Calculate uptime for the previous charger
                charger_uptime_data[current_charger]["total_uptime_seconds"] += (last_timestamp - first_message).total_seconds() + 30
            
            # Initialize new charger data
            current_charger = charger_id
            charger_uptime_data[current_charger] = {
                "total_uptime_seconds": 0,
                "total_possible_uptime_seconds": (end_time - start_time).total_seconds(),
                "charger_id": charger_id,
                "total_time_occupied_seconds": 0,  # Initialize to 0
                "session_durations": [],
                "peak_usage_times": [0] * 24  # 24-hour time slots
            }
            first_message = timestamp
        else:
            if (timestamp - last_timestamp).total_seconds() > 30:
                # The charger was offline for more than 30 seconds
                charger_uptime_data[current_charger]["total_uptime_seconds"] += (last_timestamp - first_message).total_seconds() + 30
                first_message = timestamp

        last_timestamp = timestamp

    # Finalize the last charger's uptime
    charger_uptime_data[current_charger]["total_uptime_seconds"] += (last_timestamp - first_message).total_seconds() + 30

    # Step 2: Calculate total number of transactions, electricity used, and session-related metrics
    for charger_id in charger_uptime_data.keys():
        transaction_query = f"""
        SELECT COUNT(*), SUM(total_consumption), 
               SUM(TIMESTAMPDIFF(SECOND, start_time, stop_time)) as total_time_occupied
        FROM transactions 
        WHERE start_time BETWEEN %s AND %s AND charger_id = %s
        """
        # Execute the query with parameters
        cursor = db.execute_sql(transaction_query, [start_time, end_time, charger_id])
        total_transactions, total_electricity_used, total_time_occupied = cursor.fetchone()

        total_electricity_used = total_electricity_used or 0.0
        total_time_occupied = total_time_occupied or 0  # Ensure total_time_occupied is not None

        # Calculate session durations and peak usage times
        session_query = f"""
        SELECT start_time, TIMESTAMPDIFF(SECOND, start_time, stop_time) as session_duration
        FROM transactions
        WHERE start_time BETWEEN %s AND %s AND charger_id = %s
        """
        # Execute the query with parameters
        cursor = db.execute_sql(session_query, [start_time, end_time, charger_id])
        sessions = cursor.fetchall()

        for start_time, session_duration in sessions:
            charger_uptime_data[charger_id]["session_durations"].append(session_duration)
            hour_of_day = start_time.hour
            charger_uptime_data[charger_id]["peak_usage_times"][hour_of_day] += 1

        # Calculate peak usage hours
        max_usage_count = max(charger_uptime_data[charger_id]["peak_usage_times"])
        peak_usage_hours = [f"{hour}:00 - {hour + 1}:00" for hour, count in enumerate(charger_uptime_data[charger_id]["peak_usage_times"]) if count == max_usage_count]

        if max_usage_count == 0:
            peak_usage_hours = ["No peak usage times - charger was not used during this period."]

        # Compile the results
        uptime_seconds = charger_uptime_data[charger_id]["total_uptime_seconds"]
        uptime_percentage = round((uptime_seconds / charger_uptime_data[charger_id]["total_possible_uptime_seconds"]) * 100, 3)
        average_session_duration_seconds = sum(charger_uptime_data[charger_id]["session_durations"]) / total_transactions if total_transactions > 0 else 0
        occupancy_rate = round((total_time_occupied / charger_uptime_data[charger_id]["total_possible_uptime_seconds"]) * 100, 3)

        analytics_data = {
            "charger_id": charger_id,
            "timestamp": datetime.now(),
            "total_uptime": format_duration(uptime_seconds),
            "uptime_percentage": uptime_percentage,
            "total_transactions": total_transactions,
            "total_electricity_used_kwh": total_electricity_used,
            "occupancy_rate_percentage": occupancy_rate,
            "average_session_duration": format_duration(average_session_duration_seconds),
            "peak_usage_times": ", ".join(peak_usage_hours)  # Store as a comma-separated string
        }

        # Save analytics data to the database
        Analytics.create(**analytics_data)

        analytics[charger_id] = analytics_data

    # If a specific charger ID is requested, return data only for that charger
    if request.charger_id:
        return analytics.get(charger_id, {"error": "Charger ID not found."})
    
    return {"analytics": analytics}

if __name__ == "__main__":
    port=int(config("F_SERVER_PORT"))
    uvicorn.run(app, host=config("F_SERVER_HOST"), port=port)