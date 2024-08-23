import asyncio
import logging
from datetime import datetime, timezone as dt_timezone
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Header, Depends, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, Dict
from peewee import DoesNotExist
from io import BytesIO
import qrcode
import requests
import json
from decouple import config
import os
from OCPP_Requests import ChargePoint  # Assuming this is where ChargePoint is implemented
from models import Reservation, QRCodeData, db, Analytics
from Chargers_to_CMS_Parser import parse_and_store_cancel_reservation_response  # Assuming this handles responses


logging.basicConfig(level=logging.DEBUG)

API_KEY_NAME = "x-api-key"
        
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def verify_api_key_middleware(request: Request, call_next):
    # Check if the request path starts with "/api/"
    if request.url.path.startswith("/api/"):
        api_key = request.headers.get("x-api-key")
        expected_api_key = config("API_KEY")
        if api_key != expected_api_key:
            raise HTTPException(status_code=403, detail="Invalid API key")
    response = await call_next(request)
    return response

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
        self.offline_threshold = 30  # Time in seconds without messages to consider a charger offline
        self.active_connections = {}

    async def handle_charge_point(self, websocket: WebSocket, charge_point_id: str):
        await websocket.accept()

        if charge_point_id in self.active_connections:
            logging.info(f"Charge point {charge_point_id} already connected, closing new connection.")
            await websocket.close(code=1000)
            return

        if not await self.verify_charger_id(charge_point_id):
            await websocket.close(code=1000)
            return

        logging.info(f"Charge point {charge_point_id} connected.")
        self.active_connections[charge_point_id] = websocket

        ws_adapter = WebSocketAdapter(websocket)
        charge_point = ChargePoint(charge_point_id, ws_adapter)
        self.charge_points[charge_point_id] = charge_point

        try:
            # Start the charge point in a background task
            start_task = asyncio.create_task(charge_point.start())

            # Wait for a brief moment to ensure the start method has begun
            await asyncio.sleep(5)  # Adjust the sleep time as necessary

            # Now send the ChangeConfiguration request
            await self.send_heartbeat_interval_change(charge_point_id)

            # Await the completion of the start method
            await start_task

        except WebSocketDisconnect:
            logging.info(f"Charge point {charge_point_id} disconnected.")
            self.charge_points.pop(charge_point_id, None)
            self.active_connections.pop(charge_point_id, None)

    async def send_heartbeat_interval_change(self, charge_point_id: str):
        try:
            logging.info(f"Sending HeartbeatInterval change to {charge_point_id}")
            response = await self.send_request(
                charge_point_id=charge_point_id,
                request_method='change_configuration',
                key='HeartbeatInterval',
                value='10'
            )

            if response.status == 'Accepted':
                logging.info(f"Successfully set HeartbeatInterval to 10 seconds for charger {charge_point_id}")
            else:
                logging.error(f"Failed to set HeartbeatInterval for charger {charge_point_id}: {response}")

        except Exception as e:
            logging.error(f"Exception while sending HeartbeatInterval change: {e}")

    async def verify_charger_id(self, charge_point_id: str) -> bool:
        uid, charger_serialnum = charge_point_id.split("/")
        first_api_url = config("APICHARGERDATA")
        apiauthkey = config("APIAUTHKEY")
        timeout=120
        response = requests.get(first_api_url, headers={"apiauthkey": apiauthkey}, timeout=timeout)
        
        if response.status_code != 200:
            logging.error("Error fetching charger data from API")
            return False
        
        charger_data = response.json().get("data", [])
        charger = next((item for item in charger_data if item["uid"] == uid and item["Chargerserialnum"] == charger_serialnum), None)
        
        if not charger:
            logging.error(f"Charger with ID {charge_point_id} not found in the system.")
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

    async def check_offline_chargers(self):
        now = datetime.now(dt_timezone.utc)
        offline_chargers = [cp_id for cp_id, cp in self.charge_points.items()
                            if (now - cp.last_message_time).total_seconds() > self.offline_threshold]
        for charge_point_id in offline_chargers:
            self.charge_points[charge_point_id].online = False
            logging.info(f"Charge point {charge_point_id} marked as offline due to inactivity.")

# Instantiate the central system
central_system = CentralSystem()

# WebSocket endpoint that supports charger_id with slashes
@app.websocket("/{charger_id:path}")
async def websocket_endpoint(websocket: WebSocket, charger_id: str):
    await central_system.handle_charge_point(websocket, charger_id)

# FastAPI request models
class ChangeAvailabilityRequest(BaseModel):
    charge_point_id: str
    connector_id: int
    type: str

class StartTransactionRequest(BaseModel):
    charge_point_id: str
    id_tag: str
    connector_id: int

class StopTransactionRequest(BaseModel):
    charge_point_id: str
    transaction_id: int

class ChangeConfigurationRequest(BaseModel):
    charge_point_id: str
    key: str
    value: str

class UnlockConnectorRequest(BaseModel):
    charge_point_id: str
    connector_id: int

class GetDiagnosticsRequest(BaseModel):
    charge_point_id: str
    location: str
    start_time: Optional[str]
    stop_time: Optional[str]
    retries: Optional[int]
    retry_interval: Optional[int]

class UpdateFirmwareRequest(BaseModel):
    charge_point_id: str
    location: str
    retrieve_date: str
    retries: Optional[int]
    retry_interval: Optional[int]

class ResetRequest(BaseModel):
    charge_point_id: str
    type: str

class TriggerMessageRequest(BaseModel):
    charge_point_id: str
    requested_message: str

class ReserveNowRequest(BaseModel):
    charge_point_id: str
    connector_id: int
    expiry_date: str
    id_tag: str
    reservation_id: int

class CancelReservationRequest(BaseModel):
    charge_point_id: str
    reservation_id: int

class GetConfigurationRequest(BaseModel):
    charge_point_id: str

class StatusRequest(BaseModel):
    charge_point_id: str

class MakeQRCodes(BaseModel):
    charge_point_id:str
    Delete: Optional[bool] = None

class ChargerToCMSQueryRequest(BaseModel):
    filters: Dict[str, str]  # Dictionary of column name and value pairs for filtering
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Start time for filtering
    end_time: Optional[datetime] = None  # End time for filtering

class CMSToChargerQueryRequest(BaseModel):
    filters: Dict[str, str]  # Dictionary of column name and value pairs for filtering
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Start time for filtering
    end_time: Optional[datetime] = None  # End time for filtering

class ChargerAnalyticsRequest(BaseModel):
    start_time: Optional[datetime] = None  # Optional start time for the analytics period
    end_time: Optional[datetime] = None  # Optional end time for the analytics period
    charger_id: Optional[str] = None  # Optional filter by charger_id
    include_charger_ids: Optional[bool] = False  # Whether to include the list of unique charger IDs

# REST API endpoints

@app.post("/api/change_availability")
async def change_availability(request: ChangeAvailabilityRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='change_availability',
        connector_id=request.connector_id,
        type=request.type
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/start_transaction")
async def start_transaction(request: StartTransactionRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='remote_start_transaction',
        id_tag=request.id_tag,
        connector_id=request.connector_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/stop_transaction")
async def stop_transaction(request: StopTransactionRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='remote_stop_transaction',
        transaction_id=request.transaction_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/change_configuration")
async def change_configuration(request: ChangeConfigurationRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='change_configuration',
        key=request.key,
        value=request.value
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/clear_cache")
async def clear_cache(request: GetConfigurationRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='clear_cache'
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/unlock_connector")
async def unlock_connector(request: UnlockConnectorRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='unlock_connector',
        connector_id=request.connector_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/get_diagnostics")
async def get_diagnostics(request: GetDiagnosticsRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
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

@app.post("/api/update_firmware")
async def update_firmware(request: UpdateFirmwareRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='update_firmware',
        location=request.location,
        retrieve_date=request.retrieve_date,
        retries=request.retries,
        retry_interval=request.retry_interval
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/reset")
async def reset(request: ResetRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='reset',
        type=request.type
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/get_configuration")
async def get_configuration(request: GetConfigurationRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='get_configuration'
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return response  # Return the configuration response as JSON

@app.post("/api/status")
async def get_charge_point_status(request: StatusRequest):
    charge_point_id = request.charge_point_id
    
    if charge_point_id == "all_online":
        all_online_statuses = {}
        for cp_id, charge_point in central_system.charge_points.items():
            if charge_point.online:
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
    else:
        all_statuses = {}
        for cp_id, charge_point in central_system.charge_points.items():
            online_status = "Online (with error)" if charge_point.online and charge_point.has_error else "Online" if charge_point.online else "Offline"
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
                    "last_meter_value": conn_state.get("last_meter_value"),
                    "last_transaction_consumption_kwh": conn_state.get("last_transaction_consumption_kwh", 0),
                    "error_code": conn_state.get("error_code", "NoError"),
                    "transaction_id": conn_state.get("transaction_id"),
                    "next_reservation": next_reservation_info
                }

            all_statuses[cp_id] = {
                "status": charge_point.state["status"],
                "connectors": connectors_status,
                "online": online_status,
                "last_message_received_time": charge_point.last_message_time.isoformat()
            }
        return all_statuses

@app.post("/api/trigger_message")
async def trigger_message(request: TriggerMessageRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='trigger_message',
        requested_message=request.requested_message
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/reserve_now")
async def reserve_now(request: ReserveNowRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='reserve_now',
        connector_id=request.connector_id,
        expiry_date=request.expiry_date,
        id_tag=request.id_tag,
        reservation_id=request.reservation_id
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/cancel_reservation")
async def cancel_reservation(request: CancelReservationRequest):
    response = await central_system.cancel_reservation(
        charge_point_id=request.charge_point_id,
        reservation_id=request.reservation_id
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/api/make_qr_code/")
async def make_qr_code(request: MakeQRCodes):
    # Load the API key from the .env file
    apiauthkey = config("APIAUTHKEY")

    # Extract uid and ChargerSerialNum from charge_point_id
    uid, charger_serialnum = request.charge_point_id.split("/")
    
    # Get the QR code directory path from the environment variable
    qr_path = config("QRPATH")
    if not os.path.exists(qr_path):
        os.makedirs(qr_path)

    try:
        # Check if the QR code already exists in the database for both charger_id and charger_serial_number
        qr_data = QRCodeData.get(
            (QRCodeData.charger_id == uid) & 
            (QRCodeData.charger_serial_number == charger_serialnum)
        )
        
        if request.Delete:
            # If Delete is True, delete the existing QR code file and its record
            if os.path.exists(qr_data.image_path):
                os.remove(qr_data.image_path)
            qr_data.delete_instance()
            return {"message": f"QR code for charger {uid} and serial number {charger_serialnum} has been deleted."}
        else:
            # If Delete is False, return the existing QR code from the file system
            img_io = BytesIO()
            with open(qr_data.image_path, 'rb') as f:
                img_io.write(f.read())
            img_io.seek(0)  # Ensure we're reading from the start
            return StreamingResponse(img_io, media_type="image/png")
    
    except QRCodeData.DoesNotExist:
        # If the QR code doesn't exist, and Delete is True, return a 404 error
        if request.Delete:
            raise HTTPException(status_code=404, detail="QR code not found, nothing to delete.")

    # Proceed to generate a new QR code if it doesn't exist or if Delete is False
    timeout = 120

    # Step 1: Fetch data from the first API (if necessary for validation)
    first_api_url = config("APICHARGERDATA")
    response1 = requests.get(first_api_url, headers={"apiauthkey": apiauthkey}, timeout=timeout)
    
    if response1.status_code != 200:
        raise HTTPException(status_code=500, detail="Error fetching data from the first API")
    
    charger_data = response1.json().get("data", [])
    
    # Validate the charger exists
    charger = next((item for item in charger_data if item["uid"] == uid and item["Chargerserialnum"] == charger_serialnum), None)
    
    if not charger:
        raise HTTPException(status_code=404, detail="Charger not found")

    # Generate the QR code containing only the uid
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=5,
        border=2,
    )
    qr.add_data(uid)  # Only add the UID to the QR code
    qr.make(fit=True)

    img = qr.make_image(fill='black', back_color='white')

    # Prepare the image for saving
    current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    image_filename = f"{uid}_{current_time}.png"  # Ensuring the file is saved as a PNG
    image_path = os.path.join(qr_path, image_filename)
    
    # Save the image to the specified path in PNG format
    img.save(image_path)

    # Store the path to the image and the filename in the database
    qr_data = QRCodeData.create(
        charger_id=uid,
        charger_serial_number=charger_serialnum,
        image_path=image_path,  # Store the path to the image
        filename=image_filename,  # Store the filename of the image
        generation_date=datetime.now()
    )

    # Return the QR code image as a response
    img_io = BytesIO()
    with open(image_path, 'rb') as f:
        img_io.write(f.read())
    img_io.seek(0)
    return StreamingResponse(img_io, media_type="image/png")

@app.post("/api/query_charger_to_cms")
async def query_charger_to_cms(request: ChargerToCMSQueryRequest):
    # Validate the limit parameter format if provided
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        start, end = 1, 100  # Default limit if not provided

    # Ensure at least one filter is provided
    if not request.filters:
        raise HTTPException(status_code=400, detail="At least one column filter must be provided.")

    # Dynamically build the SQL query
    base_query = "SELECT * FROM Charger_to_CMS WHERE 1=1"
    params = []

    # Add filtering conditions
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
    base_query_with_limit = base_query + " ORDER BY id LIMIT %s, %s"
    params_with_limit = params + [start - 1, end - start + 1]

    # Execute the query with limit
    cursor = db.execute_sql(base_query_with_limit, params_with_limit)
    rows = cursor.fetchall()

    # If no results found within the limit, remove the limit and fetch all matching rows
    if not rows:
        cursor = db.execute_sql(base_query + " ORDER BY id", params)
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

@app.post("/api/query_cms_to_charger")
async def query_cms_to_charger(request: CMSToChargerQueryRequest):
    # Validate the limit parameter format if provided
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        start, end = 1, 100  # Default limit if not provided

    # Ensure at least one filter is provided
    if not request.filters:
        raise HTTPException(status_code=400, detail="At least one column filter must be provided.")

    # Dynamically build the SQL query
    base_query = "SELECT * FROM CMS_to_Charger WHERE 1=1"
    params = []

    # Add filtering conditions
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
    base_query_with_limit = base_query + " ORDER BY id LIMIT %s, %s"
    params_with_limit = params + [start - 1, end - start + 1]

    # Execute the query with limit
    cursor = db.execute_sql(base_query_with_limit, params_with_limit)
    rows = cursor.fetchall()

    # If no results found within the limit, remove the limit and fetch all matching rows
    if not rows:
        cursor = db.execute_sql(base_query + " ORDER BY id", params)
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

@app.post("/api/query_transactions")
async def query_transactions(request: ChargerToCMSQueryRequest):
    # Validate the limit parameter format if provided
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        start, end = 1, 100  # Default limit if not provided

    # Ensure at least one filter is provided
    if not request.filters:
        raise HTTPException(status_code=400, detail="At least one column filter must be provided.")

    # Dynamically build the SQL query
    base_query = "SELECT * FROM transactions WHERE 1=1"
    params = []

    # Add filtering conditions
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
    base_query_with_limit = base_query + " ORDER BY id LIMIT %s, %s"
    params_with_limit = params + [start - 1, end - start + 1]

    # Execute the query with limit
    cursor = db.execute_sql(base_query_with_limit, params_with_limit)
    rows = cursor.fetchall()

    # If no results found within the limit, remove the limit and fetch all matching rows
    if not rows:
        cursor = db.execute_sql(base_query + " ORDER BY id", params)
        rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]

    # Convert the result to a list of dictionaries
    results = [dict(zip(columns, row)) for row in rows]

    return {"data": results}

@app.post("/api/query_reservations")
async def query_reservations(request: ChargerToCMSQueryRequest):
    # Validate the limit parameter format if provided
    if request.limit:
        try:
            start, end = map(int, request.limit.split('-'))
            if start > end or start < 1 or end < 1:
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid limit format. Use 'start-end' format, e.g., '1-100'.")
    else:
        start, end = 1, 100  # Default limit if not provided

    # Ensure at least one filter is provided
    if not request.filters:
        raise HTTPException(status_code=400, detail="At least one column filter must be provided.")

    # Dynamically build the SQL query
    base_query = "SELECT * FROM reservations WHERE 1=1"
    params = []

    # Add filtering conditions
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
    base_query_with_limit = base_query + " ORDER BY id LIMIT %s, %s"
    params_with_limit = params + [start - 1, end - start + 1]

    # Execute the query with limit
    cursor = db.execute_sql(base_query_with_limit, params_with_limit)
    rows = cursor.fetchall()

    # If no results found within the limit, remove the limit and fetch all matching rows
    if not rows:
        cursor = db.execute_sql(base_query + " ORDER BY id", params)
        rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]

    # Convert the result to a list of dictionaries
    results = [dict(zip(columns, row)) for row in rows]

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
        params.append(request.charger_id)

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
        return analytics.get(request.charger_id, {"error": "Charger ID not found."})
    
    return {"analytics": analytics}


if __name__ == "__main__":
    import uvicorn
    port=int(config("F_SERVER_PORT"))
    uvicorn.run(app, host=config("F_SERVER_HOST"), port=port)