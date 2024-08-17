import asyncio
import logging
from datetime import datetime, timezone as dt_timezone
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional, Dict
from peewee import DoesNotExist
from OCPP_Requests import ChargePoint  # Assuming this is where ChargePoint is implemented
from models import Reservation
from Chargers_to_CMS_Parser import parse_and_store_cancel_reservation_response  # Assuming this handles responses
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.DEBUG)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

    async def handle_charge_point(self, websocket: WebSocket, charge_point_id: str):
        await websocket.accept()
        logging.info(f"Charge point {charge_point_id} connected.")
        
        # Wrap FastAPI WebSocket in an adapter
        ws_adapter = WebSocketAdapter(websocket)
        charge_point = ChargePoint(charge_point_id, ws_adapter)
        self.charge_points[charge_point_id] = charge_point

        try:
            await charge_point.start()  # This now uses the wrapped WebSocket object
        except WebSocketDisconnect:
            logging.info(f"Charge point {charge_point_id} disconnected.")
            self.charge_points.pop(charge_point_id, None)

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
@app.websocket("/ws/{charger_id:path}")
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

class GetMeterValuesRequest(BaseModel):
    charge_point_id: str
    connector_id: int

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

# REST API endpoints

@app.post("/change_availability")
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

@app.post("/start_transaction")
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

@app.post("/stop_transaction")
async def stop_transaction(request: StopTransactionRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='remote_stop_transaction',
        transaction_id=request.transaction_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/change_configuration")
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

@app.post("/clear_cache")
async def clear_cache(request: GetConfigurationRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='clear_cache'
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/unlock_connector")
async def unlock_connector(request: UnlockConnectorRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='unlock_connector',
        connector_id=request.connector_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/get_diagnostics")
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

@app.post("/update_firmware")
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

@app.post("/reset")
async def reset(request: ResetRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='reset',
        type=request.type
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/get_meter_values")
async def get_meter_values(request: GetMeterValuesRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='get_meter_values',
        connector_id=request.connector_id
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/get_configuration")
async def get_configuration(request: GetConfigurationRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='get_configuration'
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return response  # Return the configuration response as JSON

@app.post("/status")
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

@app.post("/trigger_message")
async def trigger_message(request: TriggerMessageRequest):
    response = await central_system.send_request(
        charge_point_id=request.charge_point_id,
        request_method='trigger_message',
        requested_message=request.requested_message
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

@app.post("/reserve_now")
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

@app.post("/cancel_reservation")
async def cancel_reservation(request: CancelReservationRequest):
    response = await central_system.cancel_reservation(
        charge_point_id=request.charge_point_id,
        reservation_id=request.reservation_id
    )
    if isinstance((response, dict) and "error" in response):
        raise HTTPException(status_code=404, detail=response["error"])
    return {"status": response.status}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
