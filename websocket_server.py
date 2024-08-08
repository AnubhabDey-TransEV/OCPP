import asyncio
import websockets
import logging
from datetime import datetime, timezone as dt_timezone
from OCPP_Requests import ChargePoint

logging.basicConfig(level=logging.DEBUG)

class CustomWebSocketServerProtocol(websockets.WebSocketServerProtocol):
    def __init__(self, central_system, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.central_system = central_system

    async def pong(self, data: bytes) -> None:
        charge_point_id = self.path.strip("/")
        charge_point = self.central_system.charge_points.get(charge_point_id)
        if charge_point:
            charge_point.mark_as_online()
        await super().pong(data)

class CentralSystem:
    def __init__(self):
        self.charge_points = {}
        self.offline_check_interval = 10  # Interval to check for offline chargers in seconds
        self.offline_threshold = 30  # Time in seconds without messages to consider a charger offline

    async def handle_charge_point(self, websocket, path):
        charge_point_id = path.strip("/")
        logging.info(f"Charge point {charge_point_id} attempting to connect.")
        try:
            charge_point = ChargePoint(charge_point_id, websocket)
            self.charge_points[charge_point_id] = charge_point
            logging.info(f"Charge point {charge_point_id} connected.")
            await charge_point.start()
        except Exception as e:
            logging.error(f"Error handling charge point {charge_point_id}: {e}")
        finally:
            # When the connection is closed, mark the charge point as offline
            logging.info(f"Charge point {charge_point_id} disconnected.")
            if charge_point_id in self.charge_points:
                self.charge_points[charge_point_id].online = False

    async def send_request(self, charge_point_id, request_method, *args, **kwargs):
        logging.debug(f"Attempting to send request {request_method} to charge point {charge_point_id}")
        charge_point = self.charge_points.get(charge_point_id)
        if not charge_point:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        
        method = getattr(charge_point, request_method, None)
        if method is None:
            logging.error(f"Request method {request_method} not found on ChargePoint {charge_point_id}.")
            return {"error": f"Request method {request_method} not found"}

        logging.debug(f"Arguments for {request_method}: args = {args}, kwargs = {kwargs}")
        for arg in args:
            logging.debug(f"Type of arg: {type(arg)}, value: {arg}")
        for key, value in kwargs.items():
            logging.debug(f"Type of kwarg {key}: {type(value)}, value: {value}")

        try:
            response = await method(*args, **kwargs)
            logging.info(f"Sent {request_method} to charge point {charge_point_id} with response: {response}")
            return response
        except Exception as e:
            logging.error(f"Error sending {request_method} to charge point {charge_point_id}: {e}")
            return {"error": str(e)}

    async def check_offline_chargers(self):
        while True:
            await asyncio.sleep(self.offline_check_interval)
            now = datetime.now(dt_timezone.utc)
            offline_chargers = [cp_id for cp_id, cp in self.charge_points.items()
                                if (now - cp.last_message_time).total_seconds() > self.offline_threshold]
            for charge_point_id in offline_chargers:
                self.charge_points[charge_point_id].online = False
                logging.info(f"Charge point {charge_point_id} marked as offline due to inactivity.")

    async def server(self):
        logging.info("WebSocket server started.")
        await asyncio.gather(
            websockets.serve(
                self.handle_charge_point,
                "0.0.0.0",
                5000,
                create_protocol=lambda *args, **kwargs: CustomWebSocketServerProtocol(self, *args, **kwargs)
            ),
            self.check_offline_chargers()
        )

if __name__ == "__main__":
    central_system = CentralSystem()

    # Create a main coroutine to run the server
    async def main():
        await central_system.server()

    asyncio.run(main())
