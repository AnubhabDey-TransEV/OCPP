import asyncio
import websockets
import logging
from OCPP_Requests import ChargePoint

logging.basicConfig(level=logging.DEBUG)

class CentralSystem:
    def __init__(self):
        self.charge_points = {}

    async def handle_charge_point(self, websocket, path):
        charge_point_id = path.strip("/")
        charge_point = ChargePoint(charge_point_id, websocket)
        self.charge_points[charge_point_id] = charge_point
        logging.info(f"Charge point {charge_point_id} connected.")
        await charge_point.start()

    async def server(self):
        logging.info("WebSocket server started.")
        async with websockets.serve(self.handle_charge_point, "localhost", 5000):
            await asyncio.Future()  # Run forever

if __name__ == "__main__":
    central_system = CentralSystem()
    asyncio.run(central_system.server())
