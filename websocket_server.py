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
        logging.info(f"Charge point {charge_point_id} attempting to connect.")
        try:
            charge_point = ChargePoint(charge_point_id, websocket)
            self.charge_points[charge_point_id] = charge_point
            logging.info(f"Charge point {charge_point_id} connected.")
            await charge_point.start()
        except Exception as e:
            logging.error(f"Error handling charge point {charge_point_id}: {e}")

    async def send_request(self, charge_point_id, request_method, *args, **kwargs):
        """
        Send a request to a specific charge point.
        
        :param charge_point_id: ID of the charge point
        :param request_method: Method to call on the ChargePoint instance
        :param args: Positional arguments for the request
        :param kwargs: Keyword arguments for the request
        :return: Response from the charge point
        """
        logging.debug(f"Attempting to send request {request_method} to charge point {charge_point_id}")
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

    async def server(self):
        logging.info("WebSocket server started.")
        async with websockets.serve(self.handle_charge_point, "0.0.0.0", 5000):
            await asyncio.Future()  # Run forever

if __name__ == "__main__":
    central_system = CentralSystem()

    # Create a main coroutine to run the server
    async def main():
        await central_system.server()

    asyncio.run(main())
