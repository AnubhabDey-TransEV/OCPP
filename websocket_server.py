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
        charge_point = self.charge_points.get(charge_point_id)
        if not charge_point:
            logging.error(f"Charge point {charge_point_id} not found.")
            return None
        
        method = getattr(charge_point, request_method, None)
        if method is None:
            logging.error(f"Request method {request_method} not found on ChargePoint {charge_point_id}.")
            return None
        
        try:
            response = await method(*args, **kwargs)
            logging.info(f"Sent {request_method} to charge point {charge_point_id} with response: {response}")
            return response
        except Exception as e:
            logging.error(f"Error sending {request_method} to charge point {charge_point_id}: {e}")
            return None

    async def server(self):
        logging.info("WebSocket server started.")
        # Bind to all network interfaces (0.0.0.0) and listen on port 5000
        async with websockets.serve(self.handle_charge_point, "0.0.0.0", 5000):
            await asyncio.Future()  # Run forever

async def test_send_requests(central_system):
    # Wait for a charge point to connect (adjust timing or handle events as needed)
    await asyncio.sleep(10)  # Adjust as necessary

    # # Example of sending a Reset request
    # response = await central_system.send_request(
    #     charge_point_id='vctjtc/240100327',  # Replace with the actual charge point ID
    #     request_method='reset',  # Use the correct method name from ChargePoint class
    #     type='Hard'  # Specify the type of reset
    # )
    # print(f"Response: {response}")

    # Example of sending a RemoteStartTransaction request
    # start_response = await central_system.send_request(
    #     charge_point_id='vctjtc/240100327',  # Replace with the actual charge point ID
    #     request_method='remote_start_transaction',  # Use the correct method name from ChargePoint class
    #     id_tag='ABC123',  # Specify the ID tag
    #     connector_id=1  # Specify the connector ID
    # )
    # print(f"RemoteStartTransaction Response: {start_response}")

    # # Check if start_response is None before trying to access transaction_id
    # if start_response and hasattr(start_response, 'transaction_id'):
    #     transaction_id = start_response.transaction_id

    #     # Example of sending a RemoteStopTransaction request
    #     stop_response = await central_system.send_request(
    #         charge_point_id='vctjtc/240100327',  # Replace with the actual charge point ID
    #         request_method='remote_stop_transaction',  # Use the correct method name from ChargePoint class
    #         transaction_id=transaction_id  # Use the transaction ID from the start transaction response
    #     )
    #     print(f"RemoteStopTransaction Response: {stop_response}")
    # else:
    #     logging.error("Failed to start transaction or transaction_id not found.")

if __name__ == "__main__":
    central_system = CentralSystem()

    # Create a main coroutine to run both the server and test requests
    async def main():
        # Run the WebSocket server
        server_task = asyncio.create_task(central_system.server())

        # Run the test requests
        await test_send_requests(central_system)

        # Ensure the server task continues running
        await server_task

    asyncio.run(main())
