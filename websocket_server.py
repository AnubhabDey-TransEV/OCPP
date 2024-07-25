import asyncio
import websockets
import logging
from OCPP_Requests import ChargePoint

logging.basicConfig(level=logging.DEBUG)

class CentralSystem:
    def __init__(self):
        self.charge_points = {}

    async def start_transaction(self, charge_point_id, id_tag, connector_id):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].remote_start_transaction(id_tag, connector_id)

    async def stop_transaction(self, charge_point_id, transaction_id):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].remote_stop_transaction(transaction_id)

    async def change_availability(self, charge_point_id, connector_id, type):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].change_availability(connector_id, type)

    async def change_configuration(self, charge_point_id, key, value):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].change_configuration(key, value)

    async def clear_cache(self, charge_point_id):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].clear_cache()

    async def unlock_connector(self, charge_point_id, connector_id):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].unlock_connector(connector_id)

    async def get_diagnostics(self, charge_point_id, location, start_time=None, stop_time=None, retries=None, retry_interval=None):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].get_diagnostics(location, start_time, stop_time, retries, retry_interval)

    async def update_firmware(self, charge_point_id, location, retrieve_date, retries=None, retry_interval=None):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].update_firmware(location, retrieve_date, retries, retry_interval)

    async def reset(self, charge_point_id, type):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}
        return await self.charge_points[charge_point_id].reset(type)

    async def handle_charge_point(self, websocket, path):
        charge_point_id = path.strip("/")
        charge_point = ChargePoint(charge_point_id, websocket)
        self.charge_points[charge_point_id] = charge_point
        logging.info(f"Charge point {charge_point_id} connected.")
        await charge_point.start()

    def get_status(self, charge_point_id, params=None):
        if charge_point_id not in self.charge_points:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}

        cp = self.charge_points[charge_point_id]
        data = cp.charger_handler

        if not params or params == {}:
            return data

        filtered_data = {}
        for key in params:
            if key in data:
                filtered_data[key] = data[key]
        
        return filtered_data

    def get_all_status(self):
        return {cp_id: cp.charger_handler for cp_id, cp in self.charge_points.items()}

    async def server(self):
        logging.info("WebSocket server started.")
        async with websockets.serve(self.handle_charge_point, "localhost", 5000):
            await asyncio.Future()  # Run forever

if __name__ == "__main__":
    central_system = CentralSystem()
    asyncio.run(central_system.server())
