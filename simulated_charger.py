import asyncio
import websockets
import logging
from ocpp.v16 import call
from ocpp.routing import on
from ocpp.v16 import ChargePoint as CP
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)

class DummyCharger(CP):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.id = id

    async def send_boot_notification(self):
        logging.info("Sending BootNotification...")
        response = await self.call(call.BootNotificationPayload(
            charge_point_vendor="DummyVendor",
            charge_point_model="DummyModel"
        ))
        logging.info(f"BootNotification response: {response}")

    async def send_heartbeat(self):
        logging.info("Sending Heartbeat...")
        response = await self.call(call.HeartbeatPayload())
        logging.info(f"Heartbeat response: {response}")

    async def send_start_transaction(self, connector_id, id_tag):
        logging.info("Sending StartTransaction...")
        response = await self.call(call.StartTransactionPayload(
            connector_id=connector_id,
            id_tag=id_tag,
            meter_start=0,
            timestamp=datetime.utcnow().isoformat()
        ))
        logging.info(f"StartTransaction response: {response}")
        return response.transaction_id  # Return the transaction ID for later use

    async def send_stop_transaction(self, transaction_id, id_tag):
        logging.info("Sending StopTransaction...")
        response = await self.call(call.StopTransactionPayload(
            transaction_id=transaction_id,
            id_tag=id_tag,
            meter_stop=100,
            timestamp=datetime.utcnow().isoformat()
        ))
        logging.info(f"StopTransaction response: {response}")

    async def send_meter_values(self, connector_id, transaction_id):
        logging.info("Sending MeterValues...")
        response = await self.call(call.MeterValuesPayload(
            connector_id=connector_id,
            transaction_id=transaction_id,
            meter_value=[{
                'timestamp': datetime.utcnow().isoformat(),
                'value': 50
            }]
        ))
        logging.info(f"MeterValues response: {response}")

    async def send_status_notification(self, connector_id, status):
        logging.info("Sending StatusNotification...")
        response = await self.call(call.StatusNotificationPayload(
            connector_id=connector_id,
            status=status,
            error_code="NoError"
        ))
        logging.info(f"StatusNotification response: {response}")

    async def run(self):
        await self.send_boot_notification()
        await self.send_heartbeat()
        transaction_id = await self.send_start_transaction(connector_id=1, id_tag="ABC123")
        await self.send_meter_values(connector_id=1, transaction_id=transaction_id)
        await self.send_stop_transaction(transaction_id=transaction_id, id_tag="ABC123")
        await self.send_status_notification(connector_id=1, status="Available")

async def main():
    async with websockets.connect("ws://localhost:5000/charger1") as websocket:
        charger = DummyCharger("charger1", websocket)
        await charger.start()
        await charger.run()

if __name__ == "__main__":
    asyncio.run(main())
