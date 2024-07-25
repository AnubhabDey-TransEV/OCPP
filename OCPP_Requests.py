import asyncio
import websockets
from datetime import datetime
from ocpp.routing import on
from ocpp.v16 import call, call_result, ChargePoint as CP
from Methods import currdatetime
import logging

logging.basicConfig(level=logging.DEBUG)

class ChargePoint(CP):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.charger_handler = {}

    @on('BootNotification')
    async def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        logging.debug(f"BootNotification received: {charge_point_vendor}, {charge_point_model}, {kwargs}")
        self.charger_handler[self.id] = {
            "vendor": charge_point_vendor,
            "model": charge_point_model,
            "connectors": {},
            "current_time": currdatetime(),
            **kwargs
        }
        logging.info(f"Boot Notification from {charge_point_vendor}'s, {charge_point_model} at {currdatetime()}")
        return call_result.BootNotificationPayload(
            current_time=currdatetime(),
            interval=300,
            status='Accepted'
        )

    @on('Heartbeat')
    async def on_heartbeat(self, **kwargs):
        logging.debug(f"Heartbeat received: {kwargs}")
        self.charger_handler[self.id]["last_heartbeat"] = currdatetime()
        self.charger_handler[self.id].update(kwargs)
        logging.info(f'Heartbeat received at {currdatetime()}')
        return call_result.HeartbeatPayload(
            current_time=currdatetime(),
        )

    @on('StartTransaction')
    async def on_start_transaction(self, connector_id, id_tag, timestamp, meter_start, reservation_id=None, **kwargs):
        logging.debug(f"StartTransaction request received: connectorID {connector_id}, idTag {id_tag}, timestamp {timestamp}, meter_start {meter_start}, reservation_id {reservation_id}, {kwargs}")
        if "connectors" not in self.charger_handler[self.id]:
            self.charger_handler[self.id]["connectors"] = {}
        self.charger_handler[self.id]["connectors"][connector_id] = {
            "transaction_id": 1,
            "id_tag": id_tag,
            "start_timestamp": timestamp,
            "meter_start": meter_start,
            "reservation_id": reservation_id,
            **kwargs
        }
        logging.info(f"StartTransaction: {self.charger_handler}")
        return call_result.StartTransactionPayload(
            transaction_id=1,
            id_tag_info={
                'status': 'Accepted'
            }
        )

    @on('StopTransaction')
    async def on_stop_transaction(self, transaction_id, id_tag, timestamp, meter_stop, reason=None, **kwargs):
        logging.debug(f"StopTransaction request received: transactionID {transaction_id}, idTag {id_tag}, timestamp {timestamp}, meter_stop {meter_stop}, reason {reason}, {kwargs}")
        for connector_id, data in self.charger_handler[self.id]["connectors"].items():
            if data["transaction_id"] == transaction_id:
                self.charger_handler[self.id]["connectors"][connector_id].update({
                    "stop_timestamp": timestamp,
                    "meter_stop": meter_stop,
                    "transaction_reason": reason,
                    **kwargs
                })
                break
        logging.info(f"StopTransaction: {self.charger_handler}")
        return call_result.StopTransactionPayload(
            id_tag_info={
                'status': 'Accepted'
            }
        )

    @on('MeterValues')
    async def on_meter_values(self, connector_id, transaction_id, meter_value, **kwargs):
        logging.debug(f"MeterValues received from connector {connector_id}: {meter_value}, {kwargs}")
        if "connectors" not in self.charger_handler[self.id]:
            self.charger_handler[self.id]["connectors"] = {}
        if connector_id not in self.charger_handler[self.id]["connectors"]:
            self.charger_handler[self.id]["connectors"][connector_id] = {}
        self.charger_handler[self.id]["connectors"][connector_id]["meter_values"] = meter_value
        self.charger_handler[self.id]["connectors"][connector_id].update(kwargs)
        logging.info(f"MeterValues: {self.charger_handler}")
        return call_result.MeterValuesPayload()

    @on('StatusNotification')
    async def on_status_notification(self, connector_id, status, error_code, info=None, timestamp=None, **kwargs):
        logging.debug(f"StatusNotification received: connectorID {connector_id}, status {status}, error_code {error_code}, info {info}, timestamp {timestamp}, {kwargs}")
        if "connectors" not in self.charger_handler[self.id]:
            self.charger_handler[self.id]["connectors"] = {}
        self.charger_handler[self.id]["connectors"][connector_id] = {
            "status": status,
            "error_code": error_code,
            "info": info,
            "timestamp": timestamp,
            **kwargs
        }
        logging.info(f"StatusNotification: {self.charger_handler}")

    @on('DiagnosticsStatusNotification')
    async def on_diagnostics_status_notification(self, status, **kwargs):
        logging.debug(f"DiagnosticsStatusNotification received: status {status}, {kwargs}")
        self.charger_handler[self.id]["diagnostics_status"] = {
            "status": status,
            **kwargs
        }
        logging.info(f"DiagnosticsStatusNotification: {self.charger_handler}")

    @on('FirmwareStatusNotification')
    async def on_firmware_status_notification(self, status, **kwargs):
        logging.debug(f"FirmwareStatusNotification received: status {status}, {kwargs}")
        self.charger_handler[self.id]["firmware_status"] = {
            "status": status,
            **kwargs
        }
        logging.info(f"FirmwareStatusNotification: {self.charger_handler}")

    # Below are the methods to request data from charging points

    async def remote_start_transaction(self, id_tag, connector_id):
        logging.debug(f"Sending RemoteStartTransaction: id_tag {id_tag}, connector_id {connector_id}")
        request = call.RemoteStartTransactionPayload(
            id_tag=id_tag,
            connector_id=connector_id,
        )
        response = await self.call(request)
        logging.debug(f"RemoteStartTransaction response: {response}")
        return response

    async def remote_stop_transaction(self, transaction_id):
        logging.debug(f"Sending RemoteStopTransaction: transaction_id {transaction_id}")
        request = call.RemoteStopTransactionPayload(transaction_id=transaction_id)
        response = await self.call(request)
        logging.debug(f"RemoteStopTransaction response: {response}")
        return response

    async def change_availability(self, connector_id, type):
        logging.debug(f"Sending ChangeAvailability: connector_id {connector_id}, type {type}")
        request = call.ChangeAvailabilityPayload(
            connector_id=connector_id,
            type=type,
        )
        response = await self.call(request)
        logging.debug(f"ChangeAvailability response: {response}")
        return response

    async def change_configuration(self, key, value):
        logging.debug(f"Sending ChangeConfiguration: key {key}, value {value}")
        request = call.ChangeConfigurationPayload(
            key=key,
            value=value,
        )
        response = await self.call(request)
        logging.debug(f"ChangeConfiguration response: {response}")
        return response

    async def clear_cache(self):
        logging.debug(f"Sending ClearCache")
        request = call.ClearCachePayload()
        response = await self.call(request)
        logging.debug(f"ClearCache response: {response}")
        return response

    async def unlock_connector(self, connector_id):
        logging.debug(f"Sending UnlockConnector: connector_id {connector_id}")
        request = call.UnlockConnectorPayload(
            connector_id=connector_id,
        )
        response = await self.call(request)
        logging.debug(f"UnlockConnector response: {response}")
        return response

    async def get_diagnostics(self, location, start_time=None, stop_time=None, retries=None, retry_interval=None):
        logging.debug(f"Sending GetDiagnostics: location {location}, start_time {start_time}, stop_time {stop_time}, retries {retries}, retry_interval {retry_interval}")
        request = call.GetDiagnosticsPayload(
            location=location,
            start_time=start_time,
            stop_time=stop_time,
            retries=retries,
            retry_interval=retry_interval,
        )
        response = await self.call(request)
        logging.debug(f"GetDiagnostics response: {response}")
        return response

    async def update_firmware(self, location, retrieve_date, retries=None, retry_interval=None):
        logging.debug(f"Sending UpdateFirmware: location {location}, retrieve_date {retrieve_date}, retries {retries}, retry_interval {retry_interval}")
        request = call.UpdateFirmwarePayload(
            location=location,
            retrieve_date=retrieve_date,
            retries=retries,
            retry_interval=retry_interval,
        )
        response = await self.call(request)
        logging.debug(f"UpdateFirmware response: {response}")
        return response

    async def reset(self, type):
        logging.debug(f"Sending Reset: type {type}")
        request = call.ResetPayload(
            type=type,
        )
        response = await self.call(request)
        logging.debug(f"Reset response: {response}")
        return response