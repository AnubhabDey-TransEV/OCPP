import asyncio
import websockets
from datetime import datetime
from ocpp.routing import on
from ocpp.v16 import call, call_result, ChargePoint as CP
import logging
from Filters import (format_boot_notification, format_heartbeat, format_start_transaction,
                     format_stop_transaction, format_meter_values, format_status_notification,
                     format_diagnostics_status, format_firmware_status, format_remote_start_transaction,
                     format_remote_stop_transaction, format_change_availability, format_change_configuration,
                     format_clear_cache, format_unlock_connector, format_get_diagnostics, format_update_firmware,
                     format_reset)

logging.basicConfig(level=logging.DEBUG)

class ChargePoint(CP):

    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.charger_handler = {}

    def currdatetime(self):
        return datetime.now(datetime.UTC).isoformat()

    @on('BootNotification')
    async def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        logging.debug(f"BootNotification received: {charge_point_vendor}, {charge_point_model}, {kwargs}")
        self.charger_handler[self.id] = {
            "vendor": charge_point_vendor,
            "model": charge_point_model,
            "connectors": {},
            "current_time": self.currdatetime(),
            **kwargs
        }

        logging.info(f"Boot Notification from {charge_point_vendor}'s, {charge_point_model} at {self.currdatetime()}")

        acknowledgment = call_result.BootNotificationPayload(
            current_time=self.currdatetime(),
            interval=300,
            status='Accepted'
        )

        # Call the formatting function from Filters.py
        format_boot_notification(self.id, acknowledgment, 'Central System', charge_point_vendor, charge_point_model, vendor_response=acknowledgment.vendor, model_response=acknowledgment.model)

        return acknowledgment

    @on('Heartbeat')
    async def on_heartbeat(self, **kwargs):
        logging.debug(f"Heartbeat received: {kwargs}")
        self.charger_handler[self.id]["last_heartbeat"] = self.currdatetime()
        self.charger_handler[self.id].update(kwargs)

        logging.info(f'Heartbeat received at {self.currdatetime()}')

        acknowledgment = call_result.HeartbeatPayload(current_time=self.currdatetime())

        # Call the formatting function from Filters.py
        format_heartbeat(self.id, acknowledgment, 'Central System')

        return acknowledgment

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

        acknowledgment = call_result.StartTransactionPayload(
            transaction_id=1,
            id_tag_info={
                'status': 'Accepted'
            }
        )

        # Call the formatting function from Filters.py
        format_start_transaction(self.id, acknowledgment, 'Central System', connector_id, id_tag, timestamp, meter_start, transaction_id_response=acknowledgment.transaction_id, id_tag_response=acknowledgment.id_tag_info.status)

        return acknowledgment

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

        acknowledgment = call_result.StopTransactionPayload(
            id_tag_info={
                'status': 'Accepted'
            }
        )

        # Call the formatting function from Filters.py
        format_stop_transaction(self.id, acknowledgment, 'Central System', transaction_id, id_tag, timestamp, meter_stop, reason, id_tag_response=acknowledgment.id_tag_info.status)

        return acknowledgment

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

        acknowledgment = call_result.MeterValuesPayload()

        # Call the formatting function from Filters.py
        format_meter_values(self.id, acknowledgment, 'Central System', connector_id, transaction_id, meter_value, status_response=acknowledgment.status)

        return acknowledgment

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

        acknowledgment = call_result.StatusNotificationPayload()

        # Call the formatting function from Filters.py
        format_status_notification(self.id, acknowledgment, 'Central System', connector_id, status, error_code)

        return acknowledgment

    @on('DiagnosticsStatusNotification')
    async def on_diagnostics_status_notification(self, status, **kwargs):
        logging.debug(f"DiagnosticsStatusNotification received: status {status}, {kwargs}")
        self.charger_handler[self.id]["diagnostics_status"] = {
            "status": status,
            **kwargs
        }

        logging.info(f"DiagnosticsStatusNotification: {self.charger_handler}")

        acknowledgment = call_result.DiagnosticsStatusNotificationPayload()

        # Call the formatting function from Filters.py
        format_diagnostics_status(self.id, acknowledgment, 'Central System', status)

        return acknowledgment

    @on('FirmwareStatusNotification')
    async def on_firmware_status_notification(self, status, **kwargs):
        logging.debug(f"FirmwareStatusNotification received: status {status}, {kwargs}")
        self.charger_handler[self.id]["firmware_status"] = {
            "status": status,
            **kwargs
        }

        logging.info(f"FirmwareStatusNotification: {self.charger_handler}")

        acknowledgment = call_result.FirmwareStatusNotificationPayload()

        # Call the formatting function from Filters.py
        format_firmware_status(self.id, acknowledgment, 'Central System', status)

        return acknowledgment

    # Below are the methods to request data from charging points

    async def remote_start_transaction(self, id_tag, connector_id):
        logging.debug(f"Sending RemoteStartTransaction: id_tag {id_tag}, connector_id {connector_id}")
        request = call.RemoteStartTransactionPayload(
            id_tag=id_tag,
            connector_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"RemoteStartTransaction response: {response}")

        # Call the formatting function from Filters.py
        format_remote_start_transaction(self.id, response, 'Charger', id_tag, connector_id, transaction_id_response=response.transaction_id, status_response=response.status)

        return response

    async def remote_stop_transaction(self, transaction_id):
        logging.debug(f"Sending RemoteStopTransaction: transaction_id {transaction_id}")
        request = call.RemoteStopTransactionPayload(transaction_id=transaction_id)

        response = await self.call(request)
        logging.debug(f"RemoteStopTransaction response: {response}")

        # Call the formatting function from Filters.py
        format_remote_stop_transaction(self.id, response, 'Charger', transaction_id, status_response=response.status)

        return response

    async def change_availability(self, connector_id, type):
        logging.debug(f"Sending ChangeAvailability: connector_id {connector_id}, type {type}")
        request = call.ChangeAvailabilityPayload(
            connector_id=connector_id,
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"ChangeAvailability response: {response}")

        # Call the formatting function from Filters.py
        format_change_availability(self.id, response, 'Charger', connector_id, type)

        return response

    async def change_configuration(self, key, value):
        logging.debug(f"Sending ChangeConfiguration: key {key}, value {value}")
        request = call.ChangeConfigurationPayload(
            key=key,
            value=value,
        )

        response = await self.call(request)
        logging.debug(f"ChangeConfiguration response: {response}")

        # Call the formatting function from Filters.py
        format_change_configuration(self.id, response, 'Charger', key, value)

        return response

    async def clear_cache(self):
        logging.debug(f"Sending ClearCache")
        request = call.ClearCachePayload()

        response = await self.call(request)
        logging.debug(f"ClearCache response: {response}")

        # Call the formatting function from Filters.py
        format_clear_cache(self.id, response, 'Charger')

        return response

    async def unlock_connector(self, connector_id):
        logging.debug(f"Sending UnlockConnector: connector_id {connector_id}")
        request = call.UnlockConnectorPayload(
            connector_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"UnlockConnector response: {response}")

        # Call the formatting function from Filters.py
        format_unlock_connector(self.id, response, 'Charger', connector_id)

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

        # Call the formatting function from Filters.py
        format_get_diagnostics(self.id, response, 'Charger', location, start_time, stop_time, retries, retry_interval, status_response=response.status, info_response=response.info)

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

        # Call the formatting function from Filters.py
        format_update_firmware(self.id, response, 'Charger', location, retrieve_date, retries, retry_interval, status_response=response.status, info_response=response.info)

        return response

    async def reset(self, type):
        logging.debug(f"Sending Reset: type {type}")
        request = call.ResetPayload(
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"Reset response: {response}")

        # Call the formatting function from Filters.py
        format_reset(self.id, response, 'Charger', type, status_response=response.status)

        return response