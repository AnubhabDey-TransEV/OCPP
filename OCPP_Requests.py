from datetime import datetime, timezone
import pytz
import asyncio
import logging
from ocpp.routing import on
from ocpp.v16 import call, call_result, ChargePoint as CP
import Chargers_to_CMS_Parser as parser_c2c
import CMS_to_Charger_Parser as parser_ctc

logging.basicConfig(level=logging.DEBUG)
IST = pytz.timezone('Asia/Kolkata')

class ChargePoint(CP):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.charger_id = id  # Store Charger_ID

    def currdatetime(self):
        return datetime.now(timezone.utc).astimezone(IST).isoformat()

    # Inbound messages from chargers to CMS
    @on('BootNotification')
    async def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        parser_c2c.parse_and_store_boot_notification(self.charger_id, charge_point_vendor=charge_point_vendor, charge_point_model=charge_point_model, **kwargs)

        response = call_result.BootNotification(
            current_time=self.currdatetime(),
            interval=300,
            status='Accepted'
        )
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "BootNotification", "BootNotification", self.currdatetime(), status='Accepted')
        return response

    @on('Heartbeat')
    async def on_heartbeat(self, **kwargs):
        parser_c2c.parse_and_store_heartbeat(self.charger_id, **kwargs)

        response = call_result.Heartbeat(current_time=self.currdatetime())
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "Heartbeat", "Heartbeat", self.currdatetime())
        return response

    @on('StartTransaction')
    async def on_start_transaction(self, connector_id, id_tag, timestamp, meter_start, reservation_id=None, **kwargs):
        parser_c2c.parse_and_store_start_transaction(self.charger_id, connector_id=connector_id, id_tag=id_tag, timestamp=timestamp, meter_start=meter_start, reservation_id=reservation_id, **kwargs)

        response = call_result.StartTransaction(
            transaction_id=1,
            id_tag_info={
                'status': 'Accepted'
            }
        )
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "StartTransaction", "StartTransaction", self.currdatetime(), transaction_id=1, status='Accepted')
        return response

    @on('StopTransaction')
    async def on_stop_transaction(self, transaction_id, id_tag, timestamp, meter_stop, reason=None, **kwargs):
        parser_c2c.parse_and_store_stop_transaction(self.charger_id, transaction_id=transaction_id, id_tag=id_tag, timestamp=timestamp, meter_stop=meter_stop, reason=reason, **kwargs)

        response = call_result.StopTransaction(
            id_tag_info={
                'status': 'Accepted'
            }
        )
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "StopTransaction", "StopTransaction", self.currdatetime(), transaction_id=transaction_id, status='Accepted')
        return response

    @on('MeterValues')
    async def on_meter_values(self, connector_id, transaction_id, meter_value, **kwargs):
        parser_c2c.parse_and_store_meter_values(self.charger_id, connector_id=connector_id, transaction_id=transaction_id, meter_value=meter_value, **kwargs)

        response = call_result.MeterValues()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "MeterValues", "MeterValues", self.currdatetime())
        return response

    @on('StatusNotification')
    async def on_status_notification(self, connector_id, status, error_code, info=None, timestamp=None, **kwargs):
        parser_c2c.parse_and_store_status_notification(self.charger_id, connector_id=connector_id, status=status, error_code=error_code, info=info, timestamp=timestamp, **kwargs)

        response = call_result.StatusNotification()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "StatusNotification", "StatusNotification", self.currdatetime())
        return response

    @on('DiagnosticsStatusNotification')
    async def on_diagnostics_status_notification(self, status, **kwargs):
        parser_c2c.parse_and_store_diagnostics_status(self.charger_id, status=status, **kwargs)

        response = call_result.DiagnosticsStatusNotification()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "DiagnosticsStatusNotification", "DiagnosticsStatusNotification", self.currdatetime())
        return response

    @on('FirmwareStatusNotification')
    async def on_firmware_status_notification(self, status, **kwargs):
        parser_c2c.parse_and_store_firmware_status(self.charger_id, status=status, **kwargs)

        response = call_result.FirmwareStatusNotification()
        parser_ctc.parse_and_store_acknowledgment(self.charger_id, "FirmwareStatusNotification", "FirmwareStatusNotification", self.currdatetime())
        return response

    # Outbound messages from CMS to Charger
    async def remote_start_transaction(self, id_tag, connector_id):
        logging.debug(f"Sending RemoteStartTransaction: id_tag {id_tag}, connector_id {connector_id}")
        request = call.RemoteStartTransaction(
            id_tag=id_tag,
            connector_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"RemoteStartTransaction response: {response}")

        parser_ctc.parse_and_store_remote_start_transaction(self.charger_id, id_tag=id_tag, connector_id=connector_id)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "RemoteStartTransaction", "RemoteStartTransaction", self.currdatetime(), transaction_id=response.transaction_id, status=response.status)
        return response

    async def remote_stop_transaction(self, transaction_id):
        logging.debug(f"Sending RemoteStopTransaction: transaction_id {transaction_id}")
        request = call.RemoteStopTransaction(transaction_id=transaction_id)

        response = await self.call(request)
        logging.debug(f"RemoteStopTransaction response: {response}")

        parser_ctc.parse_and_store_remote_stop_transaction(self.charger_id, transaction_id=transaction_id)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "RemoteStopTransaction", "RemoteStopTransaction", self.currdatetime(), status=response.status)
        return response

    async def change_availability(self, connector_id, type):
        logging.debug(f"Sending ChangeAvailability: connector_id {connector_id}, type {type}")
        request = call.ChangeAvailability(
            connector_id=connector_id,
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"ChangeAvailability response: {response}")

        parser_ctc.parse_and_store_change_availability(self.charger_id, connector_id=connector_id, type=type)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "ChangeAvailability", "ChangeAvailability", self.currdatetime(), status=response.status)
        return response

    async def change_configuration(self, key, value):
        logging.debug(f"Sending ChangeConfiguration: key {key}, value {value}")
        request = call.ChangeConfiguration(
            key=key,
            value=value,
        )

        response = await self.call(request)
        logging.debug(f"ChangeConfiguration response: {response}")

        parser_ctc.parse_and_store_change_configuration(self.charger_id, key=key, value=value)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "ChangeConfiguration", "ChangeConfiguration", self.currdatetime(), status=response.status)
        return response

    async def clear_cache(self):
        logging.debug(f"Sending ClearCache")
        request = call.ClearCache()

        response = await self.call(request)
        logging.debug(f"ClearCache response: {response}")

        parser_ctc.parse_and_store_clear_cache(self.charger_id)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "ClearCache", "ClearCache", self.currdatetime(), status=response.status)
        return response

    async def unlock_connector(self, connector_id):
        logging.debug(f"Sending UnlockConnector: connector_id {connector_id}")
        request = call.UnlockConnector(
            connector_id=connector_id,
        )

        response = await self.call(request)
        logging.debug(f"UnlockConnector response: {response}")

        parser_ctc.parse_and_store_unlock_connector(self.charger_id, connector_id=connector_id)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "UnlockConnector", "UnlockConnector", self.currdatetime(), status=response.status)
        return response

    async def get_diagnostics(self, location, start_time=None, stop_time=None, retries=None, retry_interval=None):
        logging.debug(f"Sending GetDiagnostics: location {location}, start_time={start_time}, stop_time={stop_time}, retries={retries}, retry_interval={retry_interval}")
        request = call.GetDiagnostics(
            location=location,
            start_time=start_time,
            stop_time=stop_time,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        logging.debug(f"GetDiagnostics response: {response}")

        parser_ctc.parse_and_store_get_diagnostics(self.charger_id, location=location, start_time=start_time, stop_time=stop_time, retries=retries, retry_interval=retry_interval)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "GetDiagnostics", "GetDiagnostics", self.currdatetime(), status=response.status)
        return response

    async def update_firmware(self, location, retrieve_date, retries=None, retry_interval=None):
        logging.debug(f"Sending UpdateFirmware: location {location}, retrieve_date={retrieve_date}, retries={retries}, retry_interval={retry_interval}")
        request = call.UpdateFirmware(
            location=location,
            retrieve_date=retrieve_date,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        logging.debug(f"UpdateFirmware response: {response}")

        parser_ctc.parse_and_store_update_firmware(self.charger_id, location=location, retrieve_date=retrieve_date, retries=retries, retry_interval=retry_interval)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "UpdateFirmware", "UpdateFirmware", self.currdatetime(), status=response.status)
        return response

    async def reset(self, type):
        logging.debug(f"Sending Reset: type {type}")
        request = call.Reset(
            type=type,
        )

        response = await self.call(request)
        logging.debug(f"Reset response: {response}")

        parser_ctc.parse_and_store_reset(self.charger_id, type=type)
        parser_c2c.parse_and_store_acknowledgment(self.charger_id, "Reset", "Reset", self.currdatetime(), status=response.status)
        return response
