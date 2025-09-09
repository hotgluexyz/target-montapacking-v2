"""Montapackingv2 target sink class, which handles writing streams."""

from target_montapackingv2.client import MontapackingSink

class InboundForecastSink(MontapackingSink):

    endpoint = "inbound_forecast"
    name = "BuyOrders"

    def preprocess_record(self, record: dict, context: dict) -> None:
        line_items = self.parse_json(record.get("line_items", []))
        delivery_date = self.convert_datetime(record.get("created_at"))
        transaction_date = self.convert_datetime(record.get("transaction_date"))

        lines = [
            {
                "DeliveryDate": delivery_date,
                "Sku": i.get("sku"),
                "Quantity": i.get("quantity"),
            }
            for i in line_items
        ]

        mapping = {
            "Reference": str(record.get("id")),
            "SupplierCode": record.get("customer_id"),
            "InboundForecasts": lines,
            "Created": transaction_date,  # seems like Montapacking API ignores this field
            "DeliveryDate": delivery_date,
        }

        return mapping
    
    def upsert_record(self, record: dict, context: dict):
        endpoint = "inboundforecast/group"
        state_updates = dict()
        state_updates['error'] = []
        if record:
            try:
                state_updates['success'] = True
                buy_order_response = self.request_api(
                    "POST", endpoint=endpoint, request_data=record
                )
                buy_order_remoteId = buy_order_response.json()["UniqueId"]
                # input_id = record.get("id")
                self.logger.info(f"BuyOrder created succesfully with UniqueId {buy_order_remoteId}")
                return buy_order_remoteId, True, state_updates
            #Job should not fail.    
            except Exception as e:
                state_updates['success'] = False
                state_updates['error'].append(str(e))
                return None, False, state_updates


class UpdateInventory(MontapackingSink):

    endpoint = "update_inventory"
    name = "UpdateInventory"
    endpoint = "UpdateInventory"

    def preprocess_record(self, record: dict, context: dict) -> None:
        pass
    
    def upsert_record(self, record: dict, context: dict) -> None:
        pass
