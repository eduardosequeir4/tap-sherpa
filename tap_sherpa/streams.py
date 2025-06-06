"""Stream type classes for tap-sherpa."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers._state import increment_state

from tap_sherpa.client import SherpaStream
from tap_sherpa.pagination import PaginatedStream, PaginationMode

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class UsersStream(SherpaStream):
    """Define custom stream."""

    name = "users"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="The user's system ID",
        ),
        th.Property(
            "age",
            th.IntegerType,
            description="The user's age in years",
        ),
        th.Property(
            "email",
            th.StringType,
            description="The user's email address",
        ),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property(
            "state",
            th.StringType,
            description="State name in ISO 3166-2 format",
        ),
        th.Property("zip", th.StringType),
    ).to_dict()


class GroupsStream(SherpaStream):
    """Define custom stream."""

    name = "groups"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()


class TokenBasedStream(SherpaStream):
    """Base stream class for token-based pagination."""

    def get_records_with_token(
        self,
        service_name: str,
        token_param_name: str,
        response_items_path: list[str],
        record_mapper: t.Callable[[dict, int], dict],
        **service_params,
    ) -> t.Iterable[dict]:
        """Get records using token-based pagination.

        Args:
            service_name: Name of the SOAP service to call
            token_param_name: Name of the token parameter in the service call
            response_items_path: Path to items in the response (e.g., ["ResponseValue", "ItemStockToken"])
            record_mapper: Function to map response item to record
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the SOAP service.
        """
        # Get the stream-specific token from state, defaulting to 0
        last_token = int(self.get_starting_replication_key_value(None) or "0")
        
        while True:
            # Debug: Log the token being used for the SOAP call
            self.logger.info(f"[{self.name}] Starting sync with token: {last_token}")
            
            # Prepare parameters for the SOAP call
            call_params = {token_param_name: str(last_token), **service_params}
            
            # Log the curl command
            curl_cmd = self.client.get_curl_command(service_name, call_params, stream_name=self.name)
            self.logger.info(f"[{self.name}] SOAP Request (curl):\n{curl_cmd}")
            
            # Call the SOAP service with the last token
            response = self.client.call_service(
                service_name,
                stream_name=self.name,
                **call_params,
            )

            # Extract response time
            response_time = response.get("ResponseTime", 0)
            
            # Get items from response using the path
            items = response
            for path_part in response_items_path:
                items = items.get(path_part, {})
            items = items if isinstance(items, list) else []

            # If no items in response, save the last token and stop
            if not items:
                self.logger.info(f"[{self.name}] No more records. Last token: {last_token}")
                # Save the last token for next job
                self._increment_stream_state(str(last_token))  # Convert to string for state
                self._write_state_message()
                break

            # Process each item in the response
            highest_token = last_token
            first_token = None
            for item in items:
                record = record_mapper(item, response_time)
                current_token = int(item.get("Token", 0))
                
                # Track first token in this batch
                if first_token is None:
                    first_token = current_token
                
                yield record

                # Update the highest token
                if current_token > highest_token:
                    highest_token = current_token

            # Log token progression
            self.logger.info(
                f"[{self.name}] Token progression: {last_token} -> {first_token} -> {highest_token} "
                f"(batch size: {len(items)})"
            )

            # Update last_token for next iteration
            if highest_token != last_token:
                last_token = highest_token
                # Save the token for next job
                self._increment_stream_state(str(last_token))  # Convert to string for state
                self._write_state_message()
            else:
                # If we didn't get a higher token, we've reached the end
                self.logger.info(f"[{self.name}] Reached end of records. Last token: {last_token}")
                break


class ChangedItemsStream(PaginatedStream):
    """Stream for Sherpa Changed Items."""
    name = "changed_items"
    primary_keys = ["item_code"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("item_code", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("item_status", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedItems"
    response_path = "ResponseValue.ItemCodeToken"


class ChangedOrdersStream(PaginatedStream):
    """Stream for changed orders."""
    name = "changed_orders"
    primary_keys = ["order_number"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("order_number", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("order_status", th.StringType),
        th.Property("warehouse_code", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedOrders"
    response_path = "ResponseValue.OrderNumberToken"


class ChangedSuppliersStream(PaginatedStream):
    """Stream for changed suppliers."""
    name = "changed_suppliers"
    primary_keys = ["supplier_code"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("supplier_code", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("supplier_status", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedSuppliers"
    response_path = "ResponseValue.ClientCodeToken"


class ChangedItemSuppliersStream(PaginatedStream):
    """Stream for changed item suppliers."""
    name = "changed_item_suppliers"
    primary_keys = ["supplier_code", "item_code"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("supplier_code", th.StringType),
        th.Property("supplier_item_code", th.StringType),
        th.Property("item_code", th.StringType),
        th.Property("supplier_description", th.StringType),
        th.Property("supplier_stock", th.IntegerType),
        th.Property("supplier_price", th.NumberType),
        th.Property("preferred", th.BooleanType),
        th.Property("token", th.IntegerType),
        th.Property("available_from", th.DateTimeType),
        th.Property("supplier_item_status", th.StringType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("min_purchase_qty", th.IntegerType),
        th.Property("supplier_purchase_qty", th.IntegerType),
        th.Property("supplier_purchase_qty_multiplier", th.IntegerType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedItemSuppliers"
    response_path = "ResponseValue.SupplierItemCodeToken"


class ChangedPurchasesStream(PaginatedStream):
    """Stream for changed purchases."""
    name = "changed_purchases"
    primary_keys = ["purchase_code"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("purchase_code", th.StringType),
        th.Property("order_number", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("purchase_status", th.StringType),
        th.Property("warehouse_code", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedPurchases"
    response_path = "ResponseValue.PurchaseCodeToken"


class ChangedStockStream(PaginatedStream):
    """Stream for changed stock."""
    name = "changed_stock"
    primary_keys = ["item_code", "warehouse_code"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("item_code", th.StringType),
        th.Property("available", th.IntegerType),
        th.Property("stock", th.IntegerType),
        th.Property("reserved", th.IntegerType),
        th.Property("item_status", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("expected_date", th.DateTimeType),
        th.Property("qty_waiting_to_receive", th.IntegerType),
        th.Property("first_expected_date", th.DateTimeType),
        th.Property("first_expected_qty_waiting_to_receive", th.IntegerType),
        th.Property("last_modified", th.DateTimeType),
        th.Property("avg_purchase_price", th.NumberType),
        th.Property("warehouse_code", th.StringType),
        th.Property("cost_price", th.NumberType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedStock"
    response_path = "ResponseValue.ItemStockToken"


class ChangedParcelsStream(PaginatedStream):
    """Stream for changed parcels."""
    name = "changed_parcels"
    primary_keys = ["parcel_code"]
    replication_key = "token"
    schema = th.PropertiesList(
        th.Property("parcel_code", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("barcode", th.StringType),
        th.Property("order_number", th.StringType),
        th.Property("parcel_service_code", th.StringType),
        th.Property("parcel_type_code", th.StringType),
        th.Property("track_trace_url", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    service_name = "ChangedParcels"
    response_path = "ResponseValue.ParcelCodeToken"
