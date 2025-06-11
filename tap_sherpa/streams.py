"""Stream type classes for tap-sherpa."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from singer_sdk.helpers._state import increment_state

from tap_sherpa.client import SherpaClient

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class SherpaStream(Stream):
    """Base stream class for Sherpa streams."""

    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self.client = SherpaClient(
            wsdl_url=self.config["wsdl_url"],
            tap=self._tap,
        )

    def get_records(
        self,
        context: t.Optional[dict],
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Dictionary objects representing records from the SOAP service.
        """
        # This method should be implemented by specific stream classes
        raise NotImplementedError("Stream classes must implement get_records")


# Import PaginatedStream after SherpaStream is defined to avoid circular imports
from tap_sherpa.pagination import PaginatedStream, PaginationMode


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
