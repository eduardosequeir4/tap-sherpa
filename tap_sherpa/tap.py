"""Sherpa tap class."""

from __future__ import annotations
from typing import Optional, Iterable, Dict, Any, List

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_sherpa import streams


class TapSherpa(Tap):
    """Sherpa tap class."""

    name = "tap-sherpa"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "wsdl_url",
            th.StringType,
            required=True,
            description="The WSDL URL for the Sherpa SOAP service",
            default="https://sherpaservices-tst.sherpacloud.eu/214/Sherpa.asmx?wsdl",
        ),
        th.Property(
            "security_code",
            th.StringType,
            required=True,
            secret=True,
            description="Security code for authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "orders_per_request",
            th.IntegerType,
            description="Number of orders to fetch per request",
            default=10,
        ),
        th.Property(
            "stock_per_request",
            th.IntegerType,
            description="Number of stock records to fetch per request",
            default=10,
        ),
        th.Property(
            "chunk_size",
            th.IntegerType,
            description="Number of records to process in each chunk",
            default=10,
        ),
        th.Property(
            "max_retries",
            th.IntegerType,
            description="Maximum number of retry attempts for failed requests",
            default=3,
        ),
        th.Property(
            "retry_wait_min",
            th.IntegerType,
            description="Minimum wait time between retries in seconds",
            default=4,
        ),
        th.Property(
            "retry_wait_max",
            th.IntegerType,
            description="Maximum wait time between retries in seconds",
            default=10,
        ),
        th.Property(
            "stream_tokens",
            th.ObjectType(
                th.Property("changed_items", th.IntegerType),
                th.Property("changed_orders", th.IntegerType),
                th.Property("changed_suppliers", th.IntegerType),
                th.Property("changed_item_suppliers", th.IntegerType),
                th.Property("changed_stock", th.IntegerType),
            ),
            description="Initial tokens for each stream",
        ),
    ).to_dict()

    def discover_streams(self) -> List[streams.SherpaStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ChangedItemsStream(self),
            streams.ChangedOrdersStream(self),
            streams.ChangedSuppliersStream(self),
            streams.ChangedItemSuppliersStream(self),
            streams.ChangedPurchasesStream(self),
            streams.ChangedParcelsStream(self),
            streams.ChangedStockStream(self),
        ]


if __name__ == "__main__":
    TapSherpa.cli()
