"""Sherpa tap class."""

from __future__ import annotations
import json
import os
from pathlib import Path
import time
from typing import Optional, Iterable, Dict, Any, List

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_sherpa import streams


class TapSherpa(Tap):
    """Sherpa tap class."""

    name = "tap-sherpa"

    def __init__(self, *args, **kwargs):
        """Initialize the tap."""
        super().__init__(*args, **kwargs)
        self._load_state()

    def _load_secrets(self) -> dict:
        """Load secrets from .secrets/config.json.

        Returns:
            Dictionary containing secrets.
        """
        secrets_path = Path(".secrets/config.json")
        if not secrets_path.exists():
            return {}
        
        with open(secrets_path) as f:
            return json.load(f)

    def _get_secret(self, key: str) -> str | None:
        """Get a secret value from config or environment.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value if found, None otherwise.
        """
        # First try environment variable
        env_key = f"TAP_SHERPA_{key.upper()}"
        if value := os.getenv(env_key):
            return value
        
        # Then try secrets file
        secrets = self._load_secrets()
        return secrets.get(key)

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

    def _get_security_code(self) -> str:
        """Get the security code from config or secrets.

        Returns:
            The security code.
        """
        if value := self._get_secret("security_code"):
            return value
        return self.config["security_code"]

    def _load_state(self) -> None:
        """Load state from state.json if it exists."""
        try:
            with open("state.json", "r", encoding="utf-8") as f:
                state = json.load(f)
                # Initialize _tap_state if it doesn't exist
                if not hasattr(self, '_tap_state'):
                    self._tap_state = {"bookmarks": {}}
                # Update the state using the proper method
                for stream_name, stream_state in state.get("bookmarks", {}).items():
                    self._tap_state["bookmarks"][stream_name] = stream_state
        except FileNotFoundError:
            pass  # No state file exists yet
        except Exception as e:
            self.logger.error(f"Failed to load state from state.json: {e}")

    def emit_state(self, state: dict) -> None:
        """Emit state to stdout and write to state.json."""
        super().emit_state(state)
        try:
            with open("state.json", "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to write state to state.json: {e}")


class ChangedItemsStream(streams.SherpaStream):
    """Stream for changed items."""
    name = "changed_items"
    primary_keys = ["item_code"]
    replication_key = "token"
    replication_method = "INCREMENTAL"
    schema = th.PropertiesList(
        th.Property("item_code", th.StringType),
        th.Property("token", th.IntegerType),
        th.Property("item_status", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the API."""
        start_time = time.time()
        client = SherpaClient(
            self.config["api_url"],
            self.config["username"],
            self.config["password"],
            self.config["company_id"],
            self.config["branch_id"],
        )
        
        # Get the token from config
        last_token = self.config.get("changed_items_token", 0)
        
        while True:
            # Get changed items
            changed_items = client.get_changed_items(last_token)
            
            if not changed_items:
                self.logger.info("No more records to process")
                break
                
            # Find the highest token in this batch
            highest_token = last_token
            for item in changed_items:
                record = {
                    "item_code": item["ItemCode"],
                    "token": item["Token"],
                    "item_status": item["ItemStatus"],
                    "response_time": int((time.time() - start_time) * 1000),
                }
                
                # Update highest token
                if record["token"] > highest_token:
                    highest_token = record["token"]
                
                # Only yield records with tokens greater than the last processed token
                if record["token"] > last_token:
                    yield record
            
            # Update last_token for next iteration
            if highest_token > last_token:
                last_token = highest_token
                self._increment_stream_state(last_token)
                self._write_state_message()
            else:
                break


if __name__ == "__main__":
    TapSherpa.cli()
