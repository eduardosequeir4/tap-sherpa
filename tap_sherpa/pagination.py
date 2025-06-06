"""Pagination utilities for tap-sherpa."""

from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, Generator
import time
from tenacity import retry, stop_after_attempt, wait_exponential

from singer_sdk import typing as th
from singer_sdk.streams import Stream
from tap_sherpa.client import SherpaClient


class PaginationMode(str, Enum):
    """Supported pagination modes."""

    TOKEN = "token"
    CURSOR = "cursor"
    OFFSET = "offset"


class PaginationConfig:
    """Configuration for pagination."""

    def __init__(
        self,
        chunk_size: int = 1000,
        max_retries: int = 3,
        retry_wait_min: int = 4,
        retry_wait_max: int = 10,
        mode: PaginationMode = PaginationMode.TOKEN,
    ):
        """Initialize pagination config.

        Args:
            chunk_size: Number of records to process in each chunk
            max_retries: Maximum number of retry attempts
            retry_wait_min: Minimum wait time between retries in seconds
            retry_wait_max: Maximum wait time between retries in seconds
            mode: Pagination mode to use (TOKEN, CURSOR, or OFFSET)
        """
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.retry_wait_min = retry_wait_min
        self.retry_wait_max = retry_wait_max
        self.mode = mode


class PaginatedStream(Stream):
    """Base class for streams with pagination support."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the stream.

        Args:
            *args: Positional arguments to pass to parent class
            **kwargs: Keyword arguments to pass to parent class
        """
        super().__init__(*args, **kwargs)
        self._total_records = 0
        self._pagination_config = PaginationConfig(
            chunk_size=self.config.get("chunk_size", 1000),
            max_retries=self.config.get("max_retries", 3),
            retry_wait_min=self.config.get("retry_wait_min", 4),
            retry_wait_max=self.config.get("retry_wait_max", 10),
            mode=PaginationMode.TOKEN,  # All streams use token-based pagination
        )
        # Initialize SherpaClient for SOAP requests
        self.client = SherpaClient(
            wsdl_url=self.config["wsdl_url"],
            tap=self._tap,
        )

    def map_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Map a response item to a record.

        Args:
            item: The response item to map

        Returns:
            The mapped record
        """
        # Get the field names from the schema
        schema_props = self.schema.get("properties", {})
        field_names = list(schema_props.keys())

        # Map the record based on the stream type
        if self.name == "changed_items":
            return {
                "item_code": item.get("ItemCode"),
                "token": item.get("Token"),
                "item_status": item.get("ItemStatus"),
            }
        elif self.name == "changed_orders":
            return {
                "order_number": item.get("OrderNumber"),
                "token": item.get("Token"),
                "order_status": item.get("OrderStatus"),
                "warehouse_code": item.get("WarehouseCode"),
            }
        elif self.name == "changed_suppliers":
            return {
                "supplier_code": item.get("ClientCode"),
                "token": item.get("Token"),
                "supplier_status": "Active",  # Default status since it's not in the response
            }
        elif self.name == "changed_item_suppliers":
            return {
                "supplier_code": item.get("SupplierCode"),
                "supplier_item_code": item.get("SupplierItemCode"),
                "item_code": item.get("ItemCode"),
                "supplier_description": item.get("SupplierDescription"),
                "supplier_stock": item.get("SupplierStock"),
                "supplier_price": item.get("SupplierPrice"),
                "preferred": item.get("Preferred"),
                "token": item.get("Token"),
                "available_from": item.get("AvailableFrom"),
                "supplier_item_status": item.get("SupplierItemStatus"),
                "last_modified": item.get("LastModified"),
                "min_purchase_qty": item.get("MinPurchaseQty"),
                "supplier_purchase_qty": item.get("SupplierPurchaseQty"),
                "supplier_purchase_qty_multiplier": item.get("SupplierPurchaseQtyMultiplier"),
            }
        elif self.name == "changed_purchases":
            return {
                "purchase_code": item.get("PurchaseCode"),
                "order_number": item.get("OrderNumber"),
                "token": item.get("Token"),
                "purchase_status": item.get("PurchaseStatus"),
                "warehouse_code": item.get("WarehouseCode"),
            }
        elif self.name == "changed_parcels":
            return {
                "parcel_code": item.get("ParcelCode"),
                "token": item.get("Token"),
                "barcode": item.get("Barcode"),
                "order_number": item.get("OrderNumber"),
                "parcel_service_code": item.get("ParcelServiceCode"),
                "parcel_type_code": item.get("ParcelTypeCode"),
                "track_trace_url": item.get("TrackTraceUrl"),
            }
        elif self.name == "changed_stock":
            return {
                "item_code": item.get("ItemCode"),
                "available": item.get("Available"),
                "stock": item.get("Stock"),
                "reserved": item.get("Reserved"),
                "item_status": item.get("ItemStatus"),
                "token": item.get("Token"),
                "expected_date": item.get("ExpectedDate"),
                "qty_waiting_to_receive": item.get("QtyWaitingToReceive"),
                "first_expected_date": item.get("FirstExpectedDate"),
                "first_expected_qty_waiting_to_receive": item.get("FirstExpectedQtyWaitingToReceive"),
                "last_modified": item.get("LastModified"),
                "avg_purchase_price": item.get("AvgPurchasePrice"),
                "warehouse_code": item.get("WarehouseCode"),
                "cost_price": item.get("CostPrice"),
            }
        else:
            # For unknown streams, return None
            return None

    def _get_state(self) -> Dict[str, Any]:
        """Get the current state with additional metadata.

        Returns:
            Dictionary containing the current state
        """
        state = super()._get_state()
        state.update({
            "last_sync": time.time(),
            "total_records_processed": self._total_records,
            "replication_key_value": self.get_starting_replication_key_value(None),
        })
        return state

    def _increment_stream_state(self, token: Union[str, Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> None:
        """Increment stream state with additional tracking.

        Args:
            token: The new token value or record dictionary
            context: Optional context dictionary
        """
        # Handle both dictionary and simple token values
        if isinstance(token, dict):
            token_value = token.get("token", token.get("Token", 0))
        else:
            token_value = token
            
        # Convert token to integer for consistent handling
        token_value = int(token_value)
        
        # Always pass a record with the replication key and value
        replication_key = getattr(self, "replication_key", "token")
        record = {replication_key: token_value}
        super()._increment_stream_state(record, context=context)
        self._total_records += 1

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_request(self, service_name: str, stream_name: str = None, **params: Any) -> Dict[str, Any]:
        """Make a request with retry logic.

        Args:
            service_name: Name of the service to call
            stream_name: Name of the stream (for stream-specific token)
            **params: Parameters to pass to the service

        Returns:
            Response from the service

        Raises:
            Exception: If the request fails after all retries
        """
        try:
            return self.client.call_service(service_name, stream_name=stream_name, **params)
        except Exception as e:
            self.logger.error(f"Error making request: {str(e)}")
            raise

    def get_records_with_pagination(
        self,
        service_name: str,
        context: Optional[dict] = None,
        **service_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using the configured pagination mode.

        Args:
            service_name: Name of the service to call
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the service
        """
        if self._pagination_config.mode == PaginationMode.TOKEN:
            yield from self.get_records_with_token(
                service_name=service_name,
                token_param_name="token",
                response_path=self.response_path,
                context=context,
                **service_params,
            )
        elif self._pagination_config.mode == PaginationMode.CURSOR:
            yield from self.get_records_with_cursor(
                service_name=service_name,
                context=context,
                **service_params,
            )
        else:  # OFFSET
            yield from self.get_records_with_offset(
                service_name=service_name,
                context=context,
                **service_params,
            )

    def get_records_with_token(
        self,
        service_name: str,
        token_param_name: str,
        response_path: str,
        context: Optional[dict] = None,
        **call_params,
    ) -> Generator[Dict[str, Any], None, None]:
        """Get records using token-based pagination.

        Args:
            service_name: Name of the SOAP service to call
            token_param_name: Name of the token parameter in the SOAP call
            response_path: Path to the response items in the response dict (dot-separated string)
            **call_params: Additional parameters to pass to the SOAP call

        Yields:
            Records from the API
        """
        # 1. Get initial token from state bookmarks
        last_token = self.get_starting_replication_key_value(context)
        if last_token is None:
            last_token = 1

        self.logger.info(f"[{self.name}] Starting sync with token: {last_token}")

        while True:
            # 2. Make request using current token
            call_params[token_param_name] = last_token
            response = self._make_request(service_name, stream_name=self.name, **call_params)
            response_time = response.get("ResponseTime", 0)

            # 3. Extract items from response
            items = response
            for part in response_path.split('.'):
                if isinstance(items, dict):
                    items = items.get(part, {})
                else:
                    items = {}
            
            if not items or items == {}:
                self.logger.info(f"[{self.name}] Empty response, stopping pagination")
                break

            # Ensure items is a list
            if not isinstance(items, list):
                items = []

            # 4. Process items and find highest token
            highest_token = int(last_token)
            for item in items:
                # Get token before mapping record
                item_token = int(item.get("Token", 0))
                if item_token > highest_token:
                    highest_token = item_token

                # Map and yield record
                record = self.map_record(item)
                if record:
                    record["response_time"] = response_time
                    yield record

            # 5. Update token for next request
            if highest_token > 0:
                # Since API always returns tokens > request token, we can use highest_token directly
                next_token = str(highest_token)
                self.logger.info(f"[{self.name}] Token progression: {last_token} -> {next_token} (batch size: {len(items)})")
                last_token = next_token
                self._increment_stream_state(last_token)
                self._write_state_message()
            else:
                self.logger.info(f"[{self.name}] No valid tokens found in response, stopping pagination")
                break

    def get_records_with_cursor(
        self,
        service_name: str,
        context: Optional[dict] = None,
        cursor_param_name: str = "cursor",
        **service_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using cursor-based pagination.

        Args:
            service_name: Name of the service to call
            cursor_param_name: Name of the cursor parameter in the service call
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the service
        """
        cursor = self.get_starting_replication_key_value(context)
        
        while True:
            # Make the request with retry logic
            response = self._make_request(
                service_name,
                **{cursor_param_name: cursor, **service_params}
            )
            
            # Process records
            records = response.get("records", [])
            if not records:
                break
                
            for record in records:
                yield record
                cursor = record.get("cursor")
            
            # Update state
            self._increment_stream_state(cursor)
            self._write_state_message()

    def get_records_with_offset(
        self,
        service_name: str,
        context: Optional[dict] = None,
        offset_param_name: str = "offset",
        limit_param_name: str = "limit",
        **service_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using offset-based pagination.

        Args:
            service_name: Name of the service to call
            offset_param_name: Name of the offset parameter in the service call
            limit_param_name: Name of the limit parameter in the service call
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the service
        """
        offset = 0
        limit = self._pagination_config.chunk_size
        
        while True:
            # Make the request with retry logic
            response = self._make_request(
                service_name,
                **{
                    offset_param_name: offset,
                    limit_param_name: limit,
                    **service_params
                }
            )
            
            # Process records
            records = response.get("records", [])
            if not records:
                break
                
            for record in records:
                yield record
            
            # Update offset
            offset += len(records)
            self._increment_stream_state(str(offset))
            self._write_state_message()
            
            # If we got fewer records than the limit, we're done
            if len(records) < limit:
                break 

    def get_starting_replication_key_value(self, context: Optional[dict] = None) -> Optional[str]:
        """Get the starting replication key value from state only. Config is not a source of truth."""
        state = self._tap_state
        if "bookmarks" in state and self.name in state["bookmarks"]:
            return state["bookmarks"][self.name].get("replication_key_value")
        # Default to 1 if no token found in state
        return "1"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get records from the API.

        Args:
            context: Stream context

        Yields:
            Dictionary objects representing records from the API
        """
        token = self.get_starting_replication_key_value(context)
        service_params = {
            "securityCode": self.config["security_code"],
            "token": token,
        }
        if self.name in [
            "changed_orders",
            "changed_parcels",
            "changed_purchases",
            "changed_suppliers",
            "changed_item_suppliers",
        ]:
            service_params["count"] = str(self.config.get(f"{self.name}_per_request", 500))
        elif self.name == "changed_stock":
            service_params["maxResult"] = str(self.config.get(f"{self.name}_per_request", 500))
        # For changed_items, do not add count or maxResult
        yield from self.get_records_with_pagination(
            service_name=self.service_name,
            context=context,
            **service_params,
        ) 