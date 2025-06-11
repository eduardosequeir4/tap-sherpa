"""Custom client handling for Sherpa API."""

from __future__ import annotations

import typing as t
from zeep import Client, Settings
from zeep.transports import Transport
from requests import Session
from zeep.helpers import serialize_object
import logging

# Set up logging: only show warnings or above for zeep and its submodules
logging.basicConfig(level=logging.INFO)
logging.getLogger("zeep").setLevel(logging.WARNING)
logging.getLogger("zeep.transports").setLevel(logging.WARNING)
logging.getLogger("zeep.xsd.schema").setLevel(logging.WARNING)
logging.getLogger("zeep.wsdl.wsdl").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class SherpaClient:
    """SOAP client for Sherpa API."""

    def __init__(
        self,
        wsdl_url: str,
        tap: "TapSherpa",
        timeout: int = 30,
    ) -> None:
        """Initialize the Sherpa SOAP client.

        Args:
            wsdl_url: The WSDL URL for the Sherpa SOAP service
            tap: The tap instance to get configuration from
            timeout: Request timeout in seconds
        """
        self.wsdl_url = wsdl_url
        session = Session()
        # Set default Content-Type header
        session.headers.update({
            "Content-Type": "application/soap+xml; charset=utf-8"
        })
        transport = Transport(session=session, timeout=timeout)
        settings = Settings(strict=False)
        self.client = Client(
            wsdl_url,
            transport=transport,
            settings=settings,
        )
        self.tap = tap
        self.session = session  # Save session for dynamic header updates

    def get_curl_command(self, service_name: str, params: dict, stream_name: str = None) -> str:
        """Generate a curl command for the SOAP request.

        Args:
            service_name: Name of the SOAP service method to call
            params: Parameters to pass to the service method
            stream_name: Name of the stream (for stream-specific token)

        Returns:
            String containing the curl command
        """
        # Add authentication parameters
        params = {
            "securityCode": self.tap.config["security_code"],
            **params
        }

        # Convert all parameters to strings
        params = {k: str(v) for k, v in params.items()}

        # Create the SOAP envelope
        soap_envelope = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<soap:Envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\n  <soap:Body>\n    <{service_name} xmlns=\"http://sherpa.sherpaan.nl/\">\n{chr(10).join(f'      <{k}>{v}</{k}>' for k, v in params.items())}\n    </{service_name}>\n  </soap:Body>\n</soap:Envelope>"""

        # Generate the curl command
        curl_cmd = f"""curl -X POST \\\n  '{self.wsdl_url.replace("?wsdl", "")}' \\\n  -H 'Content-Type: application/soap+xml; charset=utf-8' \\\n  -H 'SOAPAction: \"http://sherpa.sherpaan.nl/{service_name}\"' \\\n  -d '{soap_envelope}'"""

        return curl_cmd

    def call_service(self, service_name: str, stream_name: str = None, **kwargs) -> dict:
        """Call a SOAP service method.

        Args:
            service_name: Name of the SOAP service method to call
            stream_name: Name of the stream (for stream-specific token)
            **kwargs: Arguments to pass to the service method

        Returns:
            Response from the SOAP service
        """
        # Add authentication parameters to all requests
        kwargs["securityCode"] = self.tap.config["security_code"]

        # Ensure all parameters are strings
        kwargs = {k: str(v) for k, v in kwargs.items()}

        # Set SOAPAction header dynamically for this call
        soap_action = f'"http://sherpa.sherpaan.nl/{service_name}"'
        self.session.headers["SOAPAction"] = soap_action

        # Get the service and create the operation
        service = self.client.create_service(
            '{http://sherpa.sherpaan.nl/}SherpaServiceSoap12',
            'https://sherpaservices-tst.sherpacloud.eu/214/Sherpa.asmx'
        )
        
        # Call the service method (no _http_headers)
        method = getattr(service, service_name)
        result = method(**kwargs)
        return serialize_object(result)

    def get_changed_items(self, token: int) -> list:
        """Get changed items from the API.

        Args:
            token: The token to use for pagination

        Returns:
            List of changed items
        """
        result = self.call_service("ChangedItems", token=token)
        if not result or "ChangedItemsResult" not in result:
            return []
            
        response = result["ChangedItemsResult"]
        if not response or "ResponseValue" not in response:
            return []
            
        items = response["ResponseValue"].get("ItemCodeToken", [])
        if not isinstance(items, list):
            items = [items]
            
        return items
