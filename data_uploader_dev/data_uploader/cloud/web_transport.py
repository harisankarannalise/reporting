"""This module handles web requests with appropriate headers.
It has been taken form LOKI but modified"""

import json
import os
from time import time
from urllib.parse import urljoin, urlparse

from aiohttp import ClientSession
from requests import Response
from requests import get as http_get
from requests import post as http_post

from data_uploader_dev.data_uploader.cloud.request_signer import RequestSigner

HEADER_CONTENT_TYPE = "content-type"
HEADER_CLIENT_ID = "x-annalise-ai-client-id"
HEADER_TIMESTAMP = "x-annalise-ai-timestamp"

HEADER_SIGNATURE = "x-annalise-ai-signature"
HEADER_SIGNED_HEADERS = "x-annalise-ai-signed-headers"
HEADER_APP_VERSION = "x-annalise-ai-app-version"

METHOD_GET = "GET"
METHOD_POST = "POST"

CONTENT_TYPE_JSON = "application/json; charset=UTF-8"


class WebTransport:
    """Class allowing communication with Optimus with the appropriate headers"""

    def __init__(
        self,
        api_host,
        client_id,
        client_secret,
        timeout: int = 300,
        app_version='0.0.0.not-specified',
    ):
        """Initialises a web transport, setting a default timeout and app_version (which is required)

        Args:
            timeout (int, optional): Amount of time to timeout waiting for a response. Defaults to 300.
            app_version (str, optional): What version of the particular app that is communicating with Optimus. Defaults to '0.0.0.not-specified'.
        """
        self._session: ClientSession = None
        self.timeout = timeout
        self.app_version = app_version
        self.api_host = api_host
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_headers(self, url: str, method: str, params: dict, body="", faketime=None) -> dict:
        client_id = self.client_id
        client_secret = self.client_secret
        request_signer = RequestSigner()
        headers = {
            HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
            HEADER_CLIENT_ID: client_id,
            HEADER_TIMESTAMP: str(faketime) if faketime else f"{round(time() * 1000)}",
            HEADER_SIGNED_HEADERS: ";".join(request_signer.signed_headers()),
            HEADER_APP_VERSION: self.app_version,
        }
        canonical_request = request_signer.create_canonical_request(
            method=method,
            path=urlparse(url).path,
            params=params,
            headers=headers,
            body=body,
        )
        signature = request_signer.sign(
            client_secret=client_secret, canonical_request=canonical_request
        )
        headers[HEADER_SIGNATURE] = signature
        return headers

    def send(self, path="", data=None):
        url = urljoin(self.api_host, path)
        body = json.dumps(data, separators=(',', ':'))
        headers = self._get_headers(url=url, method=METHOD_POST, params={}, body=body)
        response = http_post(url=url, headers=headers, data=body, timeout=self.timeout)
        return response

    async def send_async(self, path="", data=None):
        url = urljoin(self.api_host, path)
        body = json.dumps(data, separators=(',', ':'))
        headers = self._get_headers(url=url, method=METHOD_POST, params={}, body=body)
        if self._session is None:
            self._session = ClientSession()
        response = await self._session.post(
            url=url, headers=headers, data=body, timeout=self.timeout
        )
        return response

    def get(self, path="", params: dict = None, faketime=None) -> Response:
        url = urljoin(self.api_host, path)
        headers = self._get_headers(url=url, method=METHOD_GET, params=params, faketime=faketime)
        response = http_get(url=url, headers=headers, params=params, timeout=self.timeout)
        return response
