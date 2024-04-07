"""This module create canonical request. It has been taken from Loki."""
from hashlib import sha256
from hmac import HMAC
from typing import List
from urllib.parse import quote

HEADER_CONTENT_TYPE = "content-type"
HEADER_CLIENT_ID = "x-annalise-ai-client-id"
HEADER_TIMESTAMP = "x-annalise-ai-timestamp"

ENCODE_UTF8 = "utf-8"

SEPARATOR_LINE = "\n"
SEPARATOR_HEADER = ";"
SEPARATOR_PARAMETER = "&"


class RequestSigner:
    """The RequestSigner class will handles all aspects of creation of canonical request"""

    def signed_headers(self) -> List[str]:
        """Default list of headers to sign

        Returns:
            Signed headers: list of headers to include in canonical request
        """
        return sorted([HEADER_TIMESTAMP, HEADER_CLIENT_ID])

    def create_canonical_body(self, body="") -> str:
        return sha256(body.encode(ENCODE_UTF8)).hexdigest()

    def create_canonical_headers(self, headers: dict) -> str:
        """Create canonical headers

        Args:
            headers: the headers dict

        Returns:
            Canonical headers: a static representation of headers as a str
        """
        signed_headers = sorted(filter(lambda key: key in self.signed_headers(), headers.keys()))

        return SEPARATOR_LINE.join(
            [f"{key}:{' '.join(headers[key].split())}" for key in signed_headers]
        )

    def create_canonical_params(self, params: dict) -> str:
        """Create canonical params

        Args:
            params: the params dict

        Returns:
            Canonical params: a static representation of params as a str
        """
        safe = "~()*!.'"
        if not params:
            return ''
        return SEPARATOR_PARAMETER.join(
            [
                f"{quote(key, safe=safe)}={quote(params[key], safe=safe)}"
                for key in sorted(params.keys())
            ]
        )

    def create_canonical_request(
        self, method: str, path: str, params: dict, headers: dict, body=""
    ) -> str:
        """Create a canonical request

        Args:
            method: request method
            path: request path without the scheme nor authority
            headers: request headers
            params: request query params
            body: the request body as str

        Returns:
            Canonical request: a static representation of the request as str.
        """

        return SEPARATOR_LINE.join(
            [
                method.upper(),
                path,
                self.create_canonical_params(params),
                self.create_canonical_headers(headers),
                SEPARATOR_HEADER.join(self.signed_headers()),
                self.create_canonical_body(body) if len(body) else body,
            ]
        )

    def sign(self, client_secret: str, canonical_request: str) -> str:
        """Sign a canonical request using client secret

        Args:
            client_secret: secret to add to signature
            canonical_request: request to sign

        Returns:
            signature: an unique encrypted str of a request.
        """
        return HMAC(
            key=bytearray(client_secret.encode(ENCODE_UTF8)),
            msg=canonical_request.encode(ENCODE_UTF8),
            digestmod=sha256,
        ).hexdigest()
