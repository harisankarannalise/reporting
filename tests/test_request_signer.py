#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `data_uploader` package."""

import pytest

from data_uploader.cloud.request_signer import RequestSigner


@pytest.fixture
def request_signer():
    return RequestSigner()


def test_signing(request_signer):
    assert (
        request_signer.sign(
            client_secret="TESTSECRET",
            canonical_request='''GET
/v1/studies/2.25.20412548740337987099530620825665698323

x-annalise-ai-client-id:TESTID
x-annalise-ai-timestamp:1596689207731
x-annalise-ai-client-id;x-annalise-ai-timestamp
''',
        )
        == 'efbc77d65d4ba32abf67e47b6d78d1916d890f069d258c822d12ed62163ffdb1'
    )
