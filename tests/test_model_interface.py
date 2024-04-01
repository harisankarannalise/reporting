#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `data_uploader` package."""

import json
from unittest.mock import MagicMock, patch

import mock
import pydicom
import copy
import pytest
import requests
from requests import Response
from requests.exceptions import ConnectionError

from data_uploader import model_interface
from data_uploader.cloud import dicom_processor, web_transport
from data_uploader.model_interface import ModelInterface
from data_uploader.utils import utils
from tests.conftest import (
    MOCK_MODEL_RETURN,
    RAW_MODEL_RETURN,
    RAW_SEGMENT_ID_RETURN,
    RAW_SEGMENT_RETURN,
    mock_dcm,
    mock_dses,
)

"""
def test_model_interface(mock_dcm, mock_model_interface):
    dses = [mock_dcm]
    responses, accessions = mock_model_interface.bulk_upload_image(dses=dses, regenerate_uids=True)
    ret = mock_model_interface.bulk_get(accessions)
    #Optimus sometimes is down and gives COMPLETED_ERROR
    assert all(['COMPLETE' in v for k, v in ret[0]['classification']['status'].items()])
"""


def test_bulk_upload_image(mock_model_interface, mock_response, mock_dcm):
    ds = mock_dcm
    with patch.object(
        model_interface,
        'upload_images',
        return_value=(mock_response(), 'Banana', {'Banana': 'Bread'}, ds),
    ):
        responses, accessions, studyInstanceUid = mock_model_interface.bulk_upload_image(
            dses=[ds], regenerate_uids=True
        )
    assert len(responses) == 1
    assert len(accessions) == 1
    assert len(studyInstanceUid) == 1
    assert responses[0].status_code == 200
    assert accessions[0] == 'Banana'

    with patch.object(
        model_interface,
        'upload_images',
        return_value=(mock_response(123), 'Banana', {'Banana': 'Bread'}, ds),
    ):
        with pytest.raises(Exception) as e:
            responses, accessions = mock_model_interface.bulk_upload_image(
                dses=[ds], regenerate_uids=True
            )
            error_text = str(e.value)
            assert 'Bread' in error_text


@patch.object(web_transport, 'WebTransport')
@patch.object(dicom_processor, 'DicomProcessor')
def test_upload_image(mock_dicom_processor, mock_transport, mock_dcm, mock_response):
    mock_dicom_processor.vision_request.return_value = 1
    mock_transport.send.return_value = mock_response()

    res, accession, studyInstanceUid, ds = model_interface.upload_images(
        dses=[mock_dcm],
        transport=mock_transport,
        regenerate_uids=False,
        force_accession_equal_study_uid=True,
    )
    assert accession == mock_dcm.AccessionNumber
    assert studyInstanceUid == mock_dcm.StudyInstanceUID
    assert res.status_code == 200

    res, accession, studyInstanceUid, ds = model_interface.upload_images(
        dses=[copy.deepcopy(mock_dcm)], transport=mock_transport, regenerate_uids=True
    )
    assert accession != mock_dcm.AccessionNumber
    assert studyInstanceUid != mock_dcm.StudyInstanceUID
    assert res.status_code == 200

    # error_mock = mock.Mock()
    # error_mock.side_effect = ConnectionError
    with patch.object(model_interface, 'upload_image_helper', side_effect=ConnectionError):
        res, accession, studyInstanceUid, ds = model_interface.upload_images(
            dses=[mock_dcm], transport=mock_transport, regenerate_uids=True
        )
    assert res.status_code == 500


@patch.object(web_transport, 'WebTransport')
def test_get(mock_transport, mock_response):
    mock_transport.send.return_value = mock_response()
    res = model_interface.get(transport=mock_transport, accession='mock_accession')
    assert res['accession'] == 'mock_accession'
    assert res['classification'] == json.loads(mock_response().text)['studies'][0]
    assert res['segmentation'] == {}
    assert res['laterality'] == {}
    assert res['get_log'] is None

    mock_transport.send.return_value = mock_response(include_segment=True)
    with patch.object(
        model_interface, 'fetch_predicted_segments', side_effect=ConnectionError('Banana Bread')
    ):
        res = model_interface.get(transport=mock_transport, accession='mock_accession')
        assert res['accession'] == 'mock_accession'
        assert res['classification'] == 'ERROR'
        assert res['segmentation'] == 'ERROR'
        assert res['laterality'] == 'ERROR'
        assert 'Banana Bread' in res['get_log']

    mock_transport.send.return_value = mock_response(123)
    res = model_interface.get(transport=mock_transport, accession='mock_accession')
    assert res['accession'] == 'mock_accession'
    assert res['classification'] == 'ERROR'
    assert res['segmentation'] == 'ERROR'
    assert res['laterality'] == 'ERROR'
    assert res['get_log'] is not None

    mock_transport.send.return_value = mock_response(full_response=RAW_SEGMENT_ID_RETURN)
    mock_transport.get.return_value = mock_response(full_response=RAW_MODEL_RETURN)

    with patch.object(
        requests, 'get', return_value=mock_response(full_response=RAW_SEGMENT_RETURN)
    ):
        res = model_interface.get(transport=mock_transport, accession='study_1')
