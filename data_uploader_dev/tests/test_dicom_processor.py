#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `data_uploader` package."""

import json

import pytest
from data_uploader.cloud.dicom_processor import DicomProcessor, fields


def test_dicom_processor(test_dicom):
    with open('tests/assets/vision_request.json', 'r') as f:
        target = json.loads(f.read())

    dicom_processor = DicomProcessor(datasets=[test_dicom])
    vision_request = dicom_processor.vision_request()
    assert vision_request == target


def test_time_conversion(test_dicom):
    dicom_processor = DicomProcessor(datasets=[test_dicom])
    r = dicom_processor.convert_time(dicom_processor.get_value(fields["seriesTime"]))
    assert r == '15:36:56'
    test_dicom.SeriesTime = 'banana'
    dicom_processor = DicomProcessor(datasets=[test_dicom])
    with pytest.raises(ValueError):
        r = dicom_processor.convert_time(dicom_processor.get_value(fields["seriesTime"]))


def test_date_conversion(test_dicom):
    dicom_processor = DicomProcessor(datasets=[test_dicom])
    r = dicom_processor.convert_date(dicom_processor.get_value(fields["seriesDate"]))
    assert r == '2186-06-02'
    test_dicom.SeriesDate = 'banana'
    dicom_processor = DicomProcessor(datasets=[test_dicom])
    with pytest.raises(ValueError):
        r = dicom_processor.convert_date(dicom_processor.get_value(fields["seriesDate"]))
