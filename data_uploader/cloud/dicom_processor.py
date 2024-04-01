# -*- coding: utf-8 -*-
"""This module performs data operations on DICOMs to prepare for sending via a WebTransport.
It has been taken from Loki, but modified to allow uploading multiple DICOMs as a study"""
import base64
import datetime
import io
from logging import getLogger
from os.path import basename, dirname
import traceback
from collections import defaultdict
from enum import Enum, auto, unique
from typing import Dict, List, Union
import tempfile

from pydicom import Dataset
from pydicom.encaps import defragment_data, encapsulate, fragment_frame
from pydicom.multival import MultiValue
from pydicom.uid import JPEG2000Lossless
import numpy as np
from glymur import Jp2k

logger = getLogger(basename(dirname(__file__)))


@unique
class DataType(Enum):
    """Enum to assign to each dicom field based on underlying type"""

    STR = auto()
    INT = auto()
    FLOAT = auto()
    UID = auto()
    BYTES = auto()
    MULTI = auto()
    ARRAY = auto()
    PN = auto()


function_mapping = defaultdict(
    lambda arg: arg,
    {
        DataType.STR: lambda arg: arg,
        DataType.UID: str,
        DataType.INT: int,
        DataType.FLOAT: float,
        DataType.MULTI: lambda arg: ",".join(str(v) for v in arg)
        if type(arg) == MultiValue
        else str(arg),
        DataType.PN: lambda arg: str(arg) if arg is not None else None,
    },
)


class _DataItem(object):
    __slots__ = [
        "address",
        "data_type",
    ]

    def __init__(self, address, data_type):
        self.address = address
        self.data_type = data_type

    def __call__(self, value):
        return None if value is None else function_mapping[self.data_type](value)


fields = {
    "rescaleIntercept": _DataItem(address=0x00281052, data_type=DataType.FLOAT),
    "rescaleSlope": _DataItem(address=0x00281053, data_type=DataType.FLOAT),
    "imageNumber": _DataItem(address=0x00200013, data_type=DataType.INT),
    "photometricInterpretation": _DataItem(address=0x00280004, data_type=DataType.STR),
    "samplesPerPixel": _DataItem(address=0x00280002, data_type=DataType.STR),
    "bodyPartExamined": _DataItem(address=0x00180015, data_type=DataType.STR),
    "imageInstanceUid": _DataItem(address=0x00080018, data_type=DataType.UID),
    "sopClassUid": _DataItem(address=0x00080016, data_type=DataType.UID),
    "patientId": _DataItem(address=0x00100020, data_type=DataType.STR),
    "studyDescription": _DataItem(address=0x00081030, data_type=DataType.STR),
    "accessionNumber": _DataItem(address=0x00080050, data_type=DataType.STR),
    "studyInstanceUid": _DataItem(address=0x0020000D, data_type=DataType.UID),
    "seriesNumber": _DataItem(address=0x00200011, data_type=DataType.INT),
    "seriesDescription": _DataItem(address=0x0008103E, data_type=DataType.STR),
    "seriesInstanceUid": _DataItem(address=0x0020000E, data_type=DataType.UID),
    "patientBirthDate": _DataItem(address=0x00100030, data_type=DataType.STR),
    "patientAge": _DataItem(address=0x00101010, data_type=DataType.STR),
    "patientBirthTime": _DataItem(address=0x00100032, data_type=DataType.STR),
    "patientSex": _DataItem(address=0x00100040, data_type=DataType.STR),
    "modalitySoftwareVersion": _DataItem(address=0x00181020, data_type=DataType.MULTI),
    "modality": _DataItem(address=0x00080060, data_type=DataType.STR),
    "model": _DataItem(address=0x00081090, data_type=DataType.STR),
    "manufacturer": _DataItem(address=0x00080070, data_type=DataType.STR),
    "bitsAllocated": _DataItem(address=0x00280100, data_type=DataType.INT),
    "bitsStored": _DataItem(address=0x00280101, data_type=DataType.INT),
    "highBit": _DataItem(address=0x00280102, data_type=DataType.INT),
    "rows": _DataItem(address=0x00280010, data_type=DataType.INT),
    "columns": _DataItem(address=0x00280011, data_type=DataType.INT),
    "acquisitionDate": _DataItem(address=0x00080022, data_type=DataType.STR),
    "acquisitionTime": _DataItem(address=0x00080032, data_type=DataType.STR),
    "institutionName": _DataItem(address=0x00080080, data_type=DataType.STR),
    "institutionAddress": _DataItem(address=0x00080081, data_type=DataType.STR),
    "institutionalDepartmentName": _DataItem(address=0x00081040, data_type=DataType.STR),
    "studyDate": _DataItem(address=0x00080020, data_type=DataType.STR),
    "studyTime": _DataItem(address=0x00080030, data_type=DataType.STR),
    "patientName": _DataItem(address=0x00100010, data_type=DataType.PN),
    "seriesDate": _DataItem(address=0x00080021, data_type=DataType.STR),
    "seriesTime": _DataItem(address=0x00080031, data_type=DataType.STR),
    "kvp": _DataItem(address=0x00180060, data_type=DataType.STR),
    "exposure": _DataItem(address=0x00181153, data_type=DataType.STR),
    "exposureIndex": _DataItem(address=0x00181411, data_type=DataType.STR),
    "deviationIndex": _DataItem(address=0x00181413, data_type=DataType.STR),
    "pixelSpacing": _DataItem(address=0x00280030, data_type=DataType.MULTI),
    "imagerPixelSpacing": _DataItem(address=0x00181164, data_type=DataType.MULTI),
    "spacialResolution": _DataItem(address=0x00181050, data_type=DataType.STR),
    "detectorType": _DataItem(address=0x00187004, data_type=DataType.STR),
    "grid": _DataItem(address=0x00181166, data_type=DataType.MULTI),
    "manufacturerAttribute": _DataItem(address=0x00080070, data_type=DataType.STR),
    "manufacturerModel": _DataItem(address=0x00081090, data_type=DataType.STR),
    "viewPosition": _DataItem(address=0x00185101, data_type=DataType.STR),
    "timezoneOffsetFromUtc": _DataItem(address=0x00180201, data_type=DataType.STR),
    "spatialResolution": _DataItem(address=0x00181050, data_type=DataType.FLOAT),
}


def remove_none(input_dict: dict) -> dict:
    """Removes all key,value pairs which have a value of None"""
    return {k: v for k, v in input_dict.items() if v is not None}


def local_compress(ds: Dataset) -> Dataset:
    """Compresses a given DICOM file to JPEG2000Lossless compression.
    Requires GDCM.

    Args:
        path (str): Path to DICOM to be converted
        output (str, optional): Path to store the output file. Defaults to 'output.dcm'.

    Returns:
        None if successful, str of traceback if unsuccessful
    """
    try:
        ds.decompress()
        with tempfile.NamedTemporaryFile() as f:
            pixel_array = ds.pixel_array
            # not sure if the j2c files ever have negative values but if they do Glymur's implementation of these don't cope
            if np.min(pixel_array) < 0:
                pixel_array -= np.min(pixel_array)

            type, bitsstored = (np.uint16, 16) if int(ds.BitsStored) > 8 else (np.uint8, 8)
            ds.BitsStored = bitsstored
            ds.BitsAllocated = bitsstored
            ds.HighBit = bitsstored - 1
            ds.is_decompressed = False
            Jp2k(f.name, data=pixel_array.astype(type))
            f.seek(0)
            ds.PixelData = encapsulate(list(fragment_frame(f.read())))

            # encapsulated data needs to be OB https://pydicom.github.io/pydicom/stable/_modules/pydicom/filewriter.html
            ds[(0x7FE0, 0x0010)].VR = 'OB'

            ds[(0x7FE0, 0x0010)].is_undefined_length = True
            ds.file_meta.TransferSyntaxUID = JPEG2000Lossless
            ds.PixelRepresentation = 0
            f.seek(0)
        return ds
    except:
        return traceback.format_exc()


class _result(object):
    __slots__ = [
        "value",
    ]

    def __init__(self, value):
        self.value = value


class DicomProcessor:
    """
    DicomProcessor

    The class abstracts away the DICOM information extraction from the PACS Integration Layer.
    This class also defines what gets extracted from the DICOM files.
    """

    def __init__(self, datasets: List[Dataset]):
        """Constructor takes in an absolute file path and creates a DicomProcessor object with
        the loaded dicom files as the member variable.

        Args:
        datasets (List of pydicom.Dataset objects): All pydicom.Datasets in this request. If multiple are supplied, they must all have the same StudyInstanceUID
        """
        self.dcms = datasets
        assert len({dcm.StudyInstanceUID for dcm in self.dcms}) == 1

    def get_value(self, data_item, dcm: Dataset = None):
        """Gets the value of data_element stored in the FIRST dicom file corresponding to the
        the address being requested by default. If dcm is populated this will extract from that
        dicom file instead.

        Args:
        dcm (pydicom.Dataset): DICOM file to extract value from
        """
        if dcm:
            return data_item(dcm.get(data_item.address, _result(None)).value)
        else:
            return data_item(self.dcms[0].get(data_item.address, _result(None)).value)

    def vision_request(self):
        """This method is to create a dictionary containing the vision request information"""
        ret = {
            "study": self._extract_study(),
            "series": self._extract_series(),
            "scan": self._extract_scan(),
            "images": self._extract_images(),
        }
        return ret

    def vision_study_complete_request(self):
        """This method is to create a dictionary containing the vision study complete request information.
        This method should be triggered when the HL7 message is received from the RIS indicating that
        a study is complete.
        """

        return {"study": self._extract_study()}

    def convert_date(self, date):
        """This method extracts the date of birth from the dicom file,
        and converts it into ISO 8601 format for date."""

        if date:

            try:
                return datetime.datetime.strftime(
                    datetime.datetime.strptime(date, "%Y%m%d"), "%Y-%m-%d"
                )
            except ValueError as error:
                logger.warning(f"Unable to convert Date {date}. Error raised was {error}.")
                raise

    def convert_time(self, time):
        """This method extracts the time from the dicom file,
        and converts it into ISO 8601 format for time."""

        if time:

            try:
                compare_string = "%H%M%S.%f" if "." in time else "%H%M%S"
                return datetime.datetime.strftime(
                    datetime.datetime.strptime(time, compare_string), "%H:%M:%S"
                )

            except ValueError as error:
                logger.warning(f"Unable to convert Time {time}. Error raised was {error}.")
                raise

    def calculate_patient_age(self):
        date_format = '%Y%m%d'
        if (
            (
                self.get_value(fields["patientAge"]) is None
                or self.get_value(fields["patientAge"]) == ""
            )
            and self.get_value(fields["studyDate"]) is not None
            and self.get_value(fields["patientBirthDate"]) is not None
        ):
            delta = datetime.datetime.strptime(
                self.get_value(fields["studyDate"]), date_format
            ) - datetime.datetime.strptime(self.get_value(fields["patientBirthDate"]), date_format)
            patient_age = str(delta.days // 365.25).zfill(3) + "Y"
        else:
            patient_age = self.get_value(fields["patientAge"])
        return str(patient_age)

    def _extract_scan(self):
        """Extracts the scan information from a dicom file, and returns a dict with
        the relevant scan associated metadata."""

        data = {}

        return remove_none(data)

    def _extract_series(self):
        """Extracts the series information from the dicom file, and returns the appropriate protobuf."""

        data = {
            "seriesInstanceUid": self.get_value(fields["seriesInstanceUid"]),
            "seriesNumber": self.get_value(fields["seriesNumber"]),
        }

        return remove_none(data)

    def _extract_study(self):
        """Extracts the study information from the dicom file, and returns a dict with
        the relevant study associated metadata.
        This works even with a list of DICOMS as these fields would all be the same for \
        the list if the DICOMs all belong to the same Study"""
        patient_age = self.calculate_patient_age()
        data = {
            "studyInstanceUid": self.get_value(fields["studyInstanceUid"]),
            "accessionNumber": self.get_value(fields["accessionNumber"]),
            "description": self.get_value(fields["studyDescription"]),
            "patientId": self.get_value(fields["patientId"]),
            "patientAge": patient_age,
        }

        return remove_none(data)

    def _extract_images(self) -> List[Dict]:
        """Extracts the image information from a list of dicom files, and returns a list of dicts with
        the relevant metadata and image information

        Returns:
            List[Dict]: List of dicts of DICOM images and metadata, in the order of self.dcms
        """

        ret = []
        for dcm in self.dcms:

            height = self.get_value(fields["rows"], dcm)
            width = self.get_value(fields["columns"], dcm)
            channels = self.get_value(fields["samplesPerPixel"], dcm)

            image: io.BytesIO()

            if dcm.file_meta.TransferSyntaxUID != JPEG2000Lossless:
                logger.info('Compressing data')
                dcm = local_compress(dcm)

            image = io.BytesIO()
            try:
                image.write(defragment_data(dcm.PixelData))
            except AttributeError:
                raise Exception(dcm)
            image.seek(0)

            data = {
                "imageInstanceUid": self.get_value(fields["imageInstanceUid"], dcm),
                "sopClassUid": self.get_value(fields["sopClassUid"], dcm),
                "height": height,
                "width": width,
                "rescaleSlope": self.get_value(fields["rescaleSlope"], dcm)
                if "RescaleSlope" in dcm
                else 1,
                "rescaleIntercept": self.get_value(fields["rescaleIntercept"], dcm)
                if "RescaleIntercept" in dcm
                else 0,
                "photometricInterpretation": self.get_value(
                    fields["photometricInterpretation"], dcm
                ),
                "samplesPerPixel": channels,
                "bitsAllocated": self.get_value(fields["bitsAllocated"], dcm),
                "bitsStored": self.get_value(fields["bitsStored"], dcm),
                "highBit": self.get_value(fields["highBit"], dcm),
                "data": base64.b64encode(image.read()).decode("ascii"),
                "spacialResolution": str(self.get_value(fields["spatialResolution"], dcm)),
                "timezoneOffsetFromUtc": self.get_value(fields["timezoneOffsetFromUtc"], dcm),
            }

            data = remove_none(data)
            ret.append(data)

        return ret
