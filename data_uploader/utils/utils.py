import hashlib
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import pydicom
import shortuuid
import csv
from os import walk, path
from PIL import Image
from logging import getLogger
from os.path import dirname, basename
from collections import defaultdict


from data_uploader.utils.constants import DCM_HEADERS

logger = getLogger(basename(dirname(__file__)))


class UploadMap:
    def __init__(
        self,
        original_accession,
        original_patientid,
        original_studyinstanceuid,
        original_seriesinstanceuid,
        original_sopinstanceuid,
        new_accession,
        new_patientid,
        new_studyinstanceuid,
        new_seriesinstanceuid,
        new_sopinstanceuid,
    ):
        self.original_accession = original_accession
        self.original_patientid = original_patientid
        self.original_studyinstanceuid = original_studyinstanceuid
        self.original_seriesinstanceuid = original_seriesinstanceuid
        self.original_sopinstanceuid = original_sopinstanceuid
        self.new_accession = new_accession
        self.new_patientid = new_patientid
        self.new_studyinstanceuid = new_studyinstanceuid
        self.new_seriesinstanceuid = new_seriesinstanceuid
        self.new_sopinstanceuid = new_sopinstanceuid
        self.response_status_code = ""
        self.response_reason = ""
        self.uploaded_at = ""

    def get_csv_row(self):
        return [
            self.original_accession,
            self.original_patientid,
            self.original_studyinstanceuid,
            self.original_seriesinstanceuid,
            self.original_sopinstanceuid,
            self.new_accession,
            self.new_patientid,
            self.new_studyinstanceuid,
            self.new_seriesinstanceuid,
            self.new_sopinstanceuid,
            self.response_status_code,
            self.response_reason,
            self.uploaded_at,
        ]

    def get_csv_headers():
        return [
            "Original AccessionNumber",
            "Original PatientId",
            "Original StudyInstanceUID",
            "Original SeriesInstanceUID",
            "Original SOPInstanceUID",
            "New AccessionNumber",
            "New PatientId",
            "New StudyInstanceUID",
            "New SeriesInstanceUID",
            "New SOPInstanceUID",
            "Response Status Code",
            "Response Reason",
            "Uploaded At",
        ]

    def write_to_file(file, internal_maps):
        with open(file, "w", newline="") as output_csv:
            csv_writer = csv.writer(output_csv, dialect="excel")
            csv_writer.writerow(UploadMap.get_csv_headers())
            for map in internal_maps:
                csv_writer.writerow(map.get_csv_row())


def recursive_replace(obj, string_map: Dict):
    '''
    checks through an obj, replaces any strings that match any keys in string_map with the mapped value
    Recursively checks keys and values of dictionaries and every item in a list
    Leaves everything else otherwise untouched, return a copy.
    '''
    if type(obj) is str:
        if obj in string_map:
            return string_map[obj]
    elif type(obj) is dict:
        ret = {}
        for key, val in obj.items():
            ret[recursive_replace(key, string_map)] = recursive_replace(val, string_map)
        return ret
    elif type(obj) is list:
        ret = []
        for val in obj:
            ret.append(recursive_replace(val, string_map))
        return ret
    return obj


def get_dicom_headers(dicoms: List[bytes]) -> List[Dict[str, Any]]:
    """Produces dicts of relevant DICOM headers from a list of DICOMs.
    Attempts to Calculate PatientAge from PatientBirthDate and StudyDate if PatientAge is not present.

    Args:
        dicoms (List[bytes]): List of pydicom Datasets as bytes to extract headers from.

    Returns:
        List[Dict[str, Any]]: List of dicts with keys as the DICOM header name, and the values as the DICOM header value.
        Order of the output list reflects the order of the input list
    """
    date_format = '%Y%m%d'
    headers = []
    for image in dicoms:

        ds = pydicom.dcmread(BytesIO(image), stop_before_pixels=True)
        current_headers = {
            header: ds.data_element(header).value
            if header in ds and ds.data_element(header).value != ''
            else None
            for header in DCM_HEADERS
        }
        if (
            current_headers['PatientAge'] is None
            and current_headers['StudyDate'] is not None
            and current_headers['PatientBirthDate'] is not None
        ):
            delta = datetime.strptime(
                current_headers['StudyDate'], date_format
            ) - datetime.strptime(current_headers['PatientBirthDate'], date_format)
            current_headers['PatientAge'] = delta.days // 365.25
        headers.append(current_headers)
    return headers


class NonBinarySeries(pd.Series):
    """Helper class that subclasses a Pandas Series to allow deriving labels for non-binary labels (e.g. for a Likert Scale)"""

    def __and__(self, other):
        return NonBinarySeries(np.minimum(self, other))

    def __or__(self, other):
        return NonBinarySeries(np.maximum(self, other))

    def __invert__(self):
        return NonBinarySeries(4 - self)


def parse_str(string) -> str:
    """Helper function that translates boolean logic in a derived label definition into a Pandas-executable query

    Args:
        string (str): The query as a string

    Returns:
        str: [description]
    """
    return (
        string.replace(" or ", " | ")
        .replace(" and ", " & ")
        .replace("not ", " ~ ")
        .replace(" not", " ~ ")
    )


def decorate_token(string: str, df: pd.DataFrame) -> str:
    """Helper function that replaces all mentions of dataframe columns into references to the NonBinarySeries version of that column.
    This effectively ranslates a derived label definition into a query involving NonBinarySeries for non-binary classifications.

    Args:
        string (str):  The query as a string
        df (pd.DataFrame: The dataframe to apply the query to

    Returns:
        str: The translated query which can be eval()'ed
    """
    for col in sorted(df.columns, key=len, reverse=True):
        if string == col:
            return string.replace(col, f' NonBinarySeries(df.{col})')
        string = string.replace(' ' + col, f' NonBinarySeries(df.{col})')
        string = string.replace(col + ' ', f'NonBinarySeries(df.{col}) ')
    return string


def python_derive(string: str, df: pd.DataFrame) -> pd.Series:
    """Executes a derived label query for non-binary classifications.

    Args:
        string (str): The query to be executed
        df (pd.DataFrame): The dataframe to perform the query on

    Returns:
        pd.Series: The derived label
    """

    s = decorate_token(parse_str(string), df)
    return eval(s)


def derive_labels(orig_df: pd.DataFrame, derived_label_keys: Dict[str, str]) -> pd.DataFrame:
    """Generate dervied labels from existing label_keys

    Arguments:
        derived_label_keys {Dict[str, str]} -- {derived_label_key:python satement on label_key}
        eg. {"bullae" : "bullae_upper or bullae_lower or bullae_diffuse"}

    Returns:
        pd.DataFrame -- Pandas dataframe with new derived labels as new columns
    """

    original_index_name = orig_df.index.name
    if original_index_name is None:
        original_index_name = orig_df.index.names
    df = orig_df.copy().reset_index()
    for key, query in derived_label_keys.items():
        if query:
            # query_index = df.query(query, engine='python').index
            # df[key] = df.index.isin(query_index)
            df[key] = python_derive(query, df)
    return df.set_index(original_index_name).astype(int)


def generate_md5_sum(data: bytes) -> str:
    """Helper function to generate md5 sum for a given bytestring
    We assume all the bytes are stored in memory and do not need to be chunked

    Args:
        data (bytes): Bytes to compute the md5 sum for

    Returns:
        str: The md5 sum as a string
    """
    return hashlib.md5(data).hexdigest()


def get_files_in_directory_with_extension(extension: str, search_directory: str) -> List[str]:
    """Returns all files in folder (including nested files) that match the file extension
    Args:
        extension (str): File extension to match on
        search_directory (str): Path to find files in

    Returns:
        List[str]: List of full file paths that match search inputs
    """
    files_to_open = []
    for root, _, files in walk(search_directory, topdown=True, onerror=None):
        files_to_open.extend(
            path.join(root, filename)
            for filename in files
            if filename.lower().endswith(extension.lower())
        )
    return files_to_open


def get_dses_grouped_by_field(
    files_to_open: List[str], group_by: str
) -> Dict[str, List[pydicom.dataset.Dataset]]:
    """Takes in a list of file paths, reads each file as a pydicom dataset and groups them
    by the specified field
    Args:
        files_to_open (List[str]): List of file paths
        group_by (str): Name of pydicom dataset field as specified in https://github.com/pydicom/pydicom/blob/master/pydicom/_dicom_dict.py

    Returns:
        Dict[List[pydicom.dataset.Dataset]]: A dictionary where the keys are the unique values of the field specified and the values
        are a list of pydicom Datasets with matching values
    """
    file_details = defaultdict(list)
    # Load each dicom and group by StudyInstanceUid
    for file_to_open in files_to_open:
        try:
            ds = pydicom.read_file(file_to_open)
            file_details[getattr(ds, group_by)].append(ds)
        except:
            print(f"Couldn't read {file_to_open} as dicom")
    return file_details


def png_to_dicom(file_to_open: str) -> pydicom.dataset.Dataset:
    """Converts an 8-bit png file into a pydicom.dataset object by using a known good
    dicom file to bootsrap.
    Args:
        file_to_open (str): File path to the source 8-bit png

    Returns:
        pydicom.dataset.Dataset: dataset object containing converted png
    """
    ds = pydicom.dcmread("good_dicom.dcm")
    im_frame = Image.open(file_to_open)
    # Ensure that the image is a single channel (e.g. not RGB, RGBA, LA)
    if im_frame.mode != 'L':
        im_frame = im_frame.convert('L')

    # (8-bit pixels, black and white)
    np_frame = np.array(im_frame.getdata(), dtype=np.uint8)
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.SamplesPerPixel = 1
    ds.RescaleSlope = 1
    ds.RescaleIntercept = 0
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    ds.BitsStored = 8
    ds.BitsAllocated = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.SpatialResolution = "1"
    ds.Rows = im_frame.height
    ds.Columns = im_frame.width
    ds.PixelData = np_frame.tobytes()
    return ds


def update_uids(
    dses: List[pydicom.dataset.Dataset],
    regenerate_all_uids: bool,
    match_accession_study: bool,
    internal_maps: List[UploadMap] = [],
) -> Tuple[List[pydicom.dataset.Dataset], dict]:
    """Generates new dicom compliant UIDs (StudyInstanceUid, SeriesInstanceUid, SopInstanceUid, AccessionNumber, PatientId)
    for a list of pydicom Dataset objects. All objects in the list will have the same StudyInstanceUid, AccessionNumber, PatientId.
    Args:
        dses (List[pydicom.dataset.Dataset]): List of objects to generate new UIDs for.
        regenerate_all_uids:
        match_accession_study: Sets the accession number and the studyInstanceUID to be the same (note: accessionNumber will exceed length allowed by dicom standard)
    Returns:
        List[pydicom.dataset.Dataset]: The list of objects with new UIDs.
        dict: Maps the original uids to the new uids
    """

    new_patientid = shortuuid.uuid()[:16].upper() if regenerate_all_uids else dses[0].PatientID
    new_studyinstanceuid = (
        pydicom.uid.generate_uid() if regenerate_all_uids else dses[0].StudyInstanceUID
    )
    if match_accession_study:
        new_accession = new_studyinstanceuid
    elif regenerate_all_uids:
        new_accession = shortuuid.uuid()[:16].upper()
    else:
        new_accession = dses[0].AccessionNumber

    for dse in dses:
        new_sopinstanceuid = (
            pydicom.uid.generate_uid() if regenerate_all_uids else dse.SOPInstanceUID
        )
        new_seriesinstanceuid = (
            pydicom.uid.generate_uid() if regenerate_all_uids else dse.SeriesInstanceUID
        )

        internal_maps.append(
            UploadMap(
                getattr(dse, "AccessionNumber", ""),
                getattr(dse, "PatientID", ""),
                dse.StudyInstanceUID,
                dse.SeriesInstanceUID,
                dse.SOPInstanceUID,
                new_accession,
                new_patientid,
                new_studyinstanceuid,
                new_seriesinstanceuid,
                new_sopinstanceuid,
            )
        )
        dse.AccessionNumber = new_accession
        dse.StudyInstanceUID = new_studyinstanceuid
        dse.PatientID = new_patientid
        dse.SOPInstanceUID = new_sopinstanceuid
        dse.SeriesInstanceUID = new_seriesinstanceuid

    return dses
