#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from http.client import HTTPException, RemoteDisconnected
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import base64
import pydicom
import requests
from requests import Response
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout, Timeout
from tqdm.auto import tqdm
from urllib3.exceptions import ProtocolError

from data_uploader.cloud.dicom_processor import DicomProcessor
from data_uploader.cloud.web_transport import WebTransport
from data_uploader.utils import utils
from data_uploader.utils.example import Example

REMOTE_ERRORS = (
    ConnectionError,
    RemoteDisconnected,
    ProtocolError,
    HTTPException,
    Timeout,
    ReadTimeout,
    ConnectTimeout,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name) -14s - %(levelname) -8s - %(message)s',
    datefmt='%H:%M:%S %d/%m/%y ',
)


def upload_images(
    dses: List[pydicom.Dataset],
    transport: WebTransport,
    regenerate_uids: bool = False,
    force_accession_equal_study_uid: bool = False,
    internal_maps: List[utils.UploadMap] = [],
) -> Tuple[requests.Response, str, str, dict, List[pydicom.Dataset]]:
    """Performs upload of a list of DICOM files, all of which must have the same StudyInstanceUID.
    If regenerate_uids is True, recreates unique UIDs for study, series, and SOP for the DICOM
    NB currently, accession number is set to be the same as the (regenerated) StudyUID

    Args:
        ds (pydicom.Dataset): DICOM file to upload
        transport (WebTransport): Transport object to access the cloud service
        salt (str, optional): Salt for pydicom.uid.generate_uid. Defaults to ''.
        regenerate_uids (bool, optional): Whether or not to regenerate Study, Series and SOP UIDs. Defaults to False.

    Returns:
        Tuple[requests.Response, str, Dict, pydicom.Dataset]: A tuple containing the response and DICOM information.
        In order, contains, the response, accession number, a dictionary mapping the UIDs of the uploaded DICOM to the original UIDs
        , and finally the pydicom Dataset that was actually uploaded (with regenerated UIDs if requested).
    """
    # When calling upload image all dicoms must be of the same study instance uid and accession (if present)
    if (
        len(set([ds.StudyInstanceUID for ds in dses])) != 1
        or len(set([getattr(ds, "AccessionNumber", "empty") for ds in dses])) != 1
    ):
        raise ValueError('Input datasets contained more than 1 StudyInstanceUID or AccessionNumber')

    dses = utils.update_uids(
        dses, regenerate_uids, force_accession_equal_study_uid, internal_maps=internal_maps
    )

    dicom_processor = DicomProcessor(datasets=dses)
    vision_request = dicom_processor.vision_request()
    response = None
    try:
        response = upload_image_helper(data=vision_request, transport=transport)
        if response.status_code == 200:
            logging.info("Upload succeeded - triggering inference")
            response = study_predict_helper(
                studyInstanceUid=dses[0].StudyInstanceUID, transport=transport
            )
            logger.info(f"AccessionNumber={dses[0].AccessionNumber} prediction sent")
        else:
            raise HTTPException(f"Image upload failed with status_code:{response.status_code}")
    except REMOTE_ERRORS as err:
        logging.warning(
            f'All 3 attempts uploading Study {dses[0].StudyInstanceUID} failed due to {err}'
        )
        if response is not None:
            logging.warning(response)
        else:
            response = Response()
            response.status_code = 500

    for map in [m for m in internal_maps if m.new_studyinstanceuid == dses[0].StudyInstanceUID]:
        map.response_status_code = response.status_code
        map.response_reason = response.reason
        map.uploaded_at = datetime.now()

    return response, dses[0].AccessionNumber, dses[0].StudyInstanceUID, dses


def retry(retries=10, pass_retry_counter_to_inner=False):
    """Decorator Function for a retry loop

    Args:
        retries (int, optional): Number of retries to attempt before raising an error. Defaults to 10.
        pass_retry_counter_to_inner (bool, optional): Whether to pass the retry_counter dict to the inner function for custom retry logic. Defaults to False.

    Raises:
        ConnectionError: If any more specific connection errors are raised, specified in REMOTE_ERRORS

    Returns:
        Decorated Function
    """
    retry_counter = {'retries': retries}

    def decorator(f):
        def inner(*args, **kwargs):
            while retry_counter['retries']:
                try:
                    if pass_retry_counter_to_inner:
                        ret = f(retry_counter=retry_counter, *args, **kwargs)
                    else:
                        ret = f(*args, **kwargs)
                    retry_counter['retries'] = retries
                    return ret
                except REMOTE_ERRORS:
                    retry_counter['error'] = traceback.format_exc()
                    retry_counter['retries'] -= 1
                    time.sleep(1)
            raise ConnectionError(retry_counter['error'])

        return inner

    return decorator


@retry(retries=3)
def upload_image_helper(data: bytes, transport: WebTransport) -> requests.Response:
    """Uploads a single request to the /v1/images/upload endpoint

    Args:
        data (bytes): The request to upload
        transport (WebTransport): The WebTransport object to communicate with the model server

    Returns:
        requests.Response: The response of the communication with the model server
    """
    response = transport.send(path='/v1/images/upload', data=data)
    return response


@retry(retries=3)
def study_predict_helper(studyInstanceUid: str, transport: WebTransport) -> requests.Response:
    """Makes a request the /v1/studies/predict endpoint

    Args:
        data (bytes): The request to upload
        transport (WebTransport): The WebTransport object to communicate with the model server

    Returns:
        requests.Response: The response of the communication with the model server
    """
    data = {"studyInstanceUid": studyInstanceUid, "triggeredAt": datetime.now().isoformat()}
    logging.info(data)
    response = transport.send(path='/v1/studies/predict', data=data)
    logging.info(response)
    return response


@retry(retries=10, pass_retry_counter_to_inner=True)
def fetch_model_response(
    retry_counter: Dict,
    accession: str,
    transport: WebTransport,
    start_time: datetime,
    get_timeout: int = 300,
) -> Dict:
    """Fetches the model response for a single accession

    Args:
        retry_counter (Dict): Retry Counter objects from retry decorator for custom logic
        accession (str): Accession of the study to fetch
        transport (WebTransport): WebTransport object to communicate with the model server
        start_time (datetime.datetime): Start_time to compare to for Timeout errors
        get_timeout (int, optional): Timeout for studies stuck on "PENDING" forever. Defaults to 300.

    Raises:
        Timeout: Timeout if study stuck on pending for get_timeout seconds
        ConnectionError: If fails to access the study accession at the endpoint

    Returns:
        Dict: Raw model response JSON dict
    """
    logger.debug(f"Making filter request for {accession}")
    resp = transport.send(path=f'/v1/studies/filter', data={'accessionNumber': accession})
    if resp.status_code == 200:
        logger.debug(f"Filter request for {accession} OK")
        studies_list = json.loads(resp.text)['studies']
        if len(studies_list) == 0:
            logger.debug(f"No studies found for {accession}")
            if (datetime.now() - start_time).seconds > get_timeout:
                raise Timeout(
                    f'/v1/studies/filter with accession {accession} failed: Accession not yet available > {round(get_timeout/60,1)} min\n'
                    + resp.text
                )
            retry_counter['retries'] = 10
        else:
            # TODO: This should use the studyinstanceuid, not just assume the first
            classification_ret = studies_list[0]
            if (
                classification_ret['status']['vision'] == 'PENDING'
                or classification_ret['status']['vision'] == 'IN_PROGRESS'
            ):
                if (datetime.now() - start_time).seconds > get_timeout:
                    raise Timeout(
                        f'/v1/studies/filter with accession {accession} failed: PENDING > {round(get_timeout/60,1)} min\n'
                        + resp.text
                    )
                retry_counter['retries'] = 10
                # reset count if stuff is still in pending or in progress
            if (
                classification_ret['status']['vision'] != 'PENDING'
                and classification_ret['status']['vision'] != 'IN_PROGRESS'
            ):
                return classification_ret
    raise ConnectionError(f'/v1/studies/filter failed with accession {accession}\n' + resp.text)


@retry(retries=100)
def fetch_predicted_segments(
    classification_ret: Dict, transport: WebTransport, accession: Optional[str] = None
) -> Dict:
    """Fetches the list of segment URLS for a given accession

    Args:
        accession (str, Optional): Accession to pass to logging if fails
        classification_ret (Dict): The model response JSON dict to extract segment requests from
        transport (WebTransport): WebTransport object to communicate with the model server

    Raises:
        ConnectionError: If fails to access the study segments at the segmentation endpoint

    Returns:
        Dict: Segmentation results JSON dict
    """
    resp = transport.send(
        path='/v1/segments/status',
        data={'findingsIds': [classification_ret['findings']['vision']['id']]},
    )
    if resp.status_code == 200:
        return json.loads(resp.text)

    raise ConnectionError(f'/v1/segments/status failed for study {accession}\n' + resp.text)


@retry(retries=10)
def fetch_segment_mask(url: str) -> bytes:
    """Fetches a single segment mask from a provided URL

    Args:
        url (str): URL pointing towards segmentation PNG data

    Raises:
        ConnectionError: If the segmentation data does not exist at the given URL

    Returns:
        bytes: PNG bytes of the segmentation mask
    """
    resp = requests.get(url)
    if resp.text.startswith('<?xml'):
        raise ConnectionError(
            f'{resp.text} error was raised while retrieving segment_mask from {url}'
        )
    return base64.b64encode(resp.content).decode("ascii")


def get(accession: str, transport: WebTransport, get_timeout: int = 300) -> Dict[str, Any]:
    """Fetches model predictions for the provided study_uid, and performs basic parsing of the results.

    Args:
        accession (str): The accession to fetch the results for
        transport (WebTransport): Transport object to access the cloud service
        get_timeout (int, optional): Length of time to give up after indefinitely waiting for a 'PENDING' study

    Raises:
        Exception: If accessing the study predictions fails
        Exception: If accessing the predicted segmentation masks fails
        ConnectionError, RemoteDisconnected, ProtocolError, HTTPException, Timeout, ReadTimeout, ConnectTimeout as appropriate

    Returns:
        Dict[str, Any]: A dict with five keys: accession (containing the accession requested),
        and classification, segmentation, and laterality (containing the model predictions of that type),
        as well as get_log (containing a description of any errors encountered during the get process).
    """

    get_log = None

    for attempt in range(3):
        # Fetch return from Optimus
        try:
            logger.debug("attempt {attempt}")
            classification_ret = fetch_model_response(
                accession=accession,
                transport=transport,
                get_timeout=get_timeout,
                start_time=datetime.now(),
            )
        except REMOTE_ERRORS as e:
            logger.error(e)
            tb = f'Attempt {attempt} encountered error: {traceback.format_exc()}'
            get_log = tb if get_log is None else get_log + tb
            continue
        segment_id_to_label = {}
        laterality = {}
        segmentation_data = {}
        if 'vision' not in classification_ret['findings']:
            classification_ret = segmentation_data = laterality = get_log = classification_ret[
                'errors'
            ]['vision']
            break

        # Dissect return to tease out segmentation and laterality
        for image in classification_ret['findings']['vision']['images']:
            for segment in image['segments']:
                if 'laterality' in segment:
                    uid = image['imageInstanceUid']
                    if uid not in laterality:
                        laterality[uid] = {}
                    laterality[uid][segment['label']] = segment['laterality']
                else:
                    segment_id_to_label[segment['id']] = segment['label']

        # Only bother fetching segmentation results if there are segmentation results to fetch
        if len(segment_id_to_label):
            try:
                segment_id_ret = fetch_predicted_segments(
                    accession=accession, classification_ret=classification_ret, transport=transport
                )
            except REMOTE_ERRORS as e:
                tb = f'Attempt {attempt} encountered error: {traceback.format_exc()}'
                get_log = tb if get_log is None else get_log + tb
                continue

            # Get the URLs of the segments
            to_retrieve = []
            for finding_segment in segment_id_ret['findingsSegments']:
                if 'segments' in finding_segment:
                    for segment in finding_segment['segments']:
                        to_retrieve.append(
                            (segment['id'], segment['imageInstanceUid'], segment['url'])
                        )

            # Actually get the segmentation masks
            segmentation_data = {}
            for segment_id, uid, url in to_retrieve:
                if uid not in segmentation_data:
                    segmentation_data[uid] = {}
                segment_label = segment_id_to_label[segment_id]
                try:
                    segmentation_data[uid][segment_label] = fetch_segment_mask(url)
                except REMOTE_ERRORS as e:
                    tb = f'Attempt {attempt} encountered error: {traceback.format_exc()}'
                    get_log = tb if get_log is None else get_log + tb
                    continue
        break
    else:
        classification_ret = segmentation_data = laterality = 'ERROR'

    return {
        'accession': accession,
        'classification': classification_ret,
        'segmentation': segmentation_data,
        'laterality': laterality,
        'get_log': get_log,
    }


@retry(retries=3)
def get_study_details(transport: WebTransport) -> requests.Response:
    response = transport.get(path='/v1/studies', params=None)
    # print(f'Response: {response.json()}')
    # with open('output/studies_response.json', 'w+') as json_file:
    #     json_file.write(json.dumps(response.json()))
    return response


class ModelInterface:
    """Class to interact with the model API.
    Performs uploading of DICOMs, intitation of prediction, fetching of results, and parsing of results.
    """

    def __init__(
        self,
        api_host,
        client_id,
        client_secret,
        max_workers: int = 4,
        wait_time: int = 20,
        disable_tqdm: bool = False,
        http_timeout: int = 300,
        log_file: str = 'data_uploader.log',
        force_accession_equal_study_uid: bool = False,
        app_version: str = '0.0.0.not_specified',
    ) -> None:
        """Instantiates a ModelInterface.

        Args:
            max_workers (int, optional): Maximum number of threads for bulk cloud operations. Defaults to 4.
            wait_time (int, optional): Time in seconds to wait between uploads in serial upload mode. Defaults to 20.
            disable_tqdm (bool, optional): Whether or not to disable the individual TQDM progress bars (upload and fetch). Defaults to False.
            http_timeout (int, optional): Time in seconds before the WebTransport times out sending/getting from the server. Defaults to 300.
            force_accession_equal_study_uid (bool, optional). Whether to force the uploaded DICOM accession to be equal to the DICOM Study UID. Defaults to False.
            log_file (str, optional): Log file to log to. Defaults to data_uploader.log.
        """
        self.transport = WebTransport(
            api_host, client_id, client_secret, timeout=http_timeout, app_version=app_version
        )
        self.max_workers = max_workers
        self.wait_time = wait_time
        self.internal_maps = []
        self.disable_tqdm = disable_tqdm
        self.http_timeout = http_timeout
        self.last_salt = None
        self.force_accession_equal_study_uid = force_accession_equal_study_uid

        logger.info('ModelInterface instantiated')

    def create_gen(self, dses: List, upload_by_study: bool) -> Generator:
        """Creates the appropriately initialised generator

        Args:
            dses (List): List of either DICOMs or Example objects
            upload_by_study (bool): Whether each element returned by the generator is a single image or or an entire study

        Returns:
            Generator: Generator object yielding either images or studies
        """
        if not upload_by_study:
            upload_list = [[ds] for ds in dses]
        else:
            study_uids = set([ds.StudyInstanceUID for ds in dses])
            upload_list = [
                [ds for ds in dses if ds.StudyInstanceUID == study_uid] for study_uid in study_uids
            ]

        def upload_generator():
            for object in upload_list:
                yield object

        residuals = upload_generator()

        return residuals

    def bulk_upload_image(
        self,
        dses: List[pydicom.Dataset] = None,
        regenerate_uids: bool = False,
        upload_by_study=False,
    ) -> Tuple[List[requests.Response], List[str]]:
        """Performs bulk uploading of DICOMs.
        Will take from the internal list of examples of the ModelInterface, or a list of DICOMs (if provided)
        If regenerate_uids is True, will regenerate the SOP, Series, and Study UIDs prior to uploading.

        Args:
            dses (List[pydicom.Dataset], optional): List of DICOM files to upload to the model prediction service. Defaults to None.
            regenerate_uids (bool, optional): Whether or not to regenerate SOP, Series, Study UIDs of the DICOMs. Defaults to False.
            Upload by study: Uses multi image upload to upload entire study at once

        Raises:
            Exception: If upload was not successful after 3 passes through the list of jobs

        Returns:
            Tuple[List[requests.Response], List[str], Dict[str, str], Dict[str, str]]: A tuple containin the following:
            1) A list of the responses from the web service
            2) A list of the accessions of the uploaded DICOMs
            3) A dict of {regenerated StudyUID: original StudyUID} for all uploaded files
            4) A dict of {regenerated SOPUID: original SOPUID} for all uploaded files
        """
        logger.info('Bulk uploading images...')
        residuals = self.create_gen(dses, upload_by_study)

        count = 1
        accessions = []
        futures = []

        # Determine total number of studies for TQDM purposes
        if not upload_by_study:
            total = len(dses)
        else:
            total = len(set([ds.StudyInstanceUID for ds in dses]))

        while count == 1 or len(residuals) > 0:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = executor.map(
                    partial(
                        upload_images,
                        transport=self.transport,
                        regenerate_uids=regenerate_uids,
                        force_accession_equal_study_uid=self.force_accession_equal_study_uid,
                        internal_maps=self.internal_maps,
                    ),
                    tqdm(residuals, total=total, desc='Uploading', disable=self.disable_tqdm),
                )
            futures = list(futures)

            responses = [f[0] for f in futures]
            accessions = [f[1] for f in futures]
            studyInstanceUids = [f[2] for f in futures]

            # Check if any of the responses weren't successful, and if so, add them to a list for retrying
            residuals = []
            for response, accession, studyInstanceUid, ds in futures:
                if (
                    response.status_code == 400
                    and "DecodeJsonError: 'images' must contain less than or equal to"
                    in response.text
                ):
                    # Image count error, is never gonna pass, don't bother retrying
                    logger.warning(
                        f'Could not upload {accession} as there were too many images.  The error was {response.text}'
                    )
                elif response.status_code != 200:
                    residuals.append(ds)
            regenerate_uids = False  # only need to regenerate uids the first time

            logger.info(
                f'Completed upload pass {count}, remaining dicoms to upload {len(residuals)}'
            )
            count += 1
            if count > 2:
                exception_text = 'Could not complete upload \n\n\n*****\n'.join(
                    [f"{r.text} for {a}/{s}" for r, a, s, _ in futures if r.status_code != 200]
                )
                logger.exception(exception_text)
                raise Exception(exception_text)

        return responses, accessions, studyInstanceUids

    def bulk_get(self, accessions: List[str]) -> List[Dict]:
        """Retrieves the model predictions for a provided list of accessions.
        Updates the internal list of examples' the model_ouputs attribute with the result.
        Replaces all regenerated UIDs with the original UIDs in the result.

        Args:
            accessions (List[str]): List of accessions to retrieve results for

        Returns:
            List[Example]: All the examples corresponding to the provided accessions, with the
            results in the model_outputs attribute
        """
        accessions = list(set(accessions))
        logger.info(f'Obtaining predictions for {len(accessions)} accessions')
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = executor.map(
                partial(get, transport=self.transport),
                tqdm(accessions, desc='Fetching model predictions', disable=self.disable_tqdm),
            )

        futures = list(futures)
        accessions = []

        return futures
    
    def get_studies(self):
        response = get_study_details(self.transport)
        return response