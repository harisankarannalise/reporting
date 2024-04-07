# -*- coding: utf-8 -*-
"""Class to encapsulate one example to be sent to the API via model_interface"""

from typing import Dict, List, Union

import numpy as np

from data_uploader_dev.data_uploader.utils.constants import LAT_FINDINGS


class Example:
    """Encapsulates one test case for a given model"""

    def __init__(
        self,
        sop_uids: List[str],
        study_uid: str,
        dataset: str,
        images: List[bytes],
        labels: Dict[str, np.ndarray],
        segmentations: Dict[str, np.ndarray],
        laterality: Dict[str, np.ndarray],
        md5_sums: List[str],
        radiologist: Dict[str, np.ndarray],
    ) -> None:
        """Set up the Example to have all the information needed
        to prepare data for TF Serving

        Arguments:
            images {List[bytes]} -- A list, where each element is the source
            image in raw bytes form
            labels {Dict[str, np.ndarray]} -- A dictionary, where the key:value relationship
            is expressed as output_name:expected_value
        """
        self.sop_uids = sop_uids
        self.study_uid = study_uid
        self.md5_sums = md5_sums
        self.images = images
        self.labels = labels
        self.segmentations = segmentations
        # If using local file then laterality will be none
        self.laterality = self.process_laterality(laterality) if laterality else None
        self.radiologist = radiologist
        self.dataset = dataset

        # Initialise to accept model predictions on loading DICOMs
        self.model_outputs = None
        self.pred_cls = None
        self.pred_seg = None
        self.pred_lat = None
        self.dicom_headers = None
        self.accession = None
        self.processed_images = None

    # TODO - move process_laterality into data_loader
    def process_laterality(self, laterality: Dict[str, int]) -> Dict[str, Union[np.ndarray, None]]:
        """Processes the laterality labels into a usable form for computing F1 score later.

        Args:
            laterality (Dict[str, int]): A dict of laterality labels as output from the
            Labelbox pipeline. For each finding, this has columns 'finding_left' and
            'finding_right', which must be combined into a np array of shape (2,)

        Returns:
             Dict[str, Union[np.ndarray, None]]: A dict containing all laterality findings as keys,
            and the values as a numpy array of shape (2,) if a laterality label is present,
            otherwise None
        """
        processed_laterality = dict()
        for finding in LAT_FINDINGS:
            current_lat = np.array(
                [laterality[finding + '_right'], laterality[finding + '_left']], dtype=int
            )
            if np.sum(current_lat):
                processed_laterality[finding] = current_lat
            else:
                processed_laterality[finding] = None
        return processed_laterality

    def __repr__(self):
        return f'<{len(self.sop_uids)} images in study {self.study_uid}>'

    def __call__(self, verbose=False):
        """A debugging method. On call returns the study_uid, its accession, and images (if any) that were involved in the prediction.
        If a prediction has been made, returns the status and an error (if any)
        for classification.

        May be removed in final version

        Args:
            verbose (bool, optional): Whether to print the values. Defaults to False.

        Returns: A dict with the above information
        """
        ret = {
            'study_uid': self.study_uid,
            'accession': self.accession,
            'processed_images': self.processed_images,
        }
        if self.pred_cls:
            if 'errors' in self.pred_cls:
                ret.update(self.pred_cls)
            else:
                ret.update(
                    {
                        'status': self.model_outputs['classification']['status']['vision'],
                        'errors': self.model_outputs['classification']['errors']['vision']
                        if self.model_outputs['classification']['errors']
                        else None,
                    }
                )
        if verbose:
            for k, v in ret.items():
                print(f'{k}: {v}')
        return ret
