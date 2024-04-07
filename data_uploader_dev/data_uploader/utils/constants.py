"""Contains important constants for use in CXR Model Report
"""

import numpy as np

DCM_HEADERS = [
    'PatientAge',
    'PatientSex',
    'ViewPosition',
    'Modality',
    'DetectorType',
    'Manufacturer',
    'ManufacturerModelName',
    'KVP',
    'ExposureInuAs',
    'ExposureIndex',
    'DeviationIndex',
    'PixelSpacing',
    'PatientID',
    'StudyInstanceUID',
    'SeriesInstanceUID',
    'SOPInstanceUID',
    'PatientBirthDate',
    'StudyDate',
    'SoftwareVersions',
    'BitsStored',
]

# BOTH is needed for Ratchet, BILATERAL is needed for Optimus
LAT_MAP = {
    'BILATERAL': np.array([1, 1], dtype=int),
    'BOTH': np.array([1, 1], dtype=int),
    'RIGHT': np.array([1, 0], dtype=int),
    'LEFT': np.array([0, 1], dtype=int),
    'NONE': None,
}

LAT_FINDINGS = [
    'interstitial_thickening_lower',
    'interstitial_thickening_diffuse',
    'scapular_fracture',
    'diffuse_perihilar_airspace_opacity',
    'shoulder_replacement',
    'interstitial_thickening_volloss_upper',
    'acute_clavicle_fracture',
    'shoulder_fixation',
    'miliary',
    'rotator_cuff_anchor',
    'interstitial_thickening_volloss_lower',
    'subcutaneous_emphysema',
    'intercostal_drain',
    'shoulder_dislocation',
    'acute_humerus_fracture',
    'diffuse_airspace_opacity',
    'rib_fixation',
    'diffuse_upper_airspace_opacity',
    'clavicle_fixation',
    'lung_resection_volloss',
    'interstitial_thickening_volloss_diffuse',
    'neck_clips',
    'diffuse_lower_airspace_opacity',
    'lung_collapse',
    'interstitial_thickening_upper',
    'axillary_clips',
]

SEGMENTATION_FINDINGS = [
    'pneumothorax_segmentation',
    'effusion_segmentation',
    'pleural_mass_segmentation',
    'cvc_segmentation',
    'ngt_segmentation',
    'airspace_opacity_segmentation',
    'collapse_segmentation',
    'lesion_segmentation',
    'spine_wedge_fracture_segmentation',
    'acute_rib_fracture_segmentation',
    'spine_lesion_segmentation',
    'rib_lesion_segmentation',
    'scapular_lesion_segmentation',
    'clavicle_lesion_segmentation',
    'humeral_lesion_segmentation',
    'internal_foreign_body_segmentation',
]
