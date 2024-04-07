import pytest
from data_uploader.utils import example
from data_uploader.utils.constants import LAT_FINDINGS, LAT_MAP
import numpy as np


@pytest.fixture
def mock_laterality_data():
    def construct_laterality(side):
        assert side in ['RIGHT', 'LEFT', 'NONE', 'BILATERAL']
        ret = dict()
        for finding in LAT_FINDINGS:
            if side in ['RIGHT', 'BILATERAL']:
                ret[finding + '_right'] = 1
            else:
                ret[finding + '_right'] = 0

            if side in ['LEFT', 'BILATERAL']:
                ret[finding + '_left'] = 1
            else:
                ret[finding + '_left'] = 0
        return ret

    return construct_laterality


def test_process_laterality(mock_laterality_data):
    laterality = mock_laterality_data('NONE')
    processed_laterality = example.Example.process_laterality('self', laterality)
    assert all([v is None for k, v in processed_laterality.items()])

    laterality = mock_laterality_data('RIGHT')
    check_laterality = LAT_MAP['RIGHT']
    processed_laterality = example.Example.process_laterality('self', laterality)
    assert all([np.array_equal(v, check_laterality) for k, v in processed_laterality.items()])

    laterality = mock_laterality_data('LEFT')
    check_laterality = LAT_MAP['LEFT']
    processed_laterality = example.Example.process_laterality('self', laterality)
    assert all([np.array_equal(v, check_laterality) for k, v in processed_laterality.items()])

    laterality = mock_laterality_data('BILATERAL')
    check_laterality = LAT_MAP['BILATERAL']
    processed_laterality = example.Example.process_laterality('self', laterality)
    assert all([np.array_equal(v, check_laterality) for k, v in processed_laterality.items()])


def test_example(mock_laterality_data):
    eg = example.Example(
        sop_uids='sop_uids',
        study_uid='study_uid',
        md5_sums='md5_sums',
        dataset='dataset',
        images='images',
        labels='labels',
        segmentations='segmentations',
        laterality=mock_laterality_data('RIGHT'),
        radiologist='radiologist',
    )
    assert eg.sop_uids == 'sop_uids'
    assert eg.study_uid == 'study_uid'
    assert eg.md5_sums == 'md5_sums'
    assert eg.dataset == 'dataset'
    assert eg.images == 'images'
    assert eg.labels == 'labels'
    assert eg.segmentations == 'segmentations'
    assert eg.radiologist == 'radiologist'
    assert isinstance(eg.laterality, dict)
    assert len(eg.laterality) == len(LAT_FINDINGS)


def test_example_call(mock_laterality_data):
    eg = example.Example(
        sop_uids='sop_uids',
        study_uid='study_uid',
        md5_sums='md5_sums',
        dataset='dataset',
        images='images',
        labels='labels',
        segmentations='segmentations',
        laterality=mock_laterality_data('RIGHT'),
        radiologist='radiologist',
    )
    call_result = eg()
    assert isinstance(call_result['study_uid'], str)
    assert call_result['accession'] is None
    assert 'errors' not in call_result

    eg.pred_cls = {'errors': 'foo'}
    call_result = eg()
    assert 'errors' in call_result
    assert call_result['errors'] == 'foo'
