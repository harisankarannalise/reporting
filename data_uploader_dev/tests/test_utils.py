import pytest
import pandas as pd

from data_uploader.utils import utils
from data_uploader.utils.constants import DCM_HEADERS


@pytest.fixture
def test_dcm_file_path():
    return 'tests/assets/jpeg2000.dcm'


@pytest.fixture
def test_dcm(test_dcm_file_path):
    with open(test_dcm_file_path, 'rb') as f:
        data = f.read()
    return data


@pytest.fixture
def test_labels():
    labels = pd.DataFrame({'study_1': {'finding_1': 2, 'finding_2': 4}}).T
    labels.index.name = 'StudyInstanceUID'
    return labels


@pytest.fixture
def test_label_keys():
    return {
        'finding_3': 'finding_1 or finding_2',
        'finding_4': 'finding_1 and finding_2',
        'finding_5': 'not finding_2',
        'finding_6': 'finding_1 and not finding_2',
        'finding_7': 'finding_2 and not finding_1',
    }


def test_get_dicom_headers(test_dcm):
    r = utils.get_dicom_headers([test_dcm])
    assert len(r) == 1
    r = r[0]
    assert set(r.keys()) == set(DCM_HEADERS)


# Could parametrize in future
def test_recursive_replace():
    replace_dict = {'foo': 'bar'}
    assert utils.recursive_replace('foo', replace_dict) == 'bar'
    assert utils.recursive_replace(['banana', 'foo'], replace_dict) == ['banana', 'bar']
    assert utils.recursive_replace({'apple': 'foo', 'banana': 'bar'}, replace_dict) == {
        'apple': 'bar',
        'banana': 'bar',
    }


def test_generate_md5_sum():
    assert utils.generate_md5_sum(b'banana') == '72b302bf297a228a75730123efef7c41'
    with pytest.raises(TypeError):
        r = utils.generate_md5_sum('banana')


def test_derive_labels(test_labels, test_label_keys):
    r = utils.derive_labels(orig_df=test_labels, derived_label_keys=test_label_keys)
    assert r['finding_3'].values[0] == 4
    assert r['finding_4'].values[0] == 2
    assert r['finding_5'].values[0] == 0
    assert r['finding_6'].values[0] == 0
    assert r['finding_7'].values[0] == 2
