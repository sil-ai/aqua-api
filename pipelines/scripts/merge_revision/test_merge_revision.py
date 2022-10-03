import pytest
import argparse
from mock import patch
from align_revision import AlignRevision

@pytest.mark.parametrize('target,reference,out',
                        [(None,'.','.'),
                         ('.', None, '.'),
                         ('.','.',None)], ids=['target','reference','out'])
#test for missing path
def test_missing_path(target, reference, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(target=target,reference=reference,out=out)):
            AlignRevision()
        raise AssertionError('Missing path should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing filepath':
            pass
        else:
            raise AssertionError(f'Error is {err}') from err

#test valid alignrevision object
def test_valid_alignrevision(valid_paths, valuestorage):
    with patch('argparse.ArgumentParser.parse_args',
                return_value=argparse.Namespace(target=valid_paths['target'],
                                                reference=valid_paths['reference'],
                                                out=valid_paths['out'])):
        ar = AlignRevision()
    assert type(ar.target) == list
    assert type(ar.reference) == list
    assert ar.out_filepath == valid_paths['out']
    #save valid alignrevision for further tests
    valuestorage.valid_alignrevision = ar

#test files of different length
def test_non_aligned_files(valuestorage):
    ar = valuestorage.valid_alignrevision
    #make one file longer than the other
    ar.target = ar.target + ['extra verse']
    try:
        ar.align_revision()
    except ValueError as err:
        if err.args[0] == 'Target and reference differ by 1':
            pass
        else:
            raise AssertionError(err)

