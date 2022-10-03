import pytest
import argparse
from mock import patch
from merge_revision import MergeRevision
import pandas as pd

@pytest.mark.parametrize('target,reference,out',
                        [(None,'.','.'),
                         ('.', None, '.'),
                         ('.','.',None)], ids=['target','reference','out'])
#test for missing path
def test_missing_path(target, reference, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(target=target,reference=reference,out=out)):
            MergeRevision()
        raise AssertionError('Missing path should have failed')
    except ValueError as err:
        assert err.args[0] == 'Missing filepath', f'Error is {err}'

#test valid mergerevision object
def test_valid_mergerevision(valid_paths, valuestorage):
    with patch('argparse.ArgumentParser.parse_args',
                return_value=argparse.Namespace(target=valid_paths['target'],
                                                reference=valid_paths['reference'],
                                                out=valid_paths['out'])):
        mr = MergeRevision()
    assert type(mr.target) == list
    assert type(mr.reference) == list
    assert mr.args.out == valid_paths['out']
    #save valid mergerevision for further tests
    valuestorage.valid_mergerevision = mr

#test merge output
def test_merge_output(valuestorage, tmp_path):
    try:
        #get the instance
        mr = valuestorage.valid_mergerevision
        #redirect output to tmp_path which is system-generated and temporary of course!
        mr.args.out = str(tmp_path)
        #create the merge output file
        mr.output_merged_revisions(mr.merge_revision())
        filepath = list(tmp_path.iterdir())[0]
        merged_file = pd.read_csv(filepath)
        #check the filetype is csv
        assert str(filepath).split('.')[-1]=='csv', 'File is not csv'
        #each column should be the same length as vref
        assert len(merged_file.loc[:,'target']) == len(mr.vref)
        assert len(merged_file.loc[:,'reference']) == len(mr.vref)
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

#test files of different length
def test_non_aligned_files(valuestorage):
    mr = valuestorage.valid_mergerevision
    #make one file longer than the other
    mr.target = mr.target + ['extra verse']
    try:
        mr.merge_revision()
    except ValueError as err:
        if err.args[0] == 'Target and reference differ by 1':
            pass
        else:
            raise AssertionError(err)
