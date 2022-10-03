import pytest
import argparse
from mock import patch
from sem_sim import SemanticSimilarity

@pytest.mark.parametrize('target,reference,out',
                        [(None,'.','.'),
                         ('.', None, '.'),
                         ('.','.',None)], ids=['target','reference','out'])
#test for missing path
def test_missing_path(target, reference, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(target=target,reference=reference,out=out)):
            ss = SemanticSimilarity()
        raise AssertionError('Missing path should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing argument path':
            pass
        else:
            raise AssertionError(f'Error is {err}') from err

#test if the target or reference folders don't have csv
def test_missing_csv():
    #TODO: need a test here
    pass

#tests that all target references have a matching revision
def test_missing_target_values():
    #TODO: need a test here
    pass

#test valid sem sim object
def test_valid_semsim(valid_paths, valuestorage):
    with patch('argparse.ArgumentParser.parse_args',
                return_value=argparse.Namespace(target=valid_paths['target'],
                                                reference=valid_paths['reference'],
                                                out=valid_paths['out'])):
        ss = SemanticSimilarity()
    assert ss.target_path == valid_paths['target']
    assert ss.reference_path == valid_paths['reference']
    assert ss.out_path == valid_paths['out']
    #save valid sem sim for further tests
    valuestorage.valid_sem_sim = ss

#tests the sem sim model
def test_model(valuestorage):
    #get valid sem sim from storage
    valid_sem_sim = valuestorage.valid_sem_sim
    model = valid_sem_sim.sem_sim.model
    assert model.config['_name_or_path'] == 'setu4993/LaBSE'
    assert model.config['architectures'][0] == 'BertModel'
    assert model.embeddings.word_embeddings['num_embeddings'] == 501153

def test_tokenizer(valuestorage):
    #get valid sem sim from storage
    valid_sem_sim = valuestorage.valid_sem_sim
    pass
