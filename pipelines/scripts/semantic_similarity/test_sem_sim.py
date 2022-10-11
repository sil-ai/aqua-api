import os
import pytest
import argparse
from mock import patch
from sem_sim import SemanticSimilarity

@pytest.mark.parametrize('chunked,out',
                        [(None,'.'),
                         ('.', None)], ids=['chunked','out'])
#test for missing path
def test_missing_path(chunked, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(chunked=chunked,out=out)):
            SemanticSimilarity()
        raise AssertionError('Missing path should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing argument path':
            pass
        else:
            raise AssertionError(f'Error is {err}') from err

#test valid sem sim object
def test_valid_semsim(valid_paths, valuestorage):
    with patch('argparse.ArgumentParser.parse_args',
                return_value=argparse.Namespace(chunked=valid_paths['chunked'],
                                                out=valid_paths['out'])):
        ss = SemanticSimilarity()
    assert ss.chunked_folder == valid_paths['chunked'],'Chunked path doesn\'t match'
    assert ss.out_path == valid_paths['out'],'Out path doesn\'t match'
    #save valid sem sim for further tests
    valuestorage.valid_sem_sim = ss

#test chunk input
def test_csv_input(valuestorage):
    ss = valuestorage.valid_sem_sim
    file_list = os.listdir(ss.chunked_folder)
    #all files should be csv
    assert all([item.split('.')[1]=='csv' for item in file_list]), 'Some files are not csv'
    #number of files and chunks should match
    assert len(ss.list_of_chunks) == len(file_list), 'List of chunks and file list don\'t match'
    #make sure that the chunks are the same size
    chunk_lengths = [len(item) for item in ss.list_of_chunks]
    total_verses = sum(chunk_lengths)
    average_chunk_length = total_verses//len(ss.list_of_chunks)
    for item in chunk_lengths:
        #checks that chunk lengths are all within one of each other
        assert item==average_chunk_length or item==average_chunk_length + 1,'Some chunks are of different length'

#tests the sem sim model
def test_model(valuestorage):
    #get valid model from storage
    model = valuestorage.valid_sem_sim.sem_sim.model
    assert model.config._name_or_path == 'setu4993/LaBSE'
    assert model.config.architectures[0] == 'BertModel'
    assert model.embeddings.word_embeddings.num_embeddings == 501153

#test semantic chunks

#test sem_sim predictions

#test sem_sim json output


# def test_tokenizer(valuestorage):
#     #get valid sem sim from storage
#     valid_sem_sim = valuestorage.valid_sem_sim
#     pass
