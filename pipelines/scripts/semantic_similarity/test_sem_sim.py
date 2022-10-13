import os
import pytest
import argparse
from mock import patch
from sem_sim import SemanticSimilarity
import pandas as pd
from fuzzywuzzy import fuzz

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

@pytest.mark.parametrize('vocab_item,vocab_id',
                        [('jesus',303796),
                         ('Thomas',18110),
                         ('imagery',325221)],ids=['jesus','Thomas','imagery'])
def test_tokenizer_vocab(vocab_item,vocab_id,valuestorage):
    #get valid sem sim from storage
    tokenizer = valuestorage.valid_sem_sim.sem_sim.tokenizer
    try:
        assert tokenizer.vocab[vocab_item] == vocab_id, f'{vocab_item} does not have a vocab_id of {vocab_id}'
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

#test semantic chunks
def test_semantic_chunks(valuestorage):
    list_of_chunks = valuestorage.valid_sem_sim.list_of_chunks
    #chunks are all dataframes
    assert all([type(item)==pd.DataFrame for item in list_of_chunks])
    #chunks are sorted monotonically ascending
    chunk_names = [item.name for item in list_of_chunks]
    assert all([higher-1 == lower for lower,higher in zip(chunk_names,chunk_names[1:])])

@pytest.mark.parametrize('chunk_id,expected',[(0,5),(51,5)])
#TODO: need more interesting results to test this properly than just 5
#test sem_sim predictions
def test_predictions(chunk_id,expected, valuestorage):
    ss = valuestorage.valid_sem_sim
    chunk = ss.list_of_chunks[chunk_id]
    assert all([int(round(prediction[2],0))==expected for prediction in ss.sem_sim.predict(list(zip(chunk['target'],chunk['reference'])))])


@pytest.mark.parametrize('ref', ['GEN 1:1','GEN 23:12','EXO 12:8','EXO 14:7'])
#test sem_sim json output
def test_sem_sim_json(ref,json_output):
    #taken from mean to mean + std in calibrate_fuzzy_wuzzy.py
    similarity_mapping = { 0:(0,64.32),
                           1:(64.33, 70.76),
                           2:(70.42, 77.65),
                           3:(77.9, 83.14),
                           4:(95.3, 99.13),
                           5:(99.14,100)}

    #extract the verse with reference 'ref'
    sem_sim_verse = list(filter(lambda item:item['ref']==ref, json_output))[0]
    #calculate the fuzzywuzzy ratio between 'sent1' and 'sent2'
    fuzzy_score = fuzz.ratio(sem_sim_verse['sent1'], sem_sim_verse['sent2'])
    #map the sem_sim score onto the fuzzywuzzy ratio scale
    fuzzy_mapped_scores = similarity_mapping[sem_sim_verse['score']]
    #fuzzy score should be in the similarity mapping range for its sem_sim_score
    assert fuzzy_mapped_scores[0] <= fuzzy_score <= fuzzy_mapped_scores[1]
