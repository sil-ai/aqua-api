import pytest
import argparse
from mock import patch
from fuzzywuzzy import fuzz
import pandas as pd

from sem_sim import SemanticSimilarity


@pytest.mark.parametrize('input,out',
                        [(None,'.'),
                         ('.', None)], ids=['input','out'])
#test for missing argument
def test_missing_argument(input, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(input=input,out=out)):
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
                return_value=argparse.Namespace(input=valid_paths['input'],
                                                out=valid_paths['out'])):
        ss = SemanticSimilarity()
    assert ss.input_filename == valid_paths['input'],'Input file doesn\'t match'
    assert ss.out_path == valid_paths['out'],'Out path doesn\'t match'
    #save valid sem sim for further tests
    valuestorage.valid_sem_sim = ss

#test input file
def test_input_file(valuestorage):
    ss = valuestorage.valid_sem_sim
    assert type(ss.input) == pd.DataFrame
    assert all(pd.read_csv(ss.input_filename)) == all(ss.input)

# #tests the sem sim model
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

@pytest.mark.parametrize('idx,expected',[(12,4),(22,4)],ids=['Gen 40:16','Gen 41:3'])
#test sem_sim predictions
def test_predictions(idx, expected, valuestorage):
    ss = valuestorage.valid_sem_sim
    sent1, sent2 = ss.input.loc[idx][['target','reference']]
    prediction = ss.sem_sim.predict([sent1],[sent2])[0]
    score = prediction[2]
    assert int(round(score,0)) == expected

@pytest.mark.parametrize('ref', ['JOB 10:19','JOB 12:19','JOB 16:12','JOB 18:9'])
#test sem_sim json output
def test_sem_sim_json(ref,json_output):
    
    try:
        #extract the verse with reference 'ref'
        sem_sim_verse = list(filter(lambda item:item['ref']==ref, json_output))[0]
        #count    311.000000
        #mean       3.960161
        #std        0.465435
        #min        1.990000
        #25%        3.685000
        #50%        4.010000
        #75%        4.290000
        #max        4.840000

        #calculate the fuzzywuzzy ratio between 'sent1' and 'sent2'
        fuzzy_score = fuzz.ratio(sem_sim_verse['sent1'], sem_sim_verse['sent2'])
        #normalize the fuzzy_score to a 5 scale using a ratio of 1.22 to overcome bias
        #count    311.000000
        #mean       4.016977
        #std        0.585760
        #min        2.684000
        #25%        3.599000
        #50%        3.965000
        #75%        4.392000
        #max        5.612000

        fuzzy_normalized = fuzzy_score/20*1.22
        #fuzzy score should be within one standard deviation for its sem_sim_score
        assert (fuzzy_normalized - 0.585) <= sem_sim_verse['score'] <= (fuzzy_normalized + 0.585)
    except IndexError:
        raise AssertionError(f'{ref} is not in output')
