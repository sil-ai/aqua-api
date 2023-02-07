import os
import modal
import pytest
from fuzzywuzzy import fuzz
from models import SemSimConfig, SemSimAssessment, Results

# Manage suffix on modal endpoint if testing.
suffix = ''
if os.environ.get('MODAL_TEST') == 'TRUE':
    suffix = '_test'

stub = modal.Stub(
    name="semantic_similarity" + suffix,
    image=modal.Image.debian_slim().pip_install_from_requirements(
        "pytest_requirements.txt"
    ),
)

stub.assess = modal.Function.from_name("semantic_similarity", "assess")

@stub.function
def get_assessment(ss_assessment: SemSimAssessment, offset: int=-1) -> Results:
    return modal.container_app.assess.call(ss_assessment, offset)

#tests the assessment object
@pytest.mark.parametrize(
    "draft_id, ref_id,expected",
    [
        (1,2, 105),
    ],  
)
def test_assessment_object(draft_id, ref_id, expected, valuestorage):
    with stub.run():
        config = SemSimConfig(draft_revision=draft_id, reference_revision=ref_id)
        assessment = SemSimAssessment(assessment_id=1, configuration=config)
        results = get_assessment.call(assessment, offset=105)
        #test for the right type of results
        assert type(results) == Results
        #test for the expected length of results
        assert len(results.results)==expected
        #import pickle
        #pickle.dump(results, open('results.pkl', 'wb'))
        valuestorage.results = results.results

#tests the sem sim model
def test_model(model):
    assert model.config._name_or_path == 'setu4993/LaBSE'
    assert model.config.architectures[0] == 'BertModel'
    assert model.embeddings.word_embeddings.num_embeddings == 501153

@pytest.mark.parametrize('vocab_item,vocab_id',
                         [('jesus',303796),
                          ('Thomas',18110),
                          ('imagery',325221)],ids=['jesus','Thomas','imagery'])
def test_tokenizer_vocab(vocab_item,vocab_id,tokenizer):
    try:
        assert tokenizer.vocab[vocab_item] == vocab_id, f'{vocab_item} does not have a vocab_id of {vocab_id}'
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

@pytest.mark.parametrize('idx,expected',[(42,4),(103,3)],ids=['GEN 2:12','GEN 4:24'])
#test sem_sim predictions
def test_predictions(idx, expected, valuestorage):
    score = valuestorage.results[idx].score
    assert int(round(score,0)) == expected

#TODO: find a better way to test the accuracy of the sem sim
# @pytest.mark.parametrize('ref', ['GEN 1:1','GEN 3:21', 'GEN 4:8'])
# # #test sem_sim json output
# def test_sem_sim_results(ref,valuestorage, rev1_2):
    
#     try:
#         #extract the verse with reference 'ref'
#         result = list(filter(lambda item:item.verse==ref, valuestorage.results))[0]
#         # count    105.000000
#         # mean       4.450095
#         # std        0.267357
#         # min        3.490000
#         # 25%        4.290000
#         # 50%        4.500000
#         # 75%        4.660000
#         # max        4.910000


#         #calculate the fuzzywuzzy ratio between 'sent1' and 'sent2'
#         sent1, sent2 = rev1_2[rev1_2['verseReference']==ref].iloc[0]['text_x','text_y']
#         fuzzy_score = fuzz.ratio(sent1, sent2)
#         #normalize the fuzzy_score to a 5 scale adding 0.7 mean difference to overcome bias
#         # count    105.000000
#         # mean       3.751905
#         # std        0.393818
#         # min        2.800000
#         # 25%        3.500000
#         # 50%        3.800000
#         # 75%        4.050000
#         # max        4.750000

#         fuzzy_normalized = fuzzy_score/20 + 0.7
#         #fuzzy score should be within one standard deviation for its sem_sim_score
#         assert (fuzzy_normalized - 0.39) <= result.score <= (fuzzy_normalized + 0.39)
#     except IndexError:
#         raise AssertionError(f'{ref} is not in output')
