import modal
import pytest
from models import SemSimConfig, SemSimAssessment, Results

stub = modal.Stub(
    name="semantic_similarity_test",
    image=modal.Image.debian_slim().pip_install_from_requirements(
        "pytest_requirements.txt"
    ),
)

stub.assess = modal.Function.from_name("semantic_similarity_test", "assess")

@stub.function
def get_assessment(ss_assessment: SemSimAssessment) -> Results:
    return modal.container_app.assess.call(ss_assessment)

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
        results = get_assessment.call(assessment)
        #test for the right type of results
        assert type(results) == Results
        #test for the expected length of results
        assert len(results.results)==expected
        import pickle
        pickle.dump(results, open('results.pkl', 'wb'))
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

@pytest.mark.parametrize('idx,expected',[(42,4),(103,3)],ids=['Gen 2:12','Gen 4:24'])
#test sem_sim predictions
def test_predictions(idx, expected, valuestorage):
    score = valuestorage.results[idx].score
    assert int(round(score,0)) == expected

# @pytest.mark.parametrize('ref', ['JOB 10:19','JOB 12:19','JOB 16:12','JOB 18:9'])
# #test sem_sim json output
# def test_sem_sim_json(ref,json_output):
    
#     try:
#         #extract the verse with reference 'ref'
#         sem_sim_verse = list(filter(lambda item:item['ref']==ref, json_output))[0]
#         #count    311.000000
#         #mean       3.960161
#         #std        0.465435
#         #min        1.990000
#         #25%        3.685000
#         #50%        4.010000
#         #75%        4.290000
#         #max        4.840000

#         #calculate the fuzzywuzzy ratio between 'sent1' and 'sent2'
#         fuzzy_score = fuzz.ratio(sem_sim_verse['sent1'], sem_sim_verse['sent2'])
#         #normalize the fuzzy_score to a 5 scale using a ratio of 1.22 to overcome bias
#         #count    311.000000
#         #mean       4.016977
#         #std        0.585760
#         #min        2.684000
#         #25%        3.599000
#         #50%        3.965000
#         #75%        4.392000
#         #max        5.612000

#         fuzzy_normalized = fuzzy_score/20*1.22
#         #fuzzy score should be within one standard deviation for its sem_sim_score
#         assert (fuzzy_normalized - 0.585) <= sem_sim_verse['score'] <= (fuzzy_normalized + 0.585)
#     except IndexError:
#         raise AssertionError(f'{ref} is not in output')
