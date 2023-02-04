import modal
import pytest
from semsim_models import SemSimConfig, SemSimAssessment

stub = modal.Stub(
    name="semantic_similarity",
    image=modal.Image.debian_slim().pip_install(
        "pytest==7.1.2",
        "sqlalchemy"
        #"fuzzywuzzy",
        #"python-Levenshtein"
    ).copy(
    modal.Mount(
        local_file="../../runner/push_results/models.py",
        remote_dir="/root"
        )
    )
)

stub.assess = modal.Function.from_name("semantic_similarity", "assess")

@stub.function
def get_assessment(ss_assessment: SemSimAssessment, offset: int=-1):
    return modal.container_app.assess.call(ss_assessment, offset)

#tests the assessment object
@pytest.mark.parametrize(
    "draft_id, ref_id,expected",
    [
        #(1,2, 105),
        (18,29, 3),
    ],  
)
def test_assessment_object(draft_id, ref_id, expected, valuestorage):
    with stub.run():
        from models import Results
        config = SemSimConfig(draft_revision=draft_id,
                              reference_revision=ref_id,
                              type="semantic-similarity")
        assessment = SemSimAssessment(assessment_id=1, configuration=config)
        print(assessment)
        results = get_assessment.call(assessment, offset=105)
        print(results)
        #test for the right type of results
        assert type(results) == Results
        #test for the expected length of results
        assert len(results.results)==expected
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

@pytest.mark.parametrize('idx,expected',[(0,5),(1,5)],ids=['GEN 1:1','GEN 1:2'])
#test sem_sim predictions
def test_predictions(idx, expected, valuestorage):
    try:
        score = valuestorage.results[idx].score
        assert int(round(score,0)) == expected
    except TypeError:
        raise ValueError('No result values')
