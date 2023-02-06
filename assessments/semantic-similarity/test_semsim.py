import modal
import pytest
import random
from string import ascii_letters
from semsim_models import SemSimConfig, SemSimAssessment
from app import SemanticSimilarity

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
stub.predict = modal.Function.from_name("semantic_similarity","predict")

@stub.function
def get_assessment(ss_assessment: SemSimAssessment, offset: int=-1):
    return modal.container_app.assess.call(ss_assessment, offset)

@stub.function
def get_prediction(sent1: str, sent2: str, assessment_id: int=1):
    return modal.container_app.predict.call(sent1, sent2, assessment_id)

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
        results = get_assessment.call(assessment)
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

def create_draft_verse(verse, variance):
    num_of_chars = int(round(variance/100*len(verse),0))
    idx = [i for i,_ in enumerate(verse) if not verse.isspace()]
    sam = random.sample(idx, num_of_chars)
    lst = list(verse)
    for ind in sam:
        lst[ind] = random.choice(ascii_letters)
    return "".join(lst)

@pytest.mark.parametrize('verse_offset, variance',
                            [
                                (42,10),
                                (1042,20),
                                (4242,30),
                            ],
                            ids=['GEN 26:21','GEN 32:22','Num 16:9']
                        )
def test_swahili_revision(verse_offset, variance, swahili_revision):
    with stub.run():
        #TODO: almost there!?
        from fuzzywuzzy import fuzz
        semsim = SemanticSimilarity()
        verse = swahili_revision.iloc[verse_offset]['text']
        draft_verse = create_draft_verse(verse, variance)
        print(verse, draft_verse, fuzz.ratio(verse,draft_verse))
        prediction = get_assessment.call(verse, draft_verse, id,1)
        print(prediction)
