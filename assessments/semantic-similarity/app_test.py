import modal
import pytest
import random
from string import ascii_letters
from app import SemanticSimilarity, similarity

assessment_id = 99999

semsim_image = modal.Image.debian_slim().pip_install(
    "pytest",
    "pandas",
    "torch==1.12.0",
    "transformers==4.21.0",
    "SQLAlchemy==1.4.46"
    ).copy(
        modal.Mount(
            local_file="../../runner/push_results/models.py",
            remote_dir="/root"
        )
    ).copy(
        modal.Mount(
            local_file="./semsim_models.py",
            remote_dir="/root"
        )
    ).copy(
         modal.Mount(
            local_file="./fixtures/swahili_revision.pkl",
            remote_dir="/root/fixtures/"
        )
    )

stub = modal.Stub(
    name="semantic-similarity-test",
    image=semsim_image
)

stub.assess = modal.Function.from_name("semantic_similarity", "assess")

@stub.function#(image=semsim_image)
def get_assessment(ss_assessment, offset: int=-1):
    return modal.container_app.assess.call(ss_assessment, offset)

@stub.function#(image=semsim_image)
def assessment_object(draft_id, ref_id, expected):
    from semsim_models import SemSimConfig, SemSimAssessment
    from models import Results
    config = SemSimConfig(draft_revision=draft_id,
                            reference_revision=ref_id,
                            type="semantic-similarity")
    assessment = SemSimAssessment(assessment_id=assessment_id, configuration=config)
    results = get_assessment.call(assessment)
    #test for the right type of results
    assert type(results) == Results
    #test for the expected length of results
    assert len(results.results)==expected
    return results.results

#tests the assessment object
@pytest.mark.parametrize(
    "draft_id, ref_id,expected",
    [
        #(1,2, 105),
        (18,29, 2),
    ],  
)
def test_assessment_object(draft_id, ref_id, expected, valuestorage):
    with stub.run():
        results = assessment_object.call(draft_id, ref_id, expected)
        valuestorage.results = results

@stub.function#(image=semsim_image)
def model_tester():
    model = SemanticSimilarity().semsim_model
    assert model.config._name_or_path == 'setu4993/LaBSE'
    assert model.config.architectures[0] == 'BertModel'
    assert model.embeddings.word_embeddings.num_embeddings == 501153

#tests the sem sim model
def test_model():
    with stub.run():
        model_tester.call()

@stub.function#(image=semsim_image)
def token_tester(vocab_item, vocab_id):
        tokenizer = SemanticSimilarity().semsim_tokenizer
        try:
            assert tokenizer.vocab[vocab_item] == vocab_id, f'{vocab_item} does not have a vocab_id of {vocab_id}'
        except Exception as err:
            raise AssertionError(f'Error is {err}') from err

@pytest.mark.parametrize('vocab_item,vocab_id',
                         [('jesus',303796),
                          ('Thomas',18110),
                          ('imagery',325221)],ids=['jesus','Thomas','imagery'])
def test_tokenizer_vocab(vocab_item,vocab_id):
    with stub.run():
        token_tester.call(vocab_item, vocab_id)

@stub.function
def prediction_tester(expected, score):
    try:
        assert int(round(score,0)) == expected
    except TypeError:
        raise ValueError('No result values')

@pytest.mark.parametrize('idx,expected',[(0,5),(1,5)],ids=['GEN 1:1','GEN 1:2'])
#test sem_sim predictions
def test_predictions(idx, expected, valuestorage):
    with stub.run():
        score = valuestorage.results[idx].score
        prediction_tester.call(expected, score)

#!!! Assumes stub version of predict doesn't change from app.py
@stub.function
def predict(sent1: str, sent2: str, ref: str,
            assessment_id: int, precision: int=2):
    import torch
    from models import Result
    from app import SemanticSimilarity, similarity

    ss = SemanticSimilarity()
    sent1_input = ss.semsim_tokenizer(sent1, return_tensors="pt", padding=True)
    sent2_input = ss.semsim_tokenizer(sent2, return_tensors="pt", padding=True)
    with torch.no_grad():
        sent1_output = ss.semsim_model(**sent1_input)
        sent2_output = ss.semsim_model(**sent2_input)

    sent1_embedding = sent1_output.pooler_output
    sent2_embedding = sent2_output.pooler_output

    sim_matrix = similarity(sent1_embedding, sent2_embedding)*5
    #prints the ref to see how we are doing
    print(ref)
    sim_score = round(float(sim_matrix[0][0]),precision)
    #??? What values do you want for flag and note @dwhitena?
    return Result(assessment_id=assessment_id,
                vref=ref,
                score=sim_score)

def create_draft_verse(verse, variance):
    num_of_chars = int(round(variance/100*len(verse),0))
    idx = [i for i,_ in enumerate(verse) if not verse.isspace()]
    sam = random.sample(idx, num_of_chars)
    lst = list(verse)
    for ind in sam:
        lst[ind] = random.choice(ascii_letters)
    return "".join(lst)

@stub.function
def get_swahili_verses(verse_offset, variance):
    import pandas as pd
    swahili_revision = pd.read_pickle('./fixtures/swahili_revision.pkl')
    verse = swahili_revision.iloc[verse_offset]['text']
    draft_verse = create_draft_verse(verse, variance)
    return verse, draft_verse

@pytest.mark.parametrize('verse_offset, variance, inequality',
                            [
                                (12,5, '>4'),
                                (42,10, '>3'),
                                (1042,20,'>2'),
                                (4242,30, '<3'),
                            ],
                            ids=['NEH 10:21 5%>4',
                                 'GEN 26:21 10%>3',
                                 'GEN 32:22 20%>2',
                                 'Num 16:9 30% <3']
                        )
def test_swahili_revision(verse_offset, variance, inequality, request):
    with stub.run():
        verse, draft_verse = get_swahili_verses.call(verse_offset, variance)
        results = predict.call(verse, draft_verse, request.node.name, assessment_id)
        assert eval(f'{results.score}{inequality}')
