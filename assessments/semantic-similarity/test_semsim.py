import modal
import pytest
import random
from string import ascii_letters
from semsim_models import SemSimConfig, SemSimAssessment

stub = modal.Stub(
    name="semantic_similarity",
    image=modal.Image.debian_slim().pip_install(
        "pytest==7.1.2",
        "sqlalchemy",
        "transformers"
    ).copy(
    modal.Mount(
        local_file="../../runner/push_results/models.py",
        remote_dir="/root"
        )
    ).copy(
        modal.Mount(
            local_file="./fixtures/revisions_feb_4.pkl",
            remote_dir="/root/fixtures"
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
        (18,29, 2),
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
    with stub.run():
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

#!!! Assumes stub version of predict doesn't change from app.py
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
def test_swahili_revision(verse_offset, variance, inequality, request, swahili_revision):
    with stub.run():
        verse = swahili_revision.iloc[verse_offset]['text']
        draft_verse = create_draft_verse(verse, variance)
        assessment_id = 1
        results = predict(verse, draft_verse, request.node.name, assessment_id)
        assert eval(f'{results.score}{inequality}')
