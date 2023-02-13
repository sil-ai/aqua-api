import modal
import pytest
from pathlib import Path

from app import Assessment

volume = modal.SharedVolume().persist("pytorch-model-vol")
CACHE_PATH = "/root/model_cache"

#??? Is there an advange to putting image definition inside the stub?
stub = modal.Stub(
    name="run-semantic-similarity-test",
    image=modal.Image.debian_slim().pip_install(
    "pytest",
    "pyyaml",
    "pandas",
    "torch==1.12.0",
    "transformers==4.21.0",
    "SQLAlchemy==1.4.46"
    ).copy(
         modal.Mount.from_local_file(
            local_path="./fixtures/swahili_revision.pkl",
            remote_path="/root/fixtures/swahili_revision.pkl"
        )
    ).copy(
         modal.Mount.from_local_file(
            local_path="./fixtures/swahili_drafts.yml",
            remote_path="/root/fixtures/swahili_drafts.yml"
        )
    )
)

stub.assess = modal.Function.from_name("semantic-similarity-test", "assess")


@stub.function
def get_assessment(config, offset: int=-1):
    return modal.container_app.assess.call(config, offset)

version_abbreviation = 'SS-DEL'
version_name = 'semantic similarity delete'


# Add a version to the database for this test
def test_add_version(base_url, header):
    import requests
    test_version = {
            "name": version_name, "isoLanguage": "swh",
            "isoScript": "Latn", "abbreviation": version_abbreviation
            }
    url = base_url + '/version'
    response = requests.post(url, json=test_version, headers=header)
    if response.status_code == 400 and response.json()['detail'] == "Version abbreviation already in use.":
        print("This version is already in the database")
    else:
        assert response.json()['name'] == version_name


# Add two revisions to the database for this test
@pytest.mark.parametrize("filepath", [Path("../../fixtures/greek_lemma_luke.txt"), Path("../../fixtures/ngq-ngq.txt")])
def test_add_revision(base_url, header, filepath: Path):
    import requests
    test_abv_revision = {
            "version_abbreviation": version_abbreviation,
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200


@stub.function
def assessment_object(draft_id, ref_id, expected):
    config = Assessment(
                            revision=draft_id,
                            reference=ref_id,
                            type="semantic-similarity")
    results = get_assessment.call(config)
    #test for the right type of results
    assert type(results) == list
    #test for the expected length of results
    assert len(results)==expected
    return results


# tests the assessment object
def test_assessment_object(base_url, header, valuestorage):
    with stub.run():
        import requests
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': version_abbreviation})
        reference = response.json()[0]['id']
        revision = response.json()[1]['id']
        expected = 1142     # Length of verses in common between the two fixture revisions (basically the book of Luke)
        results = assessment_object.call(revision, reference, expected)
        valuestorage.results = results

        
@stub.function(shared_volumes={CACHE_PATH: volume})
def model_tester():
    from app import SemanticSimilarity
    model = SemanticSimilarity().semsim_model
    assert model.config._name_or_path == 'setu4993/LaBSE'
    assert model.config.architectures[0] == 'BertModel'
    assert model.embeddings.word_embeddings.num_embeddings == 501153


#tests the sem sim model
def test_model():
    with stub.run():
        model_tester.call()


@stub.function(shared_volumes={CACHE_PATH: volume})
def token_tester(vocab_item, vocab_id):
    from app import SemanticSimilarity
    tokenizer = SemanticSimilarity().semsim_tokenizer
    try:
        assert tokenizer.vocab[vocab_item] == vocab_id,\
         f'{vocab_item} does not have a vocab_id of {vocab_id}'
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

@pytest.mark.parametrize('idx,expected',[(0,1),(1,0)],ids=['LUK 1:1','LUK 1:2'])
#test sem_sim predictions
def test_predictions(idx, expected, request, valuestorage):
    
    with stub.run():
        try:
            score = valuestorage.results[idx]['score']
            prediction_tester.call(expected, score)
        except TypeError:
            raise AssertionError('No result values')


#!!! Assumes that predict is the same as app.py
@stub.function(shared_volumes={CACHE_PATH: volume})
def predict(sent1: str, sent2: str, ref: str, precision: int=2):
    import torch
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
    sim_score = round(float(sim_matrix[0][0]),precision)
    return {
        'vref': ref,
        'score': sim_score,
    }

#!!! This get_swahili_verses is deterministic
@stub.function
def get_swahili_verses(verse_offset, variance):
    import pandas as pd
    import yaml
    drafts = yaml.safe_load(open('/root/fixtures/swahili_drafts.yml'))['drafts']
    draft_verse = drafts[f"{verse_offset}-{variance}"]
    swahili_revision = pd.read_pickle('/root/fixtures/swahili_revision.pkl')
    verse = swahili_revision.iloc[verse_offset].text
    return verse, draft_verse

@pytest.mark.parametrize('verse_offset, variance, expected',
                            [
                                (12,5,4.7),
                                (42,10,3.38),
                                (1042,20,0.97),
                                (4242,30,2.14),
                            ],
                            ids=['NEH 10:21 5%',
                                 'GEN 26:21 10%',
                                 'GEN 32:22 20%',
                                 'Num 16:9 30%']
                        )
def test_swahili_revision(verse_offset, variance, expected, request):
    with stub.run():
        verse, draft_verse = get_swahili_verses.call(verse_offset, variance)
        results = predict.call(verse, draft_verse, request.node.name)
        assert results['score'] == expected


def test_delete_version(base_url, header):
    import requests
    test_delete_version = {
            "version_abbreviation": version_abbreviation
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200
