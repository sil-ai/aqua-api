import os
import modal
from typing import Literal
from pydantic import BaseModel

# Manage suffix on modal endpoint if testing.
suffix = ''
if os.environ.get('MODAL_TEST') == 'TRUE': 
    suffix = '-test'

volume = modal.SharedVolume().persist("pytorch-model-vol")
CACHE_PATH = "/root/model_cache"

stub = modal.Stub("semantic-similarity" + suffix,
                     image = modal.Image.debian_slim().pip_install(
                        "pandas==1.4.3",
                        "torch==1.12.0",
                        "transformers==4.21.0",
                     ).copy(modal.Mount(
                         local_file='../../fixtures/vref.txt',
                         remote_dir='/root'
                         )
                     ).copy(modal.Mount(
                         local_file='merge_revision.py',
                         remote_dir='/root'
                         )
                )
)

stub.run_pull_rev = modal.Function.from_name("pull_revision", "pull_revision")


class Assessment(BaseModel):
    assessment: int
    revision: int
    reference: int
    type: Literal["semantic-similarity"]


def similarity(embeddings_1, embeddings_2):
    import torch.nn.functional as F
    import torch
    normalized_embeddings_1 = F.normalize(embeddings_1, p=2)
    normalized_embeddings_2 = F.normalize(embeddings_2, p=2)
    return torch.matmul(
        normalized_embeddings_1, normalized_embeddings_2.transpose(0, 1)
    )

class SemanticSimilarity:
    def __init__(self, cache_path=CACHE_PATH):
        from transformers import BertTokenizerFast, BertModel
        try:
            self.semsim_model = BertModel.from_pretrained('setu4993/LaBSE', cache_dir=cache_path).eval()
        except OSError as e:
            print(e)
            print('Downloading model instead of using cache...')
            self.semsim_model = BertModel.from_pretrained('setu4993/LaBSE', cache_dir=cache_path, force_download=True).eval()
        print('Semantic model initialized...')

        try:
            self.semsim_tokenizer = BertTokenizerFast.from_pretrained('setu4993/LaBSE', cache_dir=cache_path)
        except OSError as e:
            print(e)
            print('Downloading tokenizer instead of using cache...')
            self.semsim_tokenizer = BertTokenizerFast.from_pretrained('setu4993/LaBSE', cache_dir=cache_path, force_download=True)
        print('Tokenizer initialized...')

    @stub.function(cpu=4)
    def predict(self, sent1: str, sent2: str, ref: str,
                assessment_id: int, precision: int=2):
        import torch
        """
        Return a prediction.

        Parameters
        ----------
        sent1, sent2 : 2 lists of verse strings to be compared
        
        returns sentences plus a score
        """
        sent1_input = self.semsim_tokenizer(sent1, return_tensors="pt", padding=True)
        sent2_input = self.semsim_tokenizer(sent2, return_tensors="pt", padding=True)
        with torch.no_grad():
            sent1_output = self.semsim_model(**sent1_input)
            sent2_output = self.semsim_model(**sent2_input)

        sent1_embedding = sent1_output.pooler_output
        sent2_embedding = sent2_output.pooler_output

        sim_matrix = similarity(sent1_embedding, sent2_embedding)*5
        #prints the ref to see how we are doing
        print(ref)
        sim_score = round(float(sim_matrix[0][0]),precision)
        print(f'{ref} has a score of {sim_score}')
        return {
            'assessment_id': assessment_id,
            'vref': ref,
            'score': sim_score
        }


@stub.function
def get_text(rev_id: int):
    return modal.container_app.run_pull_rev.call(rev_id)


@stub.function
def merge(revision_id, revision_verses, reference_id, reference_verses):
    from merge_revision import MergeRevision
    mr = MergeRevision(revision_id, revision_verses, reference_id, reference_verses)
    return mr.merge_revision()


@stub.function(
        timeout=300, 
        cpu=4,
        shared_volumes={CACHE_PATH: volume},
)
def assess(assessment: Assessment, offset=-1):
    revision = get_text.call(assessment.revision)
    reference = get_text.call(assessment.reference)
    df = merge.call(assessment.revision,
                    revision,
                    assessment.reference,
                    reference)
    sem_sim = SemanticSimilarity(cache_path=CACHE_PATH)

    #default offset is all of the verses
    sents1 = df['revision'].to_list()[:offset]
    sents2 = df['reference'].to_list()[:offset]
    refs = df.index.to_list()[:offset]
    assessment_id = [assessment.assessment]*len(refs)
    results = list(sem_sim.predict.map(sents1,sents2,refs, assessment_id))
    return results
