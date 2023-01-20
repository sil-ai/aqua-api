import modal
from semsim_models import SemSimAssessment, SemSimConfig
from pandas import DataFrame
import logging
logging.basicConfig(level=logging.DEBUG)

stub = modal.Stub("semantic_similarity",
                     image = modal.Image.debian_slim().pip_install(
                        "pandas==1.4.3"
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

semsim_image = modal.Image.debian_slim().pip_install(
    "pandas==1.4.3",
    "torch==1.12.0",
    "transformers==4.21.0",
    "SQLAlchemy==1.4.46"
).copy(
    modal.Mount(
        local_file="../../runner/push_results/models.py",
        remote_dir="/root"
    )
)

def similarity(embeddings_1, embeddings_2):
    import torch.nn.functional as F
    import torch

    normalized_embeddings_1 = F.normalize(embeddings_1, p=2)
    normalized_embeddings_2 = F.normalize(embeddings_2, p=2)
    return torch.matmul(
        normalized_embeddings_1, normalized_embeddings_2.transpose(0, 1)
    )

stub.run_pull_rev = modal.Function.from_name("pull_revision", "pull_revision")

class SemanticSimilarity:

    def __init__(self):
        #!!! can't test the model and tokenizer if I user __enter__
        from transformers import BertTokenizerFast, BertModel
        self.semsim_model = BertModel.from_pretrained('setu4993/LaBSE').eval()
        logging.info('Semantic model initialized...')
        self.semsim_tokenizer = BertTokenizerFast.from_pretrained('setu4993/LaBSE')
        logging.info('Tokenizer initialized...')

    #??? May want to raise the concurrency limit
    @stub.function(image=semsim_image,cpu=4, concurrency_limit=2)
    def predict(self, sent1: str, sent2: str, ref: str,
                assessment_id: int, precision: int=2):
        import torch
        from models import  Result
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
        #TODO: remove print
        print(sent1,sent2, sim_matrix)
        sim_score = round(float(sim_matrix[0][0]),precision)
        logging.info(f'{ref} has a score of {sim_score}')
        #??? What values do you want for flag and note @dwhitena?
        return Result(assessment_id=assessment_id,
                      vref=ref,
                      score=sim_score)

@stub.function
def get_text(rev_id: int)-> DataFrame:
    return modal.container_app.run_pull_rev.call(rev_id)

@stub.function
def merge(draft_id: int, draft_verses: DataFrame,
          reference_id: int, reference_verses: DataFrame)-> DataFrame:
    from merge_revision import MergeRevision
    mr = MergeRevision(draft_id, draft_verses, reference_id, reference_verses)
    return mr.merge_revision()

@stub.function(image=semsim_image,
               timeout=1000,
               cpu=4)
def assess(assessment: SemSimAssessment, offset=-1):
    from models import  Results
    draft = get_text.call(assessment.configuration.draft_revision)
    reference = get_text.call(assessment.configuration.reference_revision)
    df = merge.call(assessment.configuration.draft_revision,
                    draft,
                    assessment.configuration.reference_revision,
                    reference)
    sem_sim = SemanticSimilarity()
    #default offset is all of the verses
    sents1 = df['draft'].to_list()[:offset]
    sents2 = df['reference'].to_list()[:offset]
    refs = df.index.to_list()[:offset]
    assessment_id = [assessment.assessment_id]*len(refs)
    results = list(sem_sim.predict.map(sents1,sents2,refs, assessment_id))
    return Results(results=results)

if __name__ == '__main__':
    with stub.run():
        config = SemSimConfig(draft_revision=1, reference_revision=2)
        assessment = SemSimAssessment(assessment_id=1, configuration=config)
        offset = 105
        results = assess.call(assessment, offset)
