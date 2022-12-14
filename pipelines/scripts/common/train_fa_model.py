from machine.corpora import TextFileTextCorpus
from machine.translation import SymmetrizationHeuristic

from machine.translation.thot import (
    ThotFastAlignWordAlignmentModel,
    ThotSymmetrizedWordAlignmentModel,
)


def train_model(corpus: TextFileTextCorpus) -> ThotSymmetrizedWordAlignmentModel:
    """
    Takes an aligned corpus as input and trains a model on that corpus
    Inputs:
    corpus          A TextFileTextCorpus
    Outputs:
    symmetrized_model       A ThotSymmetrizedWordAlignmentModel trained on the corpus
    """
    src_trg_model = ThotFastAlignWordAlignmentModel()
    trg_src_model = ThotFastAlignWordAlignmentModel()
    symmetrized_model = ThotSymmetrizedWordAlignmentModel(src_trg_model, trg_src_model)
    symmetrized_model.heuristic = SymmetrizationHeuristic.GROW_DIAG_FINAL_AND
    trainer = symmetrized_model.create_trainer(corpus.lowercase())
    trainer.train(
        lambda status: print(
            f"Training Symmetrized FastAlign model: {status.percent_completed:.2%}"
        )
    )
    trainer.save()
    return symmetrized_model