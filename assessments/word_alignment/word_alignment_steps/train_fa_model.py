from pathlib import Path
from typing import Tuple

import pandas as pd
from machine.corpora import TextFileTextCorpus
from machine.translation import SymmetrizationHeuristic

from machine.translation.thot import (
    ThotFastAlignWordAlignmentModel,
    ThotSymmetrizedWordAlignmentModel,
)
from machine.tokenization import LatinWordTokenizer
from machine.corpora.parallel_text_corpus import ParallelTextCorpus



def create_corpus(condensed_df: pd.DataFrame) -> ParallelTextCorpus:
    """
    Takes two line-aligned condensed input files and produces a tokenized corpus. Note that this must be run on the
    output of write_condensed_files(), which removes the lines that are blank in either file.
    Inputs:
    condensed_df            A DataFrame with the vref, source and target texts

    Outputs:
    parallel_corpus     A tokenized TextFileTextCorpus
    """
    condensed_df[['src']].to_csv('condensed_src.txt', index=False, header=False)
    condensed_df[['trg']].to_csv('condensed_trg.txt', index=False, header=False)
    condensed_source = Path('condensed_src.txt')
    condensed_target = Path('condensed_trg.txt')
    with open(condensed_source, 'r') as f:
        lines = f.readlines()
    with open(condensed_source, 'w') as f:
        for line in lines:
            f.write(line.replace('"',''))
    with open(condensed_target, 'r') as f:
        lines = f.readlines()
    with open(condensed_target, 'w') as f:
        for line in lines:
            f.write(line.replace('"',''))

    source_corpus = TextFileTextCorpus(condensed_source)
    target_corpus = TextFileTextCorpus(condensed_target) if condensed_target else TextFileTextCorpus(condensed_source) # If there is no target, just get the corpus using the source twice
    parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(
        LatinWordTokenizer()
    )

    return parallel_corpus


def train_model(condensed_df: pd.DataFrame) -> Tuple[ThotSymmetrizedWordAlignmentModel, ParallelTextCorpus]:
    """
    Takes an aligned corpus as input and trains a model on that corpus
    Inputs:
    condensed_df            A DataFrame with the vref, source and target texts
    
    Outputs:
    symmetrized_model       A ThotSymmetrizedWordAlignmentModel trained on the corpus
    parallel_corpus         A ParallelTextCorpus made from condensed_df
    """
    parallel_corpus = create_corpus(condensed_df)
    src_trg_model = ThotFastAlignWordAlignmentModel()
    trg_src_model = ThotFastAlignWordAlignmentModel()
    symmetrized_model = ThotSymmetrizedWordAlignmentModel(src_trg_model, trg_src_model)
    symmetrized_model.heuristic = SymmetrizationHeuristic.GROW_DIAG_FINAL_AND
    trainer = symmetrized_model.create_trainer(parallel_corpus.lowercase())
    trainer.train(
        lambda status: print(
            f"Training Symmetrized FastAlign model: {status.percent_completed:.2%}"
        )
    )
    trainer.save()
    return symmetrized_model, parallel_corpus
