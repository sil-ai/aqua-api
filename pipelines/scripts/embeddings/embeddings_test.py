from typing import Optional
from pathlib import Path
import argparse
import json
import math

import pytest
import pandas as pd
import numpy as np

import get_data, embeddings


@pytest.mark.parametrize("sources,targets,is_bible", [
                                            (Path("fixtures/in"), Path("fixtures/in"), True), 
                                            ])
def test_get_embeddings(sources, targets, is_bible):
    for source_dir in sources.iterdir():
        meta_file = source_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        source = source_dir / f'{source_str}.txt'
        for target_dir in targets.iterdir():
            meta_file = target_dir / 'meta.json'
            with open(meta_file) as f:
                meta = json.load(f)
            target_str = meta['source']
            target = target_dir / f'{target_str}.txt'
            outpath = Path('fixtures/out') / f'{source_str}_{target_str}/'
            outpath.mkdir(parents=True, exist_ok=True)
            source_index_cache_file = source_dir / f'{source_str}-index-cache.json'
            target_index_cache_file = target_dir / f'{target_str}-index-cache.json'
            embeddings.get_embeddings( source, 
                            target, 
                            outpath, 
                            source_index_cache_file=source_index_cache_file, 
                            target_index_cache_file=target_index_cache_file, 
                            is_bible=is_bible,
                            weights_path=Path('./encoder_weights.txt'))
            assert (outpath / "embedding_scores.csv").exists()
            df = pd.read_csv(outpath / "embedding_scores.csv")
            assert df.shape[0] > 0
            assert df['embedding_score'].sum() > 0
