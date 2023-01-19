import pandas as pd
import prepare_data
from pathlib import Path

with open('fixtures/hebrew_lemma_mini.txt') as f:
    src_data = f.readlines()

tokenized_src = prepare_data.create_tokens(src_data, vref_filepath=Path('../../../fixtures/vref.txt'))

with open('fixtures/en-NASB_mini.txt') as f:
    trg_data = f.readlines()

tokenized_trg = prepare_data.create_tokens(trg_data, vref_filepath=Path('../../../fixtures/vref.txt'))

combined_df = tokenized_src.drop('src_list', axis=1).join(tokenized_trg.drop(['vref', 'src_list'], axis=1).rename(columns={'src_tokenized': 'trg_tokenized'}))
print(combined_df.head(50))
combined_df = combined_df.rename(columns={'src_tokenized': 'src', 'trg_tokenized': 'trg'})
print(prepare_data.condense_files(combined_df))