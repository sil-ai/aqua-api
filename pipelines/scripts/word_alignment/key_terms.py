from pathlib import Path
import os
import argparse
import pandas as pd
from unicodedata import normalize
from tqdm import tqdm
tqdm.pandas()


def get_key_terms(kt_file, kt_vrefs_file, outpath):
    df = pd.read_csv(outpath / 'best_in_context.csv')
    key_terms = pd.read_csv(kt_file)
    key_terms_list = key_terms['word'].unique()
    key_terms_list = [normalize("NFC", term.lower()).split('-')[0].split(' (')[0] for term in key_terms_list]

    with open(kt_vrefs_file, 'r') as f:
        major_vrefs = f.read().splitlines()
        major_vrefs = [ref.split('\t') for ref in major_vrefs]
    all_major_vrefs = set([item for sublist in major_vrefs for item in sublist])
    
    df = df[df['source'].progress_apply(lambda x: x in key_terms_list)]
    df = df[df['vref'].progress_apply(lambda x: x in all_major_vrefs)]

    return df

def run_get_key_terms(
                        outpath: Path,
                        greek_kt_file: Path=Path('key_terms/greek_key_terms.csv'),
                        hebrew_kt_file: Path=Path('key_terms/hebrew_key_terms.csv'),
                        kt_vrefs_file:Path=Path('key_terms/Major-vrefs.txt'),
                        ):
    
    df = get_key_terms(greek_kt_file, kt_vrefs_file, outpath)
    df.to_csv(outpath / 'greek_key_terms.csv')
    df = get_key_terms(hebrew_kt_file, kt_vrefs_file, outpath)
    df.to_csv(outpath / 'hebrew_key_terms.csv')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--outpath", type=Path, help="source translation")
    args, unknown = parser.parse_known_args()

    run_get_key_terms(args.outpath)




# kt_words_file = 'key_terms/Major-metadata.txt'
