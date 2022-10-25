from pathlib import Path
import os
import argparse
import pandas as pd
from unicodedata import normalize
from tqdm import tqdm
tqdm.pandas()


def get_key_terms(kt_df, lang, outpath):
    df = pd.read_csv(outpath / 'best_in_context.csv')
    
    key_terms_list = kt_df[lang].unique()
    key_terms_list = [normalize("NFC", str(term).lower()).split('-')[0].split(' (')[0] for term in key_terms_list]

    # with open(kt_vrefs_file, 'r') as f:
    #     major_vrefs = f.read().splitlines()
    #     major_vrefs = [ref.split('\t') for ref in major_vrefs]
    # all_major_vrefs = set([item for sublist in major_vrefs for item in sublist])

    df = df[df['source'].progress_apply(lambda x: x in key_terms_list)]
    lang_col = 'greek_normalized' if lang == 'greek' else lang
    # df.loc[:, 'key_term_ref'] = df.progress_apply(lambda row: key_terms[key_terms['vrefs'] in row['vrefs'] and key_terms['word'] == row['source']], axis=1)[0]
    print(df.head())
    print(kt_df.head())
    # df.loc[:, 'key_term_ref'] = df[df['vref'] == kt_df['vrefs'] and df['source'] == kt_df[lang_col]]['line']
    df.loc[:, 'key_term_ref'] = df.progress_apply(lambda row: kt_df[(kt_df['vrefs'] == row['vref']) & (kt_df[lang_col] == row['source'])]['line'], axis=1)

    return df

def run_get_key_terms(
                        
                        base_outpath: Path,
                        target: Path,
                        NT_kt_file: Path=Path('key_terms/NT_key_terms.csv'),
                        OT_kt_file: Path=Path('key_terms/OT_key_terms.csv'),
                        ):
                        
    key_terms = pd.read_csv(NT_kt_file)
    # key_terms.loc[:, 'vrefs'] = key_terms['vrefs'].apply(lambda x: x.strip("[]").replace("'", "").split(', '))
    translations = {
        'english': 'en-NIV84',
        'spanish': 'es-NTV',
        # 'french': 'fr-LBS21',
        'greek': 'greek_lemma',

    }
    for lang in translations.keys():
        outpath = base_outpath / f'{translations[lang]}_{target.stem}'
        print(key_terms.head())
        df = get_key_terms(key_terms, lang, outpath)
        df.to_csv(outpath / 'NT_key_terms.csv')
        # df = get_key_terms(OT_kt_file, outpath)
        # df.to_csv(outpath / 'hebrew_key_terms.csv')

def get_best_key_terms(outpath, source_name, target_name):
    df = pd.read_csv(outpath / 'NT_key_terms.csv')
    # df.loc[:, 'key_term_ref'] = df['key_term_ref'].apply(lambda x: x.strip("[]").split())
    # df = df.explode('key_term_ref')
    best = df.groupby(['key_term_ref', 'target']).agg({'total_score': 'mean', 'source': 'first'}).sort_values('total_score', ascending=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--outpath", type=Path, help="base outpath")
    parser.add_argument("--target", type=Path, help="target translation")

    args, unknown = parser.parse_known_args()

    run_get_key_terms(args.outpath, args.target)




# kt_words_file = 'key_terms/Major-metadata.txt'
