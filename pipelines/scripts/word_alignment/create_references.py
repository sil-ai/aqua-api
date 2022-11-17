from pathlib import Path

import pandas as pd

import get_data


def get_scores(outpath: Path):
    verse_scores = pd.read_csv(outpath / 'verse_scores.csv')
    word_scores = pd.read_csv(outpath / 'word_scores.csv')
    verse_scores = verse_scores[['vref', 'total_score']]
    word_scores = word_scores[['vref', 'source', 'target', 'total_score']]
    word_scores['target'] = word_scores['target'].apply(lambda x: x.replace(';', '";"')) # Otherwise the separation gets messed up in the CSV file
    return (word_scores, verse_scores)

def get_ref_scores(references, verse_df, word_df, source, base_outpath):
    references = [reference for reference in references if (base_outpath / f"{source.stem}_{reference}").exists()]

    for reference in references:
        outpath = base_outpath / f"{source.stem}_{reference}" 
        word_scores, verse_scores = get_scores(outpath)
        verse_df = verse_df.merge(verse_scores, how='left', on='vref').rename(columns={'total_score': reference})
        word_df = word_df.merge(word_scores, how='left', on=['vref', 'source']).rename(columns={'total_score': f'{reference}_score', 'target': f'{reference}_match'})
    verse_df['mean'] = verse_df.mean(axis=1)
    word_df['mean'] = word_df.mean(axis=1)

    verse_df['min'] = verse_df.iloc[:, 1:-1].min(axis=1)
    word_df['min'] = word_df.iloc[:, 1:-1].min(axis=1)
    print(verse_df)
    if len(references) > 1:
        verse_df['second_min'] = verse_df.drop(['vref', 'mean', 'min'], axis=1).apply(lambda row: sorted(list(row))[1], axis=1)
        word_df['second_min'] = word_df.drop(['vref', 'source', *[f'{reference}_match' for reference in references], 'mean', 'min'], axis=1).apply(lambda row: sorted(list(row))[1], axis=1)
    return verse_df, word_df


def main():
    all_references = [
        'en-NASB',
        'en-NIV84',
        'fr-LBS21',
        'swh-ONEN',
        'arb-AVD',
        'en-NLT07',
        'en-GNBUK',
        'es-NTV',
        'es-NVI99',
        'ko-RNKSV',
        'cho-CHTW',
        'en-KJV',
        'en-NIV11',
        'hop-hopNT',
        'malay_edited',
        'malay_baseline',
        # 'swhMICP-front',
        # 'wbi-wbiBT',
        # 'ndh-ndhBT'
    ]

    base_outpath = Path('data/out')
    source = Path('data/archive/greek_lemma.txt')
    # source = Path('data/archive/en-NIV11.txt')
    # source = Path('data/archive/swhMICP-front.txt')
    # source = Path('data/archive/wbi-wbiNT.txt')
    # source = Path('data/archive/ndh-ndhBT.txt')
    references = [
        'en-NASB',
        'fr-LBS21',
        'swh-ONEN',
        'arb-AVD',
        'ko-RNKSV',
        'es-NTV',
        
    ]


    df = get_data.get_ref_df(source, is_bible=True)
    df = get_data.remove_blanks_and_ranges(df)
    verse_df = df.drop('src', axis=1)
    df = get_data.get_words_from_txt_file(df, base_outpath)
    word_df = df.explode('src_words')[['vref', 'src_words']].rename(columns={'src_words': 'source'})
    
    ref_verse_df, ref_word_df = get_ref_scores(all_references, verse_df, word_df, source, base_outpath)
    ref_verse_df.to_csv(f'data/ref_data/{source.stem}_all_ref_verse_scores.csv', index=False)
    ref_word_df.to_csv(f'data/ref_data/{source.stem}_all_ref_word_scores.csv', index=False)
    ref_verse_df, ref_word_df = get_ref_scores(references, verse_df, word_df, source, base_outpath)
    ref_verse_df.to_csv(f'data/ref_data/{source.stem}_ref_verse_scores.csv', index=False)
    ref_word_df.to_csv(f'data/ref_data/{source.stem}_ref_word_scores.csv', index=False)


if __name__ == "__main__":
    main()