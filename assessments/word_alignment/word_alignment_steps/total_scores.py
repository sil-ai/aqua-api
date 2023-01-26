
import pandas as pd



def faster_df_apply(df, func):
    cols = list(df.columns)
    data, index = [], []
    for row in df.itertuples(index=True):
        row_dict = {f:v for f,v in zip(cols, row[1:])}
        data.append(func(row_dict))
        index.append(row[0])
    return pd.Series(data, index=index, dtype='object')


def run_total_scores(
                condensed_df: pd.DataFrame,
                alignment_scores_df: pd.DataFrame,
                avg_alignment_scores_df: pd.DataFrame, 
                translation_scores_df: pd.DataFrame,
                match_scores_df: pd.DataFrame,
                embedding_scores_df: pd.DataFrame,
                ):
    condensed_df.loc[:, 'source'] = condensed_df['src'].apply(lambda x: str(x).split())
    condensed_df.loc[:, 'target'] = condensed_df['trg'].apply(lambda x: str(x).split())
    condensed_df = condensed_df.explode('source').explode('target')
    alignment_scores_df['vref'] = alignment_scores_df['vref'].astype('object')  # Necessary for non-Bible, where vrefs are ints.

    alignment_scores_df = alignment_scores_df.merge(avg_alignment_scores_df, how = 'left', on=['source', 'target']).fillna(0)
    all_results = condensed_df.merge(alignment_scores_df, how='left', on=['vref', 'source', 'target']).fillna(0)

    all_results = all_results.merge(translation_scores_df, how='left', on=['source', 'target'])

    all_results.loc[:, 'avg_aligned'] = all_results.apply(lambda row: row['alignment_count'] / row['co-occurrence_count'], axis = 1).astype('float16')
    all_results.loc[:, 'translation_score'] = all_results.loc[:, 'translation_score'].apply(lambda x: 0 if x < 0.00001 else x).astype('float16')

    all_results = all_results.merge(match_scores_df, how='left', on=['source', 'target'])

    all_results = all_results.merge(embedding_scores_df, how='left', on=['source', 'target'])

    all_results.loc[:, 'total_score'] = faster_df_apply(all_results,lambda row: (row['avg_aligned'] + row['translation_score'] + row['alignment_score'] + row['match_score'] + row['embedding_score']) / 5)
    
    total_scores_df = all_results[['vref', 'source', 'target', 'total_score']]
    top_source_scores_df = total_scores_df.fillna(0)
    top_source_scores_df = top_source_scores_df.loc[top_source_scores_df.groupby(['vref', 'source'], sort=False)['total_score'].idxmax(), :].reset_index(drop=True)

    verse_scores_df = top_source_scores_df.groupby('vref', as_index=False, sort=False).mean()
    verse_scores_df = verse_scores_df.fillna(0)

    return total_scores_df, top_source_scores_df, verse_scores_df
