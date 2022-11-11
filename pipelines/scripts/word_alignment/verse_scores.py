from pathlib import Path
import argparse

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from collections import OrderedDict


def plot_results(scores, highlighted=None):
    color = ['#1e81b0'] * len(scores)
    if highlighted:
        index = list(scores.keys()).index(highlighted)
        color[index] = '#e28743'
    fig, ax = plt.subplots()
    ax.bar(range(len(scores)), list(scores.values()), align='center', color=color)
    ax.set_xticks(range(len(scores)), list(scores.keys()), rotation='vertical')

    # plt.show()
    st.pyplot(fig)


def main():
    sources = ['greek_lemma', 'en-NIV11', 'swhMICP-front']
    source_stem = st.selectbox(label='Source', options=sources)

    # source = Path('data/archive/greek_lemma.txt')
    out_dir = Path('data/out')
    files = []
    for dir in out_dir.iterdir():
        if dir.is_dir() and f'{source_stem}_' in dir.parts[-1] and (dir / 'verse_scores.csv').exists():
            files.append(dir / 'verse_scores.csv')
    targets = []
    # target = Path('data/archive/greek_lemma.txt')
    targets = sorted([file.parts[-2].replace(f'{source_stem}_', '') for file in files])
    target_stem = st.selectbox(label='Translation', options=targets)
    if not source_stem or not target_stem:
        return
    ref_path = Path('data/ref_data')
    ref = pd.read_csv(ref_path / f'{source_stem}_ref_verse_scores.csv')
    ref = ref.rename(columns = {'mean': 'ref_mean'})
    to_exclude = ['vref', 'Unnamed: 0', 'mean', 'min', 'second_min', 'ref_mean']
    cols = [col for col in ref.columns if col not in to_exclude]
    st.header('Choose reference translations for comparison')
    ref_langs = {}
    all_ref_langs = st.checkbox('All')

    for col in sorted(cols):
        ref_langs[col] = st.checkbox(col)
    if all_ref_langs:
        ref_langs = {lang: True for lang in ref_langs}

    ref_langs = {key: value for key, value in ref_langs.items() if value}

    scores = {}
    for lang in ref_langs:
        scores[lang] = ref[lang].mean()
    
    verse_scores_file = out_dir / f'{source_stem}_{target_stem}' / 'verse_scores.csv'
    verse_df = pd.read_csv(verse_scores_file)
    verse_df = verse_df.merge(ref, how='left', on='vref')
    scores[target_stem] = verse_df['total_score'].mean()
    scores = OrderedDict(sorted(scores.items(), key=lambda item: item[1], reverse=True))
    st.header('Overall score')
    plot_results(scores, highlighted = target_stem)

    st.header('Detailed breakdown')

    calibrated = st.checkbox('Calibrate scores against other translations (to normalize verses that tend to score higher or lower across translations)?')
    calibrate_colors = st.checkbox('Calibrate colors, so they are relative to other scores within this text, rather than absolute?')

    sAggregationLevel = st.selectbox('View accuracy to the reference translation by:', ['Book/Chapter', 'Chapter/Verse']) 
    blnByBookChapter = sAggregationLevel == 'Book/Chapter'
    # base_outpath = Path('data/out')
    # outpath = base_outpath / f'{source.stem}_{target.stem}'
    verse_df['verse_score'] = verse_df['total_score'].apply(lambda x: round(x, 2))
    verse_df['verse_score_calibrated'] = verse_df.apply(lambda row: row['total_score'] / row['ref_mean'], axis=1)
    verse_df['DisplayBook'] = verse_df['vref'].apply(lambda x: x.split(' ')[0])
    verse_df['DisplayChapter'] = verse_df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[0]))
    verse_df['verse'] = verse_df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[1]))

    listVerses = verse_df.to_dict('records')

    chapter_df = verse_df.groupby(['DisplayBook', 'DisplayChapter'], sort=False).agg({'total_score': 'mean', 'ref_mean': 'mean'}).reset_index()
    chapter_df['chapter_score'] = chapter_df['total_score'].apply(lambda x: round(x, 2))
    chapter_df['chapter_score_calibrated'] = chapter_df.apply(lambda row: row['total_score'] / row['ref_mean'], axis=1)

    
    listChapters = chapter_df.to_dict('records')

    if blnByBookChapter:
        df = chapter_df
        if calibrated:
            display_col = 'chapter_score_calibrated'
        else:
            display_col = 'chapter_score'
    else:
        df = verse_df
        if calibrated:
            display_col = 'verse_score_calibrated'
        else:
            display_col = 'verse_score'
    if calibrate_colors:
        mid_color_score = df[display_col].mean()
    else:
        if calibrated:
            mid_color_score = 1
        else:
            mid_color_score = ref['ref_mean'].mean()
    print(mid_color_score)

    sCalculatedField = display_col
    color_config = {
        'field': sCalculatedField,
        'type': 'quantitative',
        'legend': {'title': '', 'direction': 'vertical', 'orient': 'left',
            "labelExpr": "{'0.5': 'Possible Issue', '2.0': 'Accurate to Reference'}[datum.label]",
            "labelFontSize": 18, "titleFontSize": 20},
        'scale': {'range': ['red', 'yellow', 'green'], 'domain': [0.85*mid_color_score, mid_color_score, 1.15*mid_color_score]},
    }

    books = list(verse_df['DisplayBook'].unique())

    if blnByBookChapter:
        displayList = listChapters
        st.markdown('')
        st.vega_lite_chart(displayList, {
            "width": "1300",
            'config': {
                'view': {
                    'strokeWidth': 0,
                    'step': 25
                },
                'axis': {
                    'domain': False
                }
            },
            'mark': {'type': 'rect'},
            'encoding': {
                'x': {
                    'field': 'DisplayChapter',
                    'title': 'Chapter',
                    'type': 'nominal',
                    'axis': False
                },
                'y': {
                    'field': 'DisplayBook',
                    'title': '',
                    'type': 'nominal',
                    'sort': books,
                    'axis': {'titleFontSize': 20, 'labelFontSize': 18}
                },
                'color': color_config,
                'tooltip': [
                    # {'field': sFirstVersionChoice.lower(), 'title': sFirstVersionChoice},
                    # {'field': sSecondVersionChoice.lower(), 'title': sSecondVersionChoice},
                    #{'field': sCalculatedField, 'aggregate': 'mean', 'title': 'Mean Score'},
                    # {'field': sCalculatedField, 'aggregate': 'min', 'title': 'Min Score'},
                    # {'field': sCalculatedField, 'aggregate': 'max', 'title': 'Max Score'},
                    {'field': 'DisplayBook', 'title': 'Book'},
                    {'field': 'DisplayChapter', 'aggregate': 'max', 'title': 'Chapter'},
                    {'field': sCalculatedField, 'aggregate': 'mean', 'title': 'Score', "format": ".3f"}
                ]
            }
        })
    else:
        displayList = listVerses
        sBookChoice = st.selectbox('Select Book', books) #set(df['book'])
        st.markdown('')
        # dfFiltered = df[df.book == sBookChoice]
        st.vega_lite_chart([o for o in displayList if o['DisplayBook'] == sBookChoice], {
            'width': '1300',
            'config': {
                'view': {
                    'strokeWidth': 0,
                    'step': 25
                },
                'axis': {
                    'domain': False
                }
            },
            'params': [
                {
                'name': 'highlight',
                'select': {'type': 'point', 'on': 'mouseover'}
                },
                # {'name': 'select', 'select': 'point'}
            ],
            'mark': {'type': 'rect'}, #, 'tooltip': {'content': 'data'}
            'encoding': {
                'x': {
                    'field': 'verse',
                    'type': 'nominal',
                    'axis': False
                },
                'y': {
                    'field': 'DisplayChapter',
                    'title': '',
                    'type': 'nominal',
                    'axis': {'titleFontSize': 20, 'labelFontSize': 18}
                },
                'color':  color_config,
                'tooltip': [
                    {'field': 'DisplayChapter', 'aggregate': 'max', 'title': 'Chapter'},
                    {'field': 'verse', 'aggregate': 'max', 'title': 'Verse (orig. versification)'},
                    #{'field': sFirstVersionChoice.lower(), 'title': sFirstVersionChoice},
                    #{'field': sSecondVersionChoice.lower(), 'title': sSecondVersionChoice},
                    {'field': sCalculatedField, 'title': 'Score'}
                ]
            },
            'strokeWidth': {
                'condition': [
                    # {
                    # 'param': 'select',
                    # 'empty': false,
                    # 'value': 2
                    # },
                    {
                    'param': 'highlight',
                    'empty': False,
                    'value': 1
                    }
                ],
                'value': 0
            }
        })

if __name__ == "__main__":
    main()