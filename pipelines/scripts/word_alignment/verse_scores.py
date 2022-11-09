from pathlib import Path
import argparse

import streamlit as st
import pandas as pd


def main():
    source = Path('data/archive/greek_lemma.txt')
    data_dir = Path('data/archive')
    files = []
    for entry in data_dir.iterdir():
        if entry.is_file():
            files.append(entry)
    
    # target = Path('data/archive/greek_lemma.txt')
    target = st.selectbox(label='Translation', options=files)
    base_outpath = Path('data/out')
    ref_path = Path('data/ref_data')
    outpath = base_outpath / f'{source.stem}_{target.stem}'
    df = pd.read_csv(outpath / 'verse_scores.csv')
    ref = pd.read_csv(ref_path / f'{source.stem}_ref_scores.csv')
    ref = ref.rename(columns = {'mean': 'ref_mean'})
    df = df.merge(ref, how='left', on='vref')
    df['verse_score'] = df['total_score'].apply(lambda x: round(x, 2))
    df['verse_score_calibrated'] = df.apply(lambda row: row['verse_score'] / row['ref_mean'], axis=1)
    df['DisplayBook'] = df['vref'].apply(lambda x: x.split(' ')[0])
    df['DisplayChapter'] = df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[0]))
    df['verse'] = df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[1]))
    avg_verse_score = df['verse_score_calibrated'].mean()
    # df['total_score'] = df['total_score']*10.0

    listVerses = df.to_dict('records')

    reftrans = 'NTV'
    displayList = listVerses
    sCalculatedField = 'verse_score_calibrated'
    color_config = {
        'field': sCalculatedField,
        'type': 'quantitative',
        'legend': {'title': '', 'direction': 'vertical', 'orient': 'left',
            "labelExpr": "{'0.5': 'Possible Issue', '2.0': 'Accurate to Reference'}[datum.label]",
            "labelFontSize": 18, "titleFontSize": 20},
        'scale': {'range': ['red', 'yellow', 'green'], 'domain': [0.8*avg_verse_score, avg_verse_score, 1.2*avg_verse_score]},
    }

    sAggregationLevel = st.selectbox('View accuracy to the reference translation by:', ['Book/Chapter', 'Chapter/Verse']) 
    blnByBookChapter = sAggregationLevel == 'Book/Chapter'
    sFirstVersionChoice = reftrans

    listBooksInCanonicalOrder = ['MAT', 'MRK', 'LUK', 'JHN', 'ACT', 'ROM', '1CO', '2CO', 'GAL', 'EPH', 'PHP', 'COL', '1TH', '2TH', '1TI', '2TI', 'TIT', 'PHM', 'HEB', 'JAS', '1PE', '2PE', '1JN', '2JN', '3JN', 'JUD', 'REV']

    if blnByBookChapter:
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
                    'sort': listBooksInCanonicalOrder,
                    'axis': {'titleFontSize': 20, 'labelFontSize': 18}
                },
                'color': color_config,
                'tooltip': [
                    # {'field': sFirstVersionChoice.lower(), 'title': sFirstVersionChoice},
                    # {'field': sSecondVersionChoice.lower(), 'title': sSecondVersionChoice},
                    #{'field': sCalculatedField, 'aggregate': 'mean', 'title': 'Mean Score'},
                    #{'field': sCalculatedField, 'aggregate': 'min', 'title': 'Min Score'},
                    #{'field': sCalculatedField, 'aggregate': 'max', 'title': 'Max Score'}
                    {'field': 'DisplayBook', 'title': 'Book'},
                    {'field': 'DisplayChapter', 'aggregate': 'max', 'title': 'Chapter'},
                    {'field': sCalculatedField, 'aggregate': 'mean', 'title': 'Score', "format": ".3f"}
                ]
            }
        })
    else:
        sBookChoice = st.selectbox('Select Book', listBooksInCanonicalOrder) #set(df['book'])
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