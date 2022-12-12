from pathlib import Path
import argparse

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.colors as colors
import networkx as nx

from collections import OrderedDict
import numpy as np

import get_data


def plot_results(scores, source='Greek NT', highlighted=None):
    # Set the font family and size
    font_family = 'sans-serif'
    font_size = 9

    # Use the font manager to find the font
    font_prop = fm.FontProperties(family=font_family, size=font_size)

    plt.rc('font', family=font_prop.get_name())


    color = ['#00a7e1'] * len(scores)
    if highlighted:
        index = list(scores.keys()).index(highlighted)
        color[index] = '#FFB71B'
    fig, ax = plt.subplots()
    # Set the font for the current plot
    ax.bar(range(len(scores)), list(scores.values()), align='center', color=color)
    ax.set_xticks(range(len(scores)), [text_names.get(name, name) for name in list(scores.keys())], rotation='vertical', fontproperties=font_prop)
    ax.tick_params(axis='both', which='major', labelsize=font_size)
    plt.title(f'Extent of formal equivalence to the {text_names.get(source, source)} source text', fontproperties=font_prop)

    # plt.show()
    st.pyplot(fig)


def split_Psalm_books(row):
    if row['DisplayBook'] == 'PSA':
        if row['DisplayChapter'] <=41:
            return 'PSA 1-41'
        if row['DisplayChapter'] <=72:
            return 'PSA 42-72'
        if row['DisplayChapter'] <=89:
            return 'PSA 73-89'
        if row['DisplayChapter'] <=106:
            return 'PSA 90-106'
        return 'PSA 107-150'
    return row['DisplayBook']


def adjust_psalm_chapters(row):
    if row['DisplayBook'][:3] == 'PSA':
        if int(row['DisplayChapter']) <=41:
            return int(row['DisplayChapter'])
        if int(row['DisplayChapter']) <=72:
            return int(row['DisplayChapter']) - 41
        if int(row['DisplayChapter']) <=89:
            return int(row['DisplayChapter']) - 72
        if int(row['DisplayChapter']) <=106:
            return int(row['DisplayChapter']) - 89
        return int(row['DisplayChapter']) - 106
    return int(row['DisplayChapter'])


text_names = {
    'greek_lemma': 'Greek',
    'en-NASB': 'NASB',
    'en-KJV': 'KJV',
    'en-ESVUK': 'ESV',
    'en-RSV': 'RSV',
    'en-GNBUK': 'GNB',
    'en-NIV11': 'NIV',
    'en-NLT07': 'NLT',
    'en-CEVUS06': 'CEV',
}


@st.experimental_memo
def get_df(text_df_file):
    df = pd.read_csv(text_df_file)
    return df

@st.experimental_memo
def get_condensed_df(source_str, target_str, outpath):
    ref_df = get_data.get_ref_df(outpath / f'{source_str}.txt', outpath / f'{target_str}.txt')
    condensed_df = get_data.condense_files(ref_df)
    condensed_df = get_data.get_words_from_txt_file(condensed_df, outpath)
    return condensed_df


def update_verse(new_verse, chapters, verses):
    if new_verse in verses:
        st.session_state['verse'] = new_verse
    elif st.session_state['chapter'] + 1 in chapters:
        st.session_state['chapter'] += 1
        st.session_state['verse'] = 1
    else:
        st.session_state['chapter'] = 1
        st.session_state['verse'] = 1


def initialize_session_state(start_book):
    if 'verse' not in st.session_state:
        st.session_state['book'] = start_book
        st.session_state['chapter'] = 1
        st.session_state['verse'] = 1


def truncate_colormap(cmap, minval=0.0, maxval=1.0, n=100):
    new_cmap = colors.LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
        cmap(np.linspace(minval, maxval, n)))
    return new_cmap


def main(args):
    #select analysis
    with st.sidebar:
        analysis = st.radio('Select Analysis', ['Similarity', 'Readability', 'Dynamicity', 'Word Alignment', 'Missing Words'])
        sources = ['greek_lemma', 'en-NIV11', 'swhMICP-front', 'wbi-wbiNT', 'ndh-ndhBT', 'ntk-ntk', 'swhMFT-front']
        out_dir = args.outpath
        source_str = st.selectbox(label='Source', options=sources)
        files = []
        for dir in out_dir.iterdir():
            if dir.is_dir() and f'{source_str}_' in dir.parts[-1] and (dir / 'verse_scores.csv').exists():
                files.append(dir / 'verse_scores.csv')
        targets = []
        ref_mean = 0.45
        targets = sorted([file.parts[-2].replace(f'{source_str}_', '') for file in files])
        target_str = st.selectbox(label='Translation', options=targets)
        outpath = out_dir / f'{source_str}_{target_str}'
        df = get_df(outpath / 'by_verse_scores.csv')
        condensed_df = get_condensed_df(source_str, target_str, outpath)
        books = condensed_df['vref'].apply(lambda x: x.split(' ')[0]).unique()
        chapters = condensed_df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[0])).unique()
        verses = condensed_df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[1])).unique()
        initialize_session_state(books[0])

    if not source_str or not target_str:
        return
    #WORD ALIGNMENT
################################################################################################################################################################################
    if analysis == 'Word Alignment':
        
        st.sidebar.selectbox('Book', books, key = 'book', index = list(books).index(st.session_state['book']))
        st.sidebar.selectbox('Chapter', chapters, key = 'chapter', index = list(chapters).index(st.session_state['chapter']))
        st.sidebar.selectbox('Verse', verses, key = 'verse', index = list(verses).index(st.session_state['verse']))
        book = st.session_state['book']
        chapter = st.session_state['chapter']
        verse = st.session_state['verse']

        vref = f'{book} {str(chapter)}:{str(verse)}'
        print(vref)
        st.title(vref)
        words = condensed_df[condensed_df['vref'] == vref]
        sen = df[df['vref'] == vref]
        src_words = words.iloc[0]['src_words']
        trg_words = words.iloc[0]['trg_words']

        # Create the network diagram using NetworkX
        G = nx.Graph()
        node_labels = {}
        trg_exclude = [',', '.']
        trg_words = [word for word in trg_words if word not in trg_exclude]
        # Add nodes for each word in the two sentences
        for i, word in enumerate(src_words):
            G.add_node(f'{i}-{word}')
            node_labels[f'{i}-{word}'] = word
        for i, word in enumerate(trg_words):
            G.add_node(f'{i}-{word}')
            node_labels[f'{i}-{word}'] = word

        # Add edges between words that have a high similarity
        for i, word1 in enumerate(src_words):
            for j, word2 in enumerate(trg_words):
                # if similarity_matrix[i, j] > 0.2:
                weight = sen[(sen['source'] == word1) & (sen['target'] == word2)]['total_score'].values[0]
                # print(weight)
                if weight > 0.25:
                    G.add_edge(f'{i}-{word1}', f'{j}-{word2}', weight=weight)
        

        # Define the positions of the nodes manually
        pos = {}
        label_pos = {}

        # Place the words in the first sentence in a horizontal line
        y = 0
        for i, word in enumerate(src_words):
            pos[f'{i}-{word}'] = (y+0.07, -i)
            label_pos[f'{i}-{word}'] = (y, -i)

        # Place the words in the second sentence in a horizontal line below the first
        y = 1
        for i, word in enumerate(trg_words):
            pos[f'{i}-{word}'] = (0.9*y, -i)
            label_pos[f'{i}-{word}'] = (y, -i)

        fig, ax = plt.subplots()
        # Draw the nodes and edges, coloring the edges based on their weight
        nx.draw(G, label_pos, edge_color=[G[u][v]["weight"] for u, v in G.edges()], edge_cmap=plt.cm.YlOrRd, node_size = 1000, alpha=0)

        cmap = truncate_colormap(plt.cm.YlOrRd, 0.3, 1.0)
        # Add labels to the nodes
        nx.draw_networkx_edges(G, pos, edge_color=[G[u][v]["weight"] for u, v in G.edges()], edge_cmap=cmap)
        # nx.draw_networkx_nodes(G, pos)
        nx.draw_networkx_labels(G, label_pos, labels=node_labels, font_size = 8)
        # Show the plot
        st.pyplot(fig)
        col1, col2 = st.columns(2, gap='large')
        with col1:
            st.button("Previous verse", key = "previous-verse", on_click=update_verse, kwargs = {'new_verse': st.session_state['verse'] - 1, 'chapters': chapters, 'verses': verses})
        with col2:
            st.button("Next verse", key = "next-verse", on_click=update_verse, kwargs = {'new_verse': st.session_state['verse'] + 1, 'chapters': chapters, 'verses': verses})


    #DYNAMICITY
################################################################################################################################################################################
    if analysis == 'Dynamicity':
        
        ref_path = Path('data/ref_data')
        ref_path = ref_path / f'{source_str}/{source_str}_all_ref_verse_scores.csv'
        ref_langs = {}
        verse_scores_file = out_dir / f'{source_str}_{target_str}' / 'verse_scores.csv'
        verse_df = pd.read_csv(verse_scores_file)
        if ref_path.exists():
            all_ref = pd.read_csv(ref_path)
            all_ref = all_ref.rename(columns = {'mean': 'ref_mean'})
            to_exclude = ['vref', 'Unnamed: 0', 'mean', 'min', 'second_min', 'ref_mean']
            cols = [col for col in all_ref.columns if col not in to_exclude]
            st.header('Choose reference translations for comparison')
            all_ref_langs = st.checkbox('All')
            for col in sorted(cols):
                ref_langs[col] = st.checkbox(col)
            if all_ref_langs:
                ref_langs = {lang: True for lang in ref_langs}
            ref_langs = {key: value for key, value in ref_langs.items() if value}
            verse_df = verse_df.merge(all_ref, how='left', on='vref')
            ref_mean = all_ref['ref_mean'].mean()

        scores = {}
        for lang in ref_langs:
            scores[lang] = all_ref[lang].mean()
        scores[target_str] = verse_df['total_score'].mean()
        
        scores = OrderedDict(sorted(scores.items(), key=lambda item: item[1], reverse=True))
        st.header('Overall score')
        highlight_target = st.checkbox('Highlight the target translation as a different color?')
        highlighted = target_str if highlight_target else None
        plot_results(scores, source=source_str, highlighted = highlighted)

        st.header('Detailed breakdown')

        calibrated = st.checkbox('Calibrate scores against other translations (to normalize verses that tend to score higher or lower across translations)?')
        calibrate_colors = st.checkbox('Calibrate colors, so they are relative to other scores within this text, rather than absolute?')


        sAggregationLevel = st.selectbox('View accuracy to the reference translation by:', ['Book/Chapter', 'Chapter/Verse']) 
        blnByBookChapter = sAggregationLevel == 'Book/Chapter'

        verse_df['verse_score'] = verse_df['total_score']
        if calibrated:
            if not 'ref_mean' in verse_df.columns:
                st.write('No reference translations for this source. Uncheck the box for calibrating against other translations.')
                return
            verse_df['verse_score_calibrated'] = verse_df.apply(lambda row: row['total_score'] / row['ref_mean'], axis=1)
        verse_df['DisplayBook'] = verse_df['vref'].apply(lambda x: x.split(' ')[0])
        verse_df['DisplayChapter'] = verse_df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[0]))
        verse_df['DisplayBook'] = verse_df.apply(split_Psalm_books, axis=1)
        verse_df['DisplayChapter'] = verse_df.apply(adjust_psalm_chapters, axis=1)

        verse_df['verse'] = verse_df['vref'].apply(lambda x: int(x.split(' ')[1].split(':')[1]))

        listVerses = verse_df.to_dict('records')

        chapter_df = verse_df.groupby(['DisplayBook', 'DisplayChapter'], sort=False).agg({k:v for k,v in {'total_score': 'mean', 'ref_mean': 'mean'}.items() if k in verse_df}).reset_index()
        chapter_df['chapter_score'] = chapter_df['total_score']
        if calibrated:
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
                mid_color_score = ref_mean
        min_color_score = mid_color_score*0.66
        max_color_score = mid_color_score*1.2

        sCalculatedField = display_col
        # mid_color_score=0.5
        color_config = {
            'field': sCalculatedField,
            'type': 'quantitative',
            'legend': { 'title': '', 
                        'direction': 'vertical', 
                        'orient': 'left',
                        'values': [min_color_score, mid_color_score, max_color_score],
                        "labelExpr": f"{{ {min_color_score}: 'More dynamic', {max_color_score}: 'More formally equivalent'}}[datum['value']]",
                        # "labelExpr": "datum.value",
                        # "labelFlush":True,
                        "labelFontSize": 18, 
                        "titleFontSize": 20
                        },
            'scale': {'range': ['blue', 'white', 'orange'], 'domain': [min_color_score, mid_color_score, max_color_score]},
        }

        books = list(verse_df['DisplayBook'].unique())
        if blnByBookChapter:
            displayList = listChapters
            st.markdown('')
            st.vega_lite_chart(displayList, {"$schema": "https://vega.github.io/schema/vega-lite/v5.json",
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

    #Missing words
    ################################################################################################################################################################################
    elif analysis == 'Missing Words':
        data_dir = out_dir / f'{source_str}_{target_str}'
        print(data_dir)
        if (data_dir / 'red_flags.csv').exists():
            df = pd.read_csv(data_dir / 'red_flags.csv')
            references = [item.replace('_match', '') for item in list(df.columns) if '_match' in item]
            print(references)
            
            print(analysis)
            st.dataframe(df[['vref', 'source', 'en-NASB_match']].rename(columns={'en-NASB_match': 'English', 'vref': 'Verse Reference', 'source': 'Greek'}), width=800, height=1000)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outpath", type=Path, default=Path("data/out/"), help="Directory where output files are stored")

    args = parser.parse_args()

    main(args)
