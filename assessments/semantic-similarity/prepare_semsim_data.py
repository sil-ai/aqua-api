import re
import pandas as pd

__version__ = 'v2'

def group_chapters(idx2):
    formatted_idx = [(f'{item[0]} {item[1]}',item[2]) for item in idx2]
    by_chapter = {}
    for item in formatted_idx:
        if item[0] not in by_chapter:
            by_chapter[item[0]] = {'range_verses':[int(item[1])]}
        else:
            by_chapter[item[0]]['range_verses'].append(int(item[1]))
    return by_chapter

def get_range_end(idx3):
    #get all verses that end a range including singles
    end_of_range=[]
    for idx in range(1,len(idx3)):
        if idx3[idx] - idx3[idx-1] != 1:
            end_of_range.append(idx3[idx-1])
        if idx == len(idx3)-1:
            end_of_range.append(idx3[-1])
    return end_of_range

def get_range_start(idx3):
    #get all verses that start a range
    start_of_range=[]
    for idx in range(1,len(idx3)):
        if idx3[idx] - idx3[idx-1] == 1:
            start_of_range.append(idx3[idx-1])
    return start_of_range

def pop_between_verses(start_of_range):
    #get all in between verses
    to_pop = []
    for idx in range(1,len(start_of_range)):
        if start_of_range[idx] - start_of_range[idx-1] == 1:
            to_pop.append(start_of_range[idx])

    #pop the in between verses
    for popper in reversed(to_pop):
        start_of_range.pop(start_of_range.index(popper))
    return start_of_range

def get_segments(start_of_range, end_of_range, idx3):
    #put it all together
    idx4 = []
    sor = iter(start_of_range)
    start = next(sor)
    for item in end_of_range:
        if start < item:
            #includes verse and range
            idx4.append((start-1,item))
            try:
                start = next(sor)
            except StopIteration:
                break
        else:
            #simple range of two
            idx4.append((item-1,item))
    #get the last item if not a range
    if idx4[-1][-1] != idx3[-1]:
        idx4.append((idx3[-1]-1, idx3[-1]))
    return idx4

def process_idx(idx_list):
    return [re.search(r'(.*) (\d+):(\d+)$',item).groups() for item in idx_list]

def prepare_data(df):
    df1 = df.copy()
    idx1= df1[df1['src'].str.contains('<range>')].index
    idx2 = process_idx(idx1)
    idx_dict = group_chapters(idx2)

    for chapter,idx3 in idx_dict.items():
        end_of_range = get_range_end(idx3['range_verses'])
        idx_dict[chapter]['range_end'] = end_of_range
        start_of_range = get_range_start(idx3['range_verses'])
        start_of_range_condensed = pop_between_verses(start_of_range)
        idx_dict[chapter]['range_start'] = start_of_range_condensed
        idx_dict[chapter]['segments'] = get_segments(start_of_range_condensed,
                                                    end_of_range,
                                                    idx3['range_verses'])

    for chapter, items in idx_dict.items():
        for start,end  in items['segments']:
            start_ref = f'{chapter}:{start}'
            end_ref = f'{chapter}:{end}'
            combined_verse =  ''.join(df.loc[start_ref:end_ref,:]['tar'].to_list())
            combined_ref = f'{chapter}:{start}-{end}'
            #update the dataframe
            #!!! Make sure not to update src!
            df1.loc[start_ref, 'tar'] = combined_verse
            df1.rename(index={start_ref:combined_ref},inplace=True)
    return df1[~ df1['src'].str.contains('<range>')]

if __name__ == '__main__':
    df = pd.read_pickle('./fixtures/bible_combined.pkl')
    condensed_df = prepare_data(df)