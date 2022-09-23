#!/usr/bin/env python
# coding: utf-8

# Before running this notebook, make sure that you upload the source and target corpora. The corpora should be a text file where each line is a separate sentence. The source and target sentences should be aligned by line number. The source file should be called `src.txt` and the target file should be called `trg.txt`.
# 
# 

# Install Machine.py

# In[1]:


#get_ipython().system('pip install sil-machine[thot]')


# Create parellel corpus from source and target corpora

# In[9]:


import argparse
from tqdm import tqdm

#command line args
parser = argparse.ArgumentParser(description="Argparser")
parser.add_argument('--source', type=str, help='source translation')
parser.add_argument('--target', type=str, help='target translation')
parser.add_argument('--outfile', type=str, help='file to write to (csv)')
args, unknown = parser.parse_known_args()


# In[ ]:


from machine.corpora import TextFileTextCorpus
from machine.tokenization import LatinWordTokenizer
src_file = args.source
trg_file = args.target


def remove_empty_lines(src_file, trg_file, max_lines=999999):
    empty_lines = list(set(get_empty_lines(src_file)) | set(get_empty_lines(trg_file)))
    with open(src_file) as f:
        src_data = f.readlines()
    with open(trg_file) as f:
        trg_data = f.readlines()
    src_data = [line for num, line in enumerate(src_data) if num not in empty_lines]
    trg_data = [line for num, line in enumerate(trg_data) if num not in empty_lines]
    write_condensed_file(src_data, src_file + "_condensed", max_lines=max_lines)
    write_condensed_file(trg_data, trg_file + "_condensed", max_lines=max_lines)

import string
def write_condensed_file(data, filename, max_lines=999999):
    with open(filename, 'w') as f:
        for num, line in enumerate(data):
            line = line.translate(str.maketrans('', '', string.punctuation))
            if num < max_lines:
                if num == 0:
                    f.write(line.strip())
                else:
                    f.write('\n' + line.strip())

def get_empty_lines(file):
    empty_lines = []
    with open(file) as f:
        for num, line in enumerate(f):
            if line == '\n':
                empty_lines.append(num)
    return empty_lines

remove_empty_lines(src_file, trg_file)

source_corpus = TextFileTextCorpus(src_file + '_condensed')
target_corpus = TextFileTextCorpus(trg_file+ '_condensed')

# source_corpus = TextFileTextCorpus(src_file)
# target_corpus = TextFileTextCorpus(trg_file)
parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(LatinWordTokenizer())


# Train FastAlign model

# In[ ]:


from machine.translation import SymmetrizationHeuristic
from machine.translation.thot import ThotFastAlignWordAlignmentModel, ThotSymmetrizedWordAlignmentModel

src_trg_model = ThotFastAlignWordAlignmentModel()
trg_src_model = ThotFastAlignWordAlignmentModel()
symmetrized_model = ThotSymmetrizedWordAlignmentModel(src_trg_model, trg_src_model)
symmetrized_model.heuristic = SymmetrizationHeuristic.GROW_DIAG_FINAL_AND
trainer = symmetrized_model.create_trainer(parallel_corpus.lowercase())
trainer.train(lambda status: print(f"Training Symmetrized FastAlign model: {status.percent_completed:.2%}"))
trainer.save()


# Align the sentences in the parallel corpus

# In[ ]:

alignments = symmetrized_model.get_best_alignment_batch(parallel_corpus.lowercase().to_tuples())

data = {'source':[], 'target':[], 'word score':[], 'verse score':[]}

for source_segment, target_segment, alignment in tqdm(alignments):
    pair_indices = alignment.to_aligned_word_pairs()
    verse_score = symmetrized_model.get_avg_translation_score(source_segment, target_segment, alignment)
    for pair in pair_indices:
      data['source'].append(source_segment[pair.source_index])
      data['target'].append(target_segment[pair.target_index])
      score = symmetrized_model.get_translation_score(source_segment[pair.source_index], target_segment[pair.target_index])
      data['word score'].append(score)
      data['verse score'].append(verse_score)
        
import pandas as pd

df = pd.DataFrame(data)

df_no_dup = df.drop_duplicates(subset = ['source', 'target'])
sources = df_no_dup['source'].tolist()
targets = df_no_dup['target'].tolist()

occurrences_list = []
avg_word_scores = []
avg_verse_scores = []

for i in tqdm(range(len(df_no_dup))):
  #count how many times the pair occurs
  occurrences = df.loc[(df['source'] == sources[i]) & (df['target'] == targets[i])]
  num_occurrences = len(occurrences)
  occurrences_list.append(num_occurrences)

  #average out scores
  avg_word_scores.append(occurrences['word score'].mean())
  avg_verse_scores.append(occurrences['verse score'].mean())
  
df_no_dup['word score'] = avg_word_scores
df_no_dup['verse score'] = avg_verse_scores
df_no_dup['num occurrences'] = occurrences_list

df_no_dup.to_csv(args.outfile)

#print(df.iloc[10000:10100])

