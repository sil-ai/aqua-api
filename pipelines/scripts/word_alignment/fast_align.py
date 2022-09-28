#imports
#!pip install sil-machine[thot]
import argparse
from tqdm import tqdm
import pandas as pd
import string
import os
from machine.corpora import TextFileTextCorpus
from machine.tokenization import LatinWordTokenizer
from machine.translation import SymmetrizationHeuristic
from machine.translation.thot import ThotFastAlignWordAlignmentModel, ThotSymmetrizedWordAlignmentModel

#command line args
parser = argparse.ArgumentParser(description="Argparser")
parser.add_argument('--source', type=str, help='source translation')
parser.add_argument('--target', type=str, help='target translation')
parser.add_argument('--threshold', type=float, default=0.5, help='word score threshold {0,1}')
parser.add_argument('--outpath', type=str, help='where to write results')
args, unknown = parser.parse_known_args()

#source and target files
src_file = args.source
trg_file = args.target

#remove any empty lines from the source and target files
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

def write_condensed_file(data, filename, max_lines=999999):
    with open(filename, 'w') as f:
        for num, line in enumerate(data):
            line = line.translate(str.maketrans('', '', string.punctuation))
            if num < max_lines:
                if num == 0:
                    f.write(line.strip().lower())
                else:
                    f.write('\n' + line.strip().lower())

def get_empty_lines(file):
    empty_lines = []
    with open(file) as f:
        for num, line in enumerate(f):
            if line == '\n':
                empty_lines.append(num)
    return empty_lines

remove_empty_lines(src_file, trg_file)

#create parallel corpus
source_corpus = TextFileTextCorpus(src_file + '_condensed')
target_corpus = TextFileTextCorpus(trg_file+ '_condensed')
parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(LatinWordTokenizer())

# Train fast_align model
src_trg_model = ThotFastAlignWordAlignmentModel()
trg_src_model = ThotFastAlignWordAlignmentModel()
symmetrized_model = ThotSymmetrizedWordAlignmentModel(src_trg_model, trg_src_model)
symmetrized_model.heuristic = SymmetrizationHeuristic.GROW_DIAG_FINAL_AND
trainer = symmetrized_model.create_trainer(parallel_corpus.lowercase())
trainer.train(lambda status: print(f"Training Symmetrized FastAlign model: {status.percent_completed:.2%}"))
trainer.save()

# Align the sentences in the parallel corpus and write to df
alignments = symmetrized_model.get_best_alignment_batch(parallel_corpus.lowercase().to_tuples())
data = {'source':[], 'target':[], 'word score':[], 'verse score':[]}
print("Getting alignments...")
for source_segment, target_segment, alignment in tqdm(alignments):
    pair_indices = alignment.to_aligned_word_pairs()
    verse_score = symmetrized_model.get_avg_translation_score(source_segment, target_segment, alignment)
    for pair in pair_indices:
      score = symmetrized_model.get_translation_score(source_segment[pair.source_index], target_segment[pair.target_index])
      data['source'].append(source_segment[pair.source_index])
      data['target'].append(target_segment[pair.target_index])
      data['word score'].append(score)
      data['verse score'].append(verse_score)
df = pd.DataFrame(data)

# remove duplicates and average out verse and word scores
dups = df.groupby(['source', 'target']).size().reset_index()
avgs = df.groupby(['source', 'target']).mean().reset_index()
no_dups = pd.merge(dups, avgs)
no_dups.rename(columns={0: "count"}, inplace=True)

#apply threshold
no_dups = no_dups[no_dups['word score'] >= args.threshold]

#write results to csv
source_name = os.path.basename(args.source)
target_name = os.path.basename(args.target)
path = args.outpath + "/" + source_name.split('.')[0] + "_" + target_name.split('.')[0]
os.makedirs(path)
no_dups.to_csv(path + "/sorted.csv")
df.to_csv(path + "/in_context.csv")

#delete temp files
os.remove(src_file + '_condensed')
os.remove(trg_file + '_condensed')

