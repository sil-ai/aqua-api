import os
import re
import argparse
import logging
logging.getLogger().setLevel('DEBUG')
#import torch
import pandas as pd
from itertools import chain
from sem_sim_model import SemanticSimBa

class SemanticSimilarity:

    def __init__(self,):
        #gets args in the format --chunked /path/to/chunked_folder
        # --out /path/to/output
        args = self.get_args()
        if not (args.chunked and args.out):
            raise ValueError('Missing argument path')
        #initializes the instance variables
        self.chunked_folder = args.chunked
        self.out_path = args.out
        #gets model locally or from huggingface
        self.sem_sim = SemanticSimBa()
        self.list_of_chunks = self.get_chunks()

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Checks semantic similarity between revision and reference')
        parser.add_argument('-c','--chunked', type=str, help='Chunked file path', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def get_chunks(self):
        list_of_chunks = []
        regex_string = r'chunk(.*)\.csv'
        regex = re.compile(regex_string)
        for chunked_file in os.listdir(self.chunked_folder):
            chunk_id = int(regex.search(chunked_file).groups()[0])
            chunk_df = pd.read_csv(self.chunked_folder + '/' + chunked_file)
            chunk_df.name = chunk_id
            list_of_chunks.append(chunk_df)
        #sort the list of chunks by chunk_id
        return sorted(list_of_chunks, key=lambda item:item.name)

    def get_sem_sim(self, chunk, precision=2):
        #get sem_sim prediction for chunk
        sem_sim_object =  self.sem_sim.predict(list(zip(chunk['target'], chunk['reference'])))
        #prepares vrefs for merging
        vrefs = [[item] for item in chunk['vref'].to_list()]
        #zipping vrefs into sem_sim_object
        sem_sim_object1 = [list(chain(*item)) for item in zip(vrefs,sem_sim_object)]
        #turn the object into a dict
        keys = ['ref','sent1','sent2','score']
        sem_sims = [dict(zip(keys,item)) for item in sem_sim_object1]
        #round off score to precision digits
        sem_sims1 = [{**item,**{'score': round(item['score'],precision)}} for item in sem_sims]
        return sem_sims1

    def process_sem_sim(self, chunks):
        sem_sims = []        
        for chunk in chunks:
            sem_sim = self.get_sem_sim(chunk)
            sem_sims.extend(sem_sim)
        return sem_sims

if __name__ == '__main__':
    try:
        ss = SemanticSimilarity()
        chunks = ss.get_chunks()
        sem_sims = ss.process_sem_sim(chunks[:2])
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
