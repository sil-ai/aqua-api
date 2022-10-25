import os
import json
import argparse
from datetime import datetime
import logging
from itertools import chain

import pandas as pd
from sem_sim_model import SemanticSimBa

logging.getLogger().setLevel('DEBUG')
#gets rid of the warning
#??? Is there an issue with the process getting forked?
os.environ['TOKENIZERS_PARALLELISM'] = 'false'

class SemanticSimilarity:

    def __init__(self):
        #gets args in the format --input /path/to/input_file
        # --out /path/to/output
        args = self.get_args()
        if not (args.input and args.out):
            raise ValueError('Missing argument path')
        #initializes the instance variables
        self.input_filename = args.input
        self.input = pd.read_csv(args.input)
        self.out_path = args.out
        #gets model locally or from huggingface
        #TODO: look for a faster way to load this model - maybe keep in memory
        #!!! https://medium.com/ibm-data-ai/how-to-load-pytorch-models-340-times-faster-with-ray-8be751a6944c
        self.sem_sim = SemanticSimBa()
        logging.info('Semantic model initialized...')

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser\
            (description='Checks semantic similarity between revision and reference')
        parser.add_argument('-i','--input', type=str, help='Input file path', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def get_sem_sims(self, precision=2):
        #get sem_sim predictions for input_file
        sem_sim_object =  self.sem_sim.predict(self.input['target'].to_list(),
                                               self.input['reference'].to_list())
        #prepares vrefs for merging
        vrefs = [[item] for item in self.input['vref'].to_list()]
        #zipping vrefs into sem_sim_object
        sem_sim_object1 = [list(chain(*item)) for item in zip(vrefs,sem_sim_object)]
        #turn the object into a dict
        keys = ['ref','sent1','sent2','score']
        sem_sims = [dict(zip(keys,item)) for item in sem_sim_object1]
        #round off score to precision digits
        sem_sims1 = [{**item,**{'score': round(item['score'],precision)}} for item in sem_sims]
        return sem_sims1

    def output_sem_sims(self,sem_sims):
        today = datetime.now()
        ver1,ver2 = self.input_filename.split('/')[-1].split('_')[:2]
        chunk_name = self.input_filename.split('_')[-1].split('.')[0]
        file_name = f'{ver1}_{ver2}_semsim_{chunk_name}_{today.month}_{today.day}.json'
        json.dump(sem_sims,open('/'.join([self.out_path,file_name]),'w'))
        logging.info('File %s output to %s', file_name ,self.out_path)

if __name__ == '__main__':
    try:
        ss = SemanticSimilarity()
        sem_sim = ss.get_sem_sims()
        ss.output_sem_sims(sem_sim)
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
