import os
import argparse
import logging
import torch
import pandas as pd
logging.getLogger().setLevel('DEBUG')
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
        for chunked_file in os.listdir(self.chunked_folder):
            #TODO: need to set the name for the chunk from the name
            list_of_chunks.append(pd.read_csv(self.chunked_folder + '/' + chunked_file))
        return list_of_chunks

if __name__ == '__main__':
    try:
        ss = SemanticSimilarity()
        chunks = ss.get_chunks()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
