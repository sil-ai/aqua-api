import re
import argparse
import numpy as np
import pandas as pd
import logging
logging.getLogger().setLevel('DEBUG')

class SplitRevision:

    def __init__(self,):
        #gets the args of the form --input /path/to/my/input/file --num 100 --out /path/to/output
        args = self.get_args()
        if not (args.input and args.num and args.out):
            raise ValueError('Missing split number or path')
        #initializes the instance variables
        self.input_filepath = args.input
        self.output_filepath = args.out
        self.num = args.num
        self.revision_df = self.get_revision_file()

    def get_revision_file(self):
        return pd.read_csv(self.input_filepath)

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Split a revision into num parts')
        parser.add_argument('-i','--input', type=str, help='Input path', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path', required=True)
        parser.add_argument('-n','--num', type=int, help='Number of verses in a split section',required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def split_revision(self):
        #split revision list into roughly 'num' chunks
        logging.info(f'Splitting revision into {self.num} chunks...')
        return np.array_split(self.revision_df, self.num)

    def output_split_revisions(self, split_revisions):
        regex_string = r'(?!.*\/)(.*)\.csv'
        regex = re.compile(regex_string)
        input_filename = regex.search(self.input_filepath).groups()[0]
        #!!! Note chunk file numbering starts with zero
        for idx in range(self.num):
            output_filename = f'{self.output_filepath}/{input_filename}_chunk{idx}.csv'
            output_file = pd.DataFrame(split_revisions[idx])
            #outputs the idxth chunk of split_revisions to outputname file
            #without index and has headers
            output_file.to_csv(output_filename, index=False, header=True)
        logging.info(f'Revision chunks written to {self.output_filepath}')

if __name__ == '__main__':
    try:
        sr = SplitRevision()
        split_revisions = sr.split_revision()
        sr.output_split_revisions(split_revisions)
    except (ValueError, OSError,
            KeyError, AttributeError,
            TypeError,
            FileNotFoundError,
            IsADirectoryError) as err:
        logging.error(err)