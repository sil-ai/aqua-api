import re
import argparse
import logging
import numpy as np
import pandas as pd

def get_logger():
    module_name = __file__.split('/')[-1].split('.')[0]
    #set the root logger to a debug level
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    #set up a stream handler
    s_handler = logging.StreamHandler()
    s_handler.setLevel(logging.DEBUG)
    log_format = logging.Formatter(fmt='%(asctime)s | %(levelname)s | %(message)s | %(name)s',
                                 datefmt='%Y-%m-%d %H:%M:%S')
    s_handler.setFormatter(log_format)
    logger.addHandler(s_handler)
    return logger

class SplitRevision:

    def __init__(self,):
        self.logger = get_logger()
        #gets the args of the form --input /path/to/my/input/file --num 100 --out /path/to/output
        args = self.get_args()
        if not (args.input and args.num and args.out):
            raise ValueError('Missing split number or path')
        #initializes the instance variables
        self.input_filepath = args.input
        self.output_filepath = args.out
        self.num = args.num
        revision_file = self.get_revision_file()
        self.revision_list = self.build_revision_list(revision_file)

    def get_revision_file(self):
        return open(self.input_filepath).read().splitlines()

    @staticmethod
    def build_revision_list(revision_file):
        #TODO: better way to pass vref to these classes
        vref = open('../pull_revision/vref.txt').read().splitlines()
        #put reference and verse together and strip out missing verses
        return [item for item in list(zip(vref,revision_file)) if item[1]]

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
        self.logger.info(f'Splitting revision into {self.num} chunks...')
        return np.array_split(self.revision_list, self.num)

    def output_split_revisions(self, split_revisions):
        regex_string = r'(?!.*\/)(.*)\.txt'
        regex = re.compile(regex_string)
        input_filename = regex.search(self.input_filepath).groups()[0]
        #!!! Note chunk file numbering starts with zero
        for idx in range(self.num):
            output_filename = f'{self.output_filepath}/{input_filename}_chunk{idx}.csv'
            output_file = pd.DataFrame(split_revisions[idx])
            #outputs the idxth chunk of split_revisions to outputname file
            #without index or headers
            output_file.to_csv(output_filename, index=False, header=False)
        self.logger.info(f'Revision chunks written to {self.output_filepath}')

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
        try:
            sr.logger.error(err)
        except NameError:
            print(err)
