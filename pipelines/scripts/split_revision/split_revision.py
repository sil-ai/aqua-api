import argparse
import logging
import numpy as np

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
        self.revision_list = self.get_revision_file(args.input)
        self.out = args.out
        self.num = args.num
        self.split_revision = None

    @staticmethod
    def get_revision_file(filepath):
        return open(filepath).read().splitlines()

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
        self.split_revision = None

    def output_split_revisions(self):
        pass

if __name__ == '__main__':
    import ipdb; ipdb.set_trace()
    try:
        sr = SplitRevision()
        sr.split_revision()
        sr.output_split_revisions()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError,
            IsADirectoryError) as err:
        try:
            sr.logger.error(err)
        except NameError:
            print(err)
