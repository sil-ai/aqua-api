import argparse
from db_connect import get_session, VerseText
import pandas as pd
from datetime import datetime
from aqua_utils import get_logger

import os
import logging

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

class PullRevision:

    def __init__(self,):
        self.logger = get_logger()
        #gets the args of the form --revision 3 --out /path/to/output/file
        args = self.get_args()
        if not (args.revision and args.out):
            raise ValueError('Missing Revision Id or output path')
        #initializes the class variables
        self.revision_id = args.revision
        self.out = args.out
        self.revision_text = pd.DataFrame()
        #self.vref = self.prepare_vref()

    @staticmethod
    def prepare_vref():
        return open('./vref.txt').read().splitlines()

    def get_args(self):
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Pull and output verses from a revision')
        parser.add_argument('-r','--revision', type=int, help='Revision ID', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as se:
            if se.code == 2:
                raise ValueError('Argument error')
            else:
                self.logger.error(se.code)

    @staticmethod
    def is_duplicated(refs):
        return len(refs) != len(set(refs))

    def pull_revision(self):
        #with postgres connection gets the verses from the verseText table
        #??? Think about dividing get_session into get_engine and get_session
        __,session = next(get_session())
        self.logger.info(f'Loading verses from Revision {self.revision_id}...')
        #builds a dataframe of verses from the revision in self.revision_id
        revision_verses = pd.read_sql(session.query(VerseText).filter(VerseText.bibleRevision==self.revision_id).statement, session.bind)
        #??? Maybe rework as a try/except block? Seems convoluted
        if not revision_verses.empty:
            #checks that the version doesn't have duplicated verse references
            if not self.is_duplicated(revision_verses.verseReference):
                #loads the verses as part of the PullRevision object
                self.revision_text = revision_verses.set_index('id',drop=True)
                return self
            else:
                self.logger.info(f'Duplicated verses in Revision {self.revision_id}')
                return self
        else:
            self.logger.info(f'No verses for Revision {self.revision_id}')
            return self

    def output_revision(self):
        #saves the output as a csv file with revision_id and date
        if not self.revision_text.empty:
            date = datetime.now().strftime("%Y_%m_%d")
            self.revision_text.to_csv(self.out + f'/{self.revision_id}_{date}.csv')
            self.logger.info(f'Revision {self.revision_id} saved to file in location {self.out}')
        else:
            self.logger.info('Revision text is empty. Nothing printed.')

if __name__ == '__main__':
    try:
        pr = PullRevision()
        pr.pull_revision()
        pr.output_revision()
    except (ValueError, OSError, KeyError, AttributeError) as err:
        try:
            pr.logger.error(err)
        except NameError:
            print(err)
