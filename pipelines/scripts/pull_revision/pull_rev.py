from datetime import datetime
import logging
import argparse
import os
#TODO: make a decision about logging API-wide from Issue 40
logging.basicConfig(level=logging.DEBUG)

import numpy as np
import pandas as pd
from db_connect import get_session, VerseText


class PullRevision:

    def __init__(self,):
        #gets the args of the form --revision 3 --out /path/to/output/file
        args = self.get_args()
        if not (args.revision and args.out):
            raise ValueError('Missing Revision Id or output path')
        #initializes the instance variables
        self.revision_id = args.revision
        self.out = args.out
        self.revision_text = pd.DataFrame()
        self.vref = self.prepare_vref()

    @staticmethod
    def prepare_vref():
        #??? maybe consolidate the name to one variable?
        try:
            return pd.Series(open('./vref.txt').read().splitlines(), name='verseReference')
        except FileNotFoundError as err:
            raise FileNotFoundError(err) from err

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Pull and output verses from a revision')
        parser.add_argument('-r','--revision', type=int, help='Revision ID', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    @staticmethod
    def is_duplicated(refs):
        return len(refs) != len(set(refs))

    def pull_revision(self):
        #with postgres connection gets the verses from the verseText table
        #??? Think about dividing get_session into get_engine and get_session
        __, session = next(get_session())
        logging.info('Loading verses from Revision %s...', self.revision_id)
        #builds a dataframe of verses from the revision in self.revision_id
        revision_verses = pd.read_sql(session.query(VerseText)\
                          .filter(VerseText.bibleRevision==self.revision_id)\
                          .statement, session.bind)
        #??? Maybe rework as a try/except block? Seems convoluted
        if not revision_verses.empty:
            #checks that the version doesn't have duplicated verse references
            if not self.is_duplicated(revision_verses.verseReference):
                #loads the verses as part of the PullRevision object
                self.revision_text = revision_verses.set_index('id',drop=True)
            else:
                logging.info('Duplicated verses in Revision %s', self.revision_id)
        else:
            logging.info('No verses for Revision %s', self.revision_id)
        return self

    def prepare_output(self):
        #outer merges the vref list on the revision verses
        all_verses = pd.merge(self.revision_text,self.vref,on='verseReference',how='outer')
        #customed sort index
        vref_sort_index = dict(zip(self.vref,range(len(self.vref))))
        #map the sort order
        all_verses['sort_order'] = all_verses['verseReference'].map(vref_sort_index)
        #sort all verses based on vref custom sort
        all_verses.sort_values('sort_order', inplace=True)
        all_verses_text = all_verses['text'].replace(np.nan,'',regex=True)
        return all_verses_text.to_list()

    def output_revision(self):
        # Check whether the specified path exists or not
        isExist = os.path.exists(self.out)
        if not isExist:
            os.makedirs(self.out)
        date = datetime.now().strftime("%Y_%m_%d")
        #saves the output as a txt file with revision_id and unix date
        if not self.revision_text.empty:
            output_text = self.prepare_output()
            filename = f'{self.revision_id}_{date}.txt'
            filepath = self.out + '/' + filename
            with open(filepath,'w') as outfile:
                for verse_text in output_text:
                    outfile.write(f'{verse_text}\n')
            logging.info('Revision %s saved to file %s in location %s',
                              self.revision_id, filename, self.out)
        else:
            logging.info('Revision text is empty. Nothing printed.')

if __name__ == '__main__':
    try:
        pr = PullRevision()
        pr.pull_revision()
        pr.output_revision()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)