__version__ = '0.102'

import argparse
from aqua_connect import get_aqua_conn
import pandas as pd
from datetime import datetime
       
class PullRevision:

    def __init__(self):
        #gets the args of the form --revision 3 --out /path/to/output/file
        args = self.get_args()
        if not (args.revision and args.out):
            raise ValueError('Missing Revision Id or output path')
        #initializes the class variables
        self.revision_id = args.revision
        self.out = args.out
        self.revision_text = None

    def get_args(self):
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Pull and output verses from a revision')
        parser.add_argument('-r','--revision', type=int, help='Revision ID')
        parser.add_argument('-o','--out', type=str, help='Output path')
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit:
            raise ValueError('Argument doesn\'t match type')

    def pull_revision(self):
        #with postgres connection gets the verses from the verseText table
        with get_aqua_conn() as conn:
            print(f'Loading verses from Revision {self.revision_id}...')
            #builds the sql string to get verses for a particular revision
            #!!! assumes the table and column names don't change
            sql_string = 'SELECT * FROM "verseText" WHERE "bibleRevision"=%s'
            #builds a dataframe of verses from the revision in self.revision_id
            #  protecting against a sql injection attack
            revision_verses = pd.DataFrame(conn.execute(sql_string, self.revision_id))
        #loads the verses as part of the PullRevision object
        if not revision_verses.empty:
            self.revision_text = revision_verses.set_index('id',drop=True)
            return self
        else:
            print(f'No verses for Revision {self.revision_id}')
            return self

    def output_revision(self):
        #saves the output as a csv file with revision_id and date
        if not self.revision_text.empty:
            date = datetime.now().strftime("%Y_%m_%d")
            self.revision_text.to_csv(self.out + f'/biblerevision{self.revision_id}_{date}.csv')
            print(f'Revision {self.revision_id} saved to file in location {self.out}')
        else:
            print('Revision text is empty. Nothing printed.')

if __name__ == '__main__':
    try:
        pr = PullRevision()
        pr.pull_revision()
        pr.output_revision()
    except (ValueError, OSError, KeyError) as err:
        print(err)
