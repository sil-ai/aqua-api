__version__ = '0.101'

import argparse
from aqua_connect import get_aqua_conn
import pandas as pd
from datetime import datetime
       
class PullRevision:

    def __init__(self):
        args = self.get_args()
        if not (args.revision and args.out):
            raise ValueError('Missing Revision Id or output path')
        self.revision_id = args.revision
        self.out = args.out
        self.revision_text = None

    def get_args(self):
        parser = argparse.ArgumentParser(description='Pull and output verses from a revision')
        parser.add_argument('-r','--revision', type=int, help='Revision ID')
        parser.add_argument('-o','--out', type=str, help='Output path')
        try:
            return parser.parse_args()
        except SystemExit:
            raise ValueError('Argument doesn\'t match type')

    def pull_revision(self):
        with get_aqua_conn() as conn:
            print(f'Loading verses from Revision {self.revision_id}...')
            sql_string = 'SELECT * FROM "verseText" WHERE "bibleRevision"=%s'
            revision_verses = pd.DataFrame(conn.execute(sql_string, self.revision_id))
        if not revision_verses.empty:
            self.revision_text = revision_verses.set_index('id',drop=True)
            return self
        else:
            print(f'No verses for Revision {self.revision_id}')
            return self

    def output_revision(self):
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
    except (ValueError, OSError) as err:
        print(err)
