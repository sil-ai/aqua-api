import os
import json
import argparse
import logging
from models import AssessmentResult
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

logging.getLogger().setLevel('DEBUG')
#input -> [{**item,**{'score': round(item['score'],precision)}} for item in sem_sims]

class BulkInsert():

    def __init__(self):
        engine = create_engine(os.environ['AQUA_CONNECTION_STRING'])
        self.session = Session(engine)
        args = self.get_args()
        self.input = json.load(open(args.input))
        self.assess_id = int(args.assess_id)

    def __del__(self):
        self.session.close()

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser\
            (description='Bulk inserts assessment results')
        parser.add_argument('-i','--input', type=str, help='Input file path', required=True)
        parser.add_argument('-a','--assess_id', type=int, help='Assessment Id', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def insert(self):
        for result in self.input:
            self.insert_item(result)

    def insert_item(self, result):
        ar = AssessmentResult(assessment = self.assess_id,
                              ref=result['ref'],
                              score=result['score'])
        print(result['ref'])
        self.session.add(ar)
        self.session.commit()

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('../switchboard/.env')
    try:
        bi = BulkInsert()
        result = bi.insert()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
    
