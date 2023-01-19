import os
import json
import argparse
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models import AssessmentResult

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
        #TODO: remove idx
        try:
            #sanity check that the assessment id is new
            assert self.assessment_is_new(self.assess_id), f"Result with assessment id {self.assess_id} exists"
            for idx,result in enumerate(self.input):
                print(idx)
                self.insert_item(result)
            self.session.commit()
            return 200, 'OK'
        except (IntegrityError, AssertionError) as err:
            self.session.rollback()
            return 500, err

    def assessment_is_new(self, assess_id):
        stmt = 'select * from "assessmentResult";'
        return len(list(filter(lambda item:item[1]==assess_id, self.session.execute(stmt)))) == 0
        #self.session.query(AssessmentResult).filter_by(assessment=self.assess_id).first() is None

    def insert_item(self, result):
        ar = AssessmentResult(assessment = self.assess_id,
                              ref=result['ref'],
                              score=result['score'])
        self.session.add(ar)
       

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