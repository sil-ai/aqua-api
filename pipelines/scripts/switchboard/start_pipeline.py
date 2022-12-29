import os
import json
import argparse
import logging
#from assessment_operations import InitiateAssessment
from sqlalchemy.exc import IntegrityError

logging.getLogger().setLevel('DEBUG')

class StartPipeline:

    def __init__(self):
        args = self.get_args()
        if not (args.ref and args.assess_type):
            raise ValueError('Missing args')
        self.ref = args.ref
        self.assess_type = args.assess_type
        self.target = self.valid_target(args.target)
        self.out = args.out
        #gets the job ID from pachyderm
        try:
            self.job_id = os.environ['PACH_JOB_ID']
        except KeyError:
            raise KeyError('No valid job ID')

    def __str__(self):
        return f"Pipeline({self.job_id}) - (target={self.target},ref={self.ref},assess_type={self.assess_type})"

    @staticmethod
    def assess_type(assess_type_string: str)-> bool:
        #TODO: add other types later
        assess_type_list = ['semsim']#, 'subwords', 'comp']
        if assess_type_string in assess_type_list:
            return assess_type_string
        else:
            raise ValueError(f"Valid assessment types are {','.join(assess_type_list)}")

    def valid_target(self, target):
        #!!! target might be None if the assess_type is not semsim
        if self.assess_type == 'semsim' and target==None:
            raise ValueError('Semsim assessment type requires a valid target revision')
        else:
            return target

    def get_args(self):
        #initializes a command line argument parser
        parser = argparse.ArgumentParser\
            (description='Starts a pipeline depending on assessment type')
        parser.add_argument('-t','--target', type=int, help='Target Revision ID', required=False)
        parser.add_argument('-r','--ref', type=int, help='Reference Revision ID', required=True)
        parser.add_argument('-at','--assess_type', type=self.assess_type, help='Assessment type', required=True)
        parser.add_argument('-out', '--out', type=str, help='Output location', default='/pfs/out')
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def start(self):
        if self.assess_type == 'semsim':
            try:
                #TODO: catch sqlalchemy error if there is a violation
                #InitiateAssessment(id=self.job_id, revision=self.target,
                #              reference=self.ref, type=self.assess_type).push_assessment()
                return_json = {"job_id": self.job_id,
                               "revision": self.target,
                               "reference": self.ref,
                               "type": self.assess_type,
                               "out": self.out
                              }
                logging.info(return_json)
                json.dump(return_json, open(self.out + '/job_params.json','w'))
                return 200, return_json
            except IntegrityError as err:
                logging.error(err.args[0])
                return 500, err.args[0]
        else:
            #should not get here
            raise ValueError(f"{self.assess_type} is not a valid assessment type")

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    try:
        pipe = StartPipeline()
        result  = pipe.start()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)