import argparse
import logging
import itertools
from assessment_operations import InitiateAssessment, GetAssessment
from sqlalchemy.exc import IntegrityError

logging.getLogger().setLevel('DEBUG')

class ApiSwitchboard:
    #job_id counter
    try:
        job_id = int(open('counter.txt','r').read())+1
    except FileNotFoundError:
        all_assessments = GetAssessment().get_all_assessments()
        job_id = all_assessments[-1].id + 1
    #??? Are job and assessment ids the same thing?
    #job_id = current_id#itertools.count(current_id)

    def __init__(self):
        args = self.get_args()
        if not (args.ref and args.assess_type):
            raise ValueError('Missing args')
        self.ref = args.ref
        self.assess_type = args.assess_type
        self.target = self.valid_target(args.target)
        #self.job_id = self.current_id #next(ApiSwitchboard.job_id)

    def __str__(self):
        return f"ApiSwitchboard({self.job_id}) - (target={self.target},ref={self.ref},assess_type={self.assess_type}) "
    @staticmethod
    def assess_type(assess_type_string: str)-> bool:
        assess_type_list = ['semsim', 'subwords', 'comp']
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
            (description='Switches between assessment pipelines depending on assessment type')
        parser.add_argument('-t','--target', type=int, help='Target Revision ID', required=False)
        parser.add_argument('-r','--ref', type=int, help='Reference Revision ID', required=True)
        parser.add_argument('-at','--assess_type', type=self.assess_type, help='Assessment type', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def switch(self):
        if self.assess_type == 'semsim':
            try:
                #TODO: catch sqlalchemy error if there is a violation
                InitiateAssessment(id=self.job_id, revision=self.target,
                              reference=self.ref, type=self.assess_type).push_assessment()
                return_string = f"Assessment {self.job_id}(Semantic Similarity) has begun"
                logging.info(return_string)
                #successfully added assessment so update counter file
                with open('counter.txt','w') as counter_file:
                    counter_file.write(str(self.job_id))
                return 200, return_string
            except IntegrityError as err:
                logging.error(err.args[0])
                return 500, err.args[0]
        elif self.assess_type == 'subwords':
            #TODO: subwords stage output here
            logging.info(f"Assessment {self.job_id}(Subwords) has begun")
        elif self.assess_type == 'comp':
            #TODO: comprehension stage output here
            logging.info(f"Assessment {self.job_id}(Comprehension) has begun")
        else:
            #should not get here
            raise ValueError(f"{self.assess_type} is not a valid assessment type")

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    try:
        switchboard = ApiSwitchboard()
        result  = switchboard.switch()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
