import argparse
import logging
import itertools

logging.getLogger().setLevel('DEBUG')

class ApiSwitchboard:
#TODO: figure best way for job_id to persist
#job_id counter
    try:
        current_id = int(open('counter.txt','r').read())+1
    except FileNotFoundError:
        current_id=1
    job_id= itertools.count(current_id)

    def __init__(self):
        args = self.get_args()
        self.ref = args.ref
        self.assess_type = args.assess_type
        self.target = self.valid_target(args.target)
        self.job_id = next(ApiSwitchboard.job_id)
        with open('counter.txt','w') as counter_file:
            counter_file.write(str(self.job_id))

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
            print(self.job_id, 'semsim')
        elif self.assess_type == 'subwords':
            print(self.job_id, 'subwords')
        elif self.assess_type == 'comp':
            print(self.job_id, 'comp')
        else:
            #should not get here
            raise ValueError(f"{self.assess_type} is not a valid assessment type")

if __name__ == '__main__':
    try:
        switchboard = ApiSwitchboard()
        pipeline_path  = switchboard.switch()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
