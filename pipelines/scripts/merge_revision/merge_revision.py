import argparse
import logging
logging.getLogger().setLevel('DEBUG')
import pandas as pd

class MergeRevision:

    def __init__(self,):
        #gets the args of the form --target /path/to/my/target/file
        # --reference /path/to/my/reference/file --out /path/to/output/file
        args = self.get_args()
        if not (args.target and args.reference and args.out):
            raise ValueError('Missing filepath')
        #initializes the instance variables
        self.args = args
        self.target = self.get_file(self.args.target)
        self.reference = self.get_file(self.args.reference)
        self.vref = self.get_file('vref.txt')

    @staticmethod
    def get_file(filepath):
        return open(filepath).read().splitlines()

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Align target and reference side by side')
        parser.add_argument('-t','--target', type=str, help='Target path', required=True)
        parser.add_argument('-r','--reference', type=str, help='Reference path', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path',required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

    def check_matching_length(self):
        return len(self.target) == len(self.reference)

    def check_vref(self):
        return (len(self.target) == len(self.vref)) and (len(self.reference) == len(self.vref))
    
    def merge_revision(self):
        #check that target and reference are the same length
        if not self.check_matching_length():
            raise ValueError(f"Target and reference differ by {abs(len(self.reference)- len(self.target))}")
        #check that both target and reference are the same length as vref    
        elif not self.check_vref():
            raise ValueError('Target and/or reference length don\'t match vref')
        else:
            #merge the two revisions together
            merged_revisions = pd.DataFrame({'target':self.target, 'reference': self.reference})
            logging.info(f'Revision {self.get_revision_id(self.args.target)} and {self.get_revision_id(self.args.reference)} are merged')
            return merged_revisions

    @staticmethod
    def get_revision_id(filename):
        return filename.split('.')[0].split('_')[0]

    def output_merged_revisions(self, merged_revisions):
        target_revision = self.get_revision_id(self.args.target)
        reference_revision = self.get_revision_id(self.args.reference)
        filename = f'{target_revision}_{reference_revision}_merge.csv'
        filepath = self.args.out + '/' + filename
        merged_revisions.to_csv(filepath, index=False)
        logging.info(f'{filename} created in {self.args.out}')

if __name__ == '__main__':
    try:
        ar = MergeRevision()
        merged_revisions = ar.merge_revision()
        ar.output_merged_revisions(merged_revisions)
    except (ValueError, OSError,
            KeyError, AttributeError,
            TypeError,
            FileNotFoundError,
            IsADirectoryError) as err:
        logging.error(err)
