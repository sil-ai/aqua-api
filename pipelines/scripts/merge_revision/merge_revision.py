import re
import argparse
import logging
import numpy as np
import pandas as pd
import logging
logging.getLogger().setLevel('DEBUG')

class AlignRevision:

    def __init__(self,):
        #gets the args of the form --target /path/to/my/target/file
        # --reference /path/to/my/reference/file --out /path/to/output/file
        args = self.get_args()
        if not (args.target and args.reference and args.out):
            raise ValueError('Missing filepath')
        #initializes the instance variables
        self.target = self.get_file(args.target)
        self.reference = self.get_file(args.reference)
        self.vref = self.get_file('../../../fixtures/vref.txt')
        self.out_filepath = args.out

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
    
    def align_revision(self):
        if not self.check_matching_length():
            raise ValueError(f"Target and reference differ by {abs(len(self.reference)- len(self.target))}")
        elif not self.check_vref():
            raise ValueError('Target and/or reference don\'t match vref')
        else:
            pass

    def output_aligned_revisions(self, aligned_revisions):
        pass

if __name__ == '__main__':
    try:
        ar = AlignRevision()
        aligned_revisions = ar.align_revision()
        ar.output_aligned_revisions(aligned_revisions)
    except (ValueError, OSError,
            KeyError, AttributeError,
            TypeError,
            FileNotFoundError,
            IsADirectoryError) as err:
        logging.error(err)
