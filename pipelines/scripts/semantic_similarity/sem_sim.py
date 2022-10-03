import argparse
import logging
logging.getLogger().setLevel('DEBUG')

class SemanticSimilarity:

    def __init__(self,):
        #gets args in the format --target /path/to/my/target/file
        # --reference /path/to/my/reference/file --out /path/to/output
        args = self.get_args()
        if not (args.target and args.reference and args.out):
            raise ValueError('Missing Revision Id or output path')
        #initializes the instance variables
        self.target = args.target
        self.reference = args.reference
        self.out = args.out

    @staticmethod
    def get_args():
        #initializes a command line argument parser
        parser = argparse.ArgumentParser(description='Checks semantic similarity between revision and reference')
        parser.add_argument('-t','--target', type=str, help='Target path', required=True)
        parser.add_argument('-r','--reference', type=str, help='Reference path', required=True)
        parser.add_argument('-o','--out', type=str, help='Output path', required=True)
        #gets the arguments - will fail if they are of the wrong type
        try:
            return parser.parse_args()
        except SystemExit as sys_exit:
            if sys_exit.code == 2:
                raise ValueError('Argument error') from sys_exit
            else:
                raise ValueError(sys_exit.code) from sys_exit

if __name__ == '__main__':
    try:
        ss = SemanticSimilarity()
    except (ValueError, OSError,
            KeyError, AttributeError,
            FileNotFoundError) as err:
        logging.error(err)
