import pandas as pd
import logging
logging.getLogger().setLevel('INFO')

class MergeRevision:

    def __init__(self,revision_id, revision_verses, reference_id, reference_verses):
        self.revision_id = revision_id
        self.revision = revision_verses
        self.reference_id = reference_id
        self.reference = reference_verses
        self.vref = open('/root/vref.txt').read().splitlines()

    def check_matching_length(self):
        return len(self.revision) == len(self.reference)

    def check_vref(self):
        return (len(self.revision) == len(self.vref)) and (len(self.reference) == len(self.vref))
    
    def merge_revision(self):
        #check that draft and reference are the same length
        if not self.check_matching_length():
            raise ValueError(f"draft and reference differ by {abs(len(self.reference)- len(self.revision))}")
        #check that both draft and reference are the same length as vref    
        elif not self.check_vref():
            raise ValueError('draft and/or reference length don\'t match vref')
        else:
            #merge the two revisions together
            merged_revisions = pd.DataFrame({'revision':self.revision, 'reference': self.reference},index=self.vref)
            merged_revisions.index.name = 'vref'
            #remove verses/rows in merged_revisions that are not in draft and reference
            #??? Should I handle non-alpha verse entries?
            merged_revisions1 = merged_revisions[(merged_revisions['revision']!='') &\
                                                 (merged_revisions['reference']!='')]
            logging.info(f'Revision {self.revision_id} and {self.reference_id} are merged')
            return merged_revisions1
