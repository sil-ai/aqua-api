import pandas as pd
import logging
logging.getLogger().setLevel('INFO')

class MergeRevision:

    def __init__(self,draft_id, draft_verses, reference_id, reference_verses):
        #expecting revision in the format {'revision_id':[revision verses as list]}
        self.draft_id = draft_id
        self.draft = draft_verses
        self.reference_id = reference_id
        self.reference = reference_verses
        self.vref = open('/root/vref.txt').read().splitlines()

    def check_matching_length(self):
        return len(self.draft) == len(self.reference)

    def check_vref(self):
        return (len(self.draft) == len(self.vref)) and (len(self.reference) == len(self.vref))
    
    def merge_revision(self):
        #check that draft and reference are the same length
        if not self.check_matching_length():
            raise ValueError(f"draft and reference differ by {abs(len(self.reference)- len(self.draft))}")
        #check that both draft and reference are the same length as vref    
        elif not self.check_vref():
            raise ValueError('draft and/or reference length don\'t match vref')
        else:
            #merge the two revisions together
            merged_revisions = pd.DataFrame({'draft':self.draft, 'reference': self.reference},index=self.vref)
            merged_revisions.index.name = 'vref'
            #remove verses/rows in merged_revisions that are not in reference
            merged_revisions1 = merged_revisions[merged_revisions['draft']!='']
            logging.info(f'Revision {self.draft_id} and {self.reference_id} are merged')
            return merged_revisions1
