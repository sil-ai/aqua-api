import os
import logging
from models import Assessment#, StatusEnum
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

logging.getLogger().setLevel('DEBUG')

class InitiateAssessment:

    def __init__(self, revision, reference, type, job_id):
        engine = create_engine(os.environ['AQUA_CONNECTION_STRING'])
        self.session = Session(engine)
        self.revision = revision
        self.reference = reference
        self.type = type
        #???: Should there be a starting status?
        #status is 'RUNNING'
        self.job_id = job_id

    def __del__(self):
        self.session.close()

    def push_assessment(self):
        assess_object = Assessment(revision=self.revision,
                                   reference=self.reference,
                                   type=self.type,
                                   job_id=self.job_id)
        self.session.add(assess_object)
        try:
            self.session.commit()
            #returns the newly committed assessment.id
            return assess_object.id
        except IntegrityError as err:
            self.session.rollback()
            raise ValueError(err)

class GetAssessment:

    def __init__(self):
        engine = create_engine(os.environ['AQUA_CONNECTION_STRING'])
        self.session = Session(engine)

    def __del__(self):
        self.session.close()

    def get_assessment(self, assess_id):
        return self.session.query(Assessment).get(assess_id)

    def get_all_assessments(self):
        stmt = select(Assessment)
        return self.session.scalars(stmt).all()
