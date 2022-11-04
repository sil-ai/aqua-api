from sqlalchemy import Column, Integer, Text, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Assessment(Base):

    __tablename__ = "assessment"
    id = Column(Integer, primary_key=True)
    revision = Column(Integer)
    reference = Column(Integer)
    finished = Column(Boolean, default=False)
    type = Column(Text)

    def __repr__(self):
        return (
            f"Assessment({self.id}) - {self.type} "
            f"revision={self.revision} reference={self.reference}, finished={self.finished}"
        )
