from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from config import settings
from database.models import Base

# AQUA_DB is a required setting, validated when `config` is imported, so it is
# guaranteed to be present (non-empty) here.
DATABASE_URL = settings.aqua_db

engine = create_engine(DATABASE_URL)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


Base.query = db_session.query_property()


def init_db():
    # Import all modules here that might define models so that
    # they will be registered properly on the metadata.
    Base.metadata.create_all(bind=engine)
