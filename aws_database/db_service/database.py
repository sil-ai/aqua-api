from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve the DATABASE_URL from the .env file
DATABASE_URL = os.getenv('DATABASE_URL')

# If the DATABASE_URL is not set, we should raise an exception
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL set for Flask application. Did you forget to run 'source .env'?")

engine = create_engine(DATABASE_URL)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()

def init_db():
    # Import all modules here that might define models so that
    # they will be registered properly on the metadata.
    # Otherwise, you will have to import them first before calling init_db()
    from .models import ExampleModel  # Replace ExampleModel with your actual models
    Base.metadata.create_all(bind=engine)
