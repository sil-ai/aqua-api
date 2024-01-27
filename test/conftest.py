# conftest.py

import os
import pytest
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.models import Base, UserDB, Group, UserGroup, IsoLanguage, IsoScript, BibleVersion, BibleRevision, BookReference, ChapterReference, VerseReference
import bcrypt
from datetime import date
import pandas as pd
# Assuming you have an environment variable for your test database

engine = db.create_engine(os.getenv("AQUA_DB"))
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="module")
def test_db_session():
    Base.metadata.create_all(bind=engine)
    db_session = TestingSessionLocal()

    # Add your test data setup here
    setup_database(db_session)
    try:    
        yield db_session

    # Teardown test data
    finally:
        teardown_database(db_session)
        db_session.close()

def setup_database(db_session):
    """Set up the database for testing with distinct sections for different data types."""

    # Section 1: Setting up Users and Groups
    setup_users_and_groups(db_session)

    # Section 2: Setting up References and ISOs
    setup_references_and_isos(db_session)

    # Section 3: Loading Revision
    load_revision_data(db_session)

def setup_users_and_groups(db_session):
    """Setup test users and groups."""
    # Create users
    test_user1 = UserDB(
        username="testuser1",
        email="testuser1@example.com",
        hashed_password=bcrypt.hashpw("password1".encode(), bcrypt.gensalt()).decode(),
        is_admin=False
    )
    test_user2 = UserDB(
        username="testuser2",
        email="testuser2@example.com",
        hashed_password=bcrypt.hashpw("password2".encode(), bcrypt.gensalt()).decode(),
        is_admin=False
    )
    admin_user = UserDB(
        username="admin",
        email="admin@example.com",
        hashed_password=bcrypt.hashpw("adminpassword".encode(), bcrypt.gensalt()).decode(),
        is_admin=True
    )

    db_session.add_all([test_user1, test_user2, admin_user])
    db_session.commit()

    # Create groups
    group1 = Group(name="Group1", description="Test Group 1")
    group2 = Group(name="Group2", description="Test Group 2")

    db_session.add_all([group1, group2])
    db_session.commit()

    # Associate users with groups
    user_group1 = UserGroup(user_id=test_user1.id, group_id=group1.id)
    user_group2 = UserGroup(user_id=test_user2.id, group_id=group2.id)

    db_session.add_all([user_group1, user_group2])
    db_session.commit()

def setup_references_and_isos(db_session):
    """Setup reference data and ISO codes."""
    # Load data from CSV files
    book_ref_df = pd.read_csv('fixtures/book_reference.txt', sep='\t')
    chapter_ref_df = pd.read_csv('fixtures/chapter_reference.txt', sep='\t')
    verse_ref_df = pd.read_csv('fixtures/verse_reference.txt', sep='\t')

    # Populate book_reference, chapter_reference, verse_reference tables
    for _, row in book_ref_df.iterrows():
        db_session.add(BookReference(**row.to_dict()))
    for _, row in chapter_ref_df.iterrows():
        db_session.add(ChapterReference(**row.to_dict()))
    for _, row in verse_ref_df.iterrows():
        db_session.add(VerseReference(**row.to_dict()))

    # Add ISO language and script
    db_session.add(IsoLanguage(iso639="eng", name="english"))
    db_session.add(IsoScript(iso15924="Latn", name="latin"))
    db_session.commit()

def load_revision_data(db_session):
    """Load revision data into the database."""
    # Add version
    version = BibleVersion(name="loading_test", iso_language="eng", iso_script="Latn", abbreviation="BLTEST")
    db_session.add(version)

    # Commit to save the version and retrieve its ID for the revision
    db_session.commit()

    # Add revision
    revision = BibleRevision(date=date.today(), bible_version_id=version.id, published=False, machine_translation=True)
    db_session.add(revision)
    db_session.commit()
   
    
def teardown_database(db_session):
    engine = db_session.get_bind()
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute("SET session_replication_role = replica;")
            for table_name in reversed(Base.metadata.sorted_tables):
                connection.execute(table_name.delete())
            connection.execute("SET session_replication_role = DEFAULT;")
            transaction.commit()

