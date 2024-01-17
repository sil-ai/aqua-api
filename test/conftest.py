# conftest.py

import os
import pytest
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.models import Base, UserDB, Group, UserGroup
import bcrypt

# Assuming you have an environment variable for your test database
TEST_DATABASE_URL = "your_test_database_url"

db_engine = db.create_engine(os.getenv("AQUA_DB"))
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

@pytest.fixture(scope="module")
def security_db_session():
    Base.metadata.create_all(bind=db_engine)
    db = TestingSessionLocal()

    # Add test data
    test_user = UserDB(
        username="testuser",
        email="testuser@example.com",
        hashed_password=bcrypt.hashpw("password".encode(), bcrypt.gensalt()).decode(),
        is_admin=False
    )
    admin_user = UserDB(
        username="admin",
        email="admin@example.com",
        hashed_password=bcrypt.hashpw("adminpassword".encode(), bcrypt.gensalt()).decode(),
        is_admin=True
    )

    db.add(test_user)
    db.add(admin_user)
    db.commit()

    # Setup additional database states as necessary

    yield db

    # Teardown
    teardown_database(db_engine)  # Your existing function to clean up the database
    db.close()
    
    
def teardown_database(engine):
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute("SET session_replication_role = replica;")
            for table_name in reversed(Base.metadata.sorted_tables):
                connection.execute(table_name.delete())
            connection.execute("SET session_replication_role = DEFAULT;")
            transaction.commit()
