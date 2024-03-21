# dependencies.py

from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("AQUA_DB")
DATABASE_URL_SYNC = os.getenv("AQUA_DB_SYNC")

engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
sync_engine = create_engine(DATABASE_URL_SYNC )
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine, class_=Session)

def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_async_db():
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()