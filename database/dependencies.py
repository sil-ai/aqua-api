# dependencies.py

from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import NullPool
# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("AQUA_DB")

engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_db():
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()
