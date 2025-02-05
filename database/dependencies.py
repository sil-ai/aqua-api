# dependencies.py

import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
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
