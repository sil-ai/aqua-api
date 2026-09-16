# auth_routes.py
import socket
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.concurrency import run_in_threadpool

from database.dependencies import get_db  # Function to get the database session
from database.models import Group as GroupDB
from database.models import UserDB, UserGroup
from models import Group, Token, User
from utils.logging_config import setup_logger

from .rate_limiting import check_token_failure_gate, register_failed_login
from .utilities import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    ALGORITHM,
    NO_SUCH_USER_PASSWORD_HASH,
    SECRET_KEY,
    verify_password,
)

router = APIRouter()
container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="latest/token")


async def authenticate_user(username: str, password: str, db: AsyncSession):
    """Return the user for these credentials, or False.

    Two things here are deliberate and load-bearing, both about what an
    unauthenticated caller can learn or cost us.

    A missing row is verified against ``NO_SUCH_USER_PASSWORD_HASH`` rather than
    skipped. The obvious ``not user or not verify_password(...)`` short-circuits, so
    an unknown username answers in ~18ms against ~175ms for a known one — a tenfold
    separation with no overlap, which classifies any username with certainty on one
    request and defeats the single shared 401 both surfaces go out of their way to
    return.

    And the verification runs in a worker thread. bcrypt at cost 12 takes ~154ms of
    uninterruptible CPU; called inline from this coroutine it would hold the event
    loop for that long, so a burst of failed logins stalls every endpoint the worker
    serves, not just this one.
    """
    # A NUL byte makes asyncpg raise CharacterNotInRepertoireError, which would
    # escape as a 500 before either budget is charged — an unbounded, unauthenticated
    # source of DB round trips and ~8KB tracebacks (cf. #954). No username can
    # contain one, so treat it as a miss and let it fall through to the ordinary,
    # charged 401 with the dummy-hash timing intact.
    user = None
    if "\x00" not in username:
        result = await db.execute(select(UserDB).where(UserDB.username == username))
        user = result.scalars().first()
    hashed = user.hashed_password if user else NO_SUCH_USER_PASSWORD_HASH
    matched = await run_in_threadpool(verify_password, password, hashed)
    if not user or not matched:
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    db: AsyncSession = Depends(get_db), token: str = Depends(oauth2_scheme)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        result = await db.execute(
            select(UserDB)
            .options(selectinload(UserDB.groups))
            .where(UserDB.username == username)
        )
        user = result.scalars().first()
        if user is None:
            raise credentials_exception
        logger.debug(
            f"Authenticated user={user.username} id={user.id} is_admin={user.is_admin}",
            extra={
                "username": user.username,
                "user_id": user.id,
                "is_admin": user.is_admin,
            },
        )
        return user
    except jwt.PyJWTError:
        raise credentials_exception


@router.post("/token", response_model=Token)
async def login_for_access_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    check_token_failure_gate(request)
    user = await authenticate_user(form_data.username, form_data.password, db)
    if not user:
        register_failed_login(request)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username, "is_admin": user.is_admin},
        expires_delta=access_token_expires,
    )
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user


@router.get("/groups/me", response_model=List[Group])
async def get_groups(
    current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    stmt = select(GroupDB).join(UserGroup).where(UserGroup.user_id == current_user.id)
    result = await db.execute(stmt)
    groups = result.scalars().all()
    return groups
