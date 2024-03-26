from fastapi.security import OAuth2PasswordBearer
from fastapi import FastAPI, Depends, HTTPException, status, APIRouter
from jose import jwt
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .utilities import hash_password
from models import User, Group
from database.models import (
    UserDB,
    UserGroup,
    Group as GroupDB,
)
from database.dependencies import get_db

from .utilities import SECRET_KEY, ALGORITHM

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def get_current_admin(
    token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload is None:
            raise credentials_exception
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        result = await db.execute(select(UserDB).filter_by(username=username))
        user = result.scalars().first()
        if user is None or not user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="The user doesn't have enough privileges",
            )
        return user
    except jwt.JWTError:
        raise credentials_exception


@router.post("/users", response_model=User)
async def create_user(
    user: User = Depends(),
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(get_current_admin),
):
    result = await db.execute(select(UserDB).where(UserDB.username == user.username))
    db_user = result.scalars().first()

    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    if user.password is None:
        raise HTTPException(status_code=400, detail="Password is required")
    hashed_password = hash_password(user.password)
    db_user = UserDB(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password,
        is_admin=user.is_admin,
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    # Convert SQLAlchemy model instance to Pydantic model instance for the response
    return_user = User(
        id=db_user.id,
        username=db_user.username,
        email=db_user.email,
        is_admin=db_user.is_admin,
    )
    return_user.password = None  # Ensure password is not included in the response
    return return_user


# create group endpoint
@router.post("/groups", response_model=Group)
async def create_group(
    group: Group = Depends(),
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(get_current_admin),
):
    result = await db.execute(select(GroupDB).where(GroupDB.name == group.name))
    db_group = result.scalars().first()
    if db_group:
        raise HTTPException(status_code=400, detail="Group already exists")
    db_group = GroupDB(name=group.name, description=group.description)
    db.add(db_group)
    await db.commit()
    await db.refresh(db_group)
    return_group = Group(
        id=db_group.id, name=db_group.name, description=db_group.description
    )
    return return_group


@router.get("/groups", response_model=list[Group])
async def get_groups(
    db: AsyncSession = Depends(get_db), _: UserDB = Depends(get_current_admin)
):
    result = await db.execute(select(GroupDB))
    groups = result.scalars().all()
    return [
        Group(id=group.id, name=group.name, description=group.description)
        for group in groups
    ]


@router.post("/link-user-group", status_code=status.HTTP_201_CREATED)
async def link_user_to_group(
    username=str,
    groupname=str,
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(
        get_current_admin
    ),  # Ensuring only admin can link users to groups
):
    if not username or not groupname:
        raise HTTPException(
            status_code=400, detail="Username and group name are required"
        )

    result = await db.execute(select(UserDB).where(UserDB.username == username))
    user = result.scalars().first()

    result = await db.execute(select(GroupDB).where(GroupDB.name == groupname))
    group = result.scalars().first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Check if the link already exists
    result = await db.execute(
        select(UserGroup).where(
            (UserGroup.user_id == user.id) & (UserGroup.group_id == group.id)
        )
    )
    existing_link = result.scalars().first()
    if existing_link:
        raise HTTPException(
            status_code=400, detail="User is already linked to this group"
        )

    new_link = UserGroup(user_id=user.id, group_id=group.id)
    db.add(new_link)
    await db.commit()
    return {"message": f"User {username} successfully linked to group {groupname}"}


@router.post("/unlink-user-group", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_user_from_group(
    username=str,
    groupname=str,
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(
        get_current_admin
    ),  # Ensuring only admin can unlink users from groups
):
    if not username or not groupname:
        raise HTTPException(
            status_code=400, detail="Username and group name are required"
        )

    result = await db.execute(select(UserDB).where(UserDB.username == username))
    user = result.scalars().first()

    result = await db.execute(select(GroupDB).where(GroupDB.name == groupname))
    group = result.scalars().first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    result = await db.execute(
        select(UserGroup).where(
            (UserGroup.user_id == user.id) & (UserGroup.group_id == group.id)
        )
    )
    link = result.scalars().first()
    if not link:
        raise HTTPException(status_code=404, detail="User is not linked to this group")

    await db.delete(link)
    await db.commit()
    return {"message": f"User {username} successfully unlinked from group {groupname}"}


@router.delete("/groups", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(
    groupname: str,
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(get_current_admin),  # Ensuring only admin can delete groups
):
    result = await db.execute(select(GroupDB).where(GroupDB.name == groupname))
    group = result.scalars().first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Check if the group is linked to any user
    result = await db.execute(select(UserGroup).where(UserGroup.group_id == group.id))
    linked_users = result.scalars().first()
    if linked_users:
        raise HTTPException(
            status_code=400, detail="Group is linked to users and cannot be deleted"
        )

    await db.delete(group)
    await db.commit()
    return {"message": f"Group '{groupname}' successfully deleted"}


@router.delete("/users", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(get_current_admin),  # Ensuring only admin can delete users
):
    result = await db.execute(select(UserDB).where(UserDB.username == username))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Optional: Perform any cleanup or checks before deleting the user

    await db.delete(user)
    await db.commit()
    return {"message": f"User '{username}' successfully deleted"}


# create a change password endpoint
@router.post("/change-password")
async def change_password(
    username: str,
    new_password: str,
    db: AsyncSession = Depends(get_db),
    _: UserDB = Depends(get_current_admin),
):
    result = await db.execute(select(UserDB).where(UserDB.username == username))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.hashed_password = hash_password(new_password)
    await db.commit()
    return {"message": f"Password for user '{username}' successfully changed"}
