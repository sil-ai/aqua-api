from fastapi.security import OAuth2PasswordBearer
from fastapi import FastAPI, Depends, HTTPException, status, APIRouter
from jose import jwt
from sqlalchemy.orm import Session
from utilities import hash_password
from models import User, Group
from database.models import (
    UserDB,
    UserGroup,
    Group as GroupDB,
)
from database.dependencies import get_db

from .utilities import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES 
router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_current_admin(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
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
        user = db.query(UserDB).filter(UserDB.email == username).first()
        if user is None or not user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="The user doesn't have enough privileges",
            )
        return user
    except jwt.JWTError:
        raise credentials_exception
    
    
    
@router.post("/users/", response_model=User)
async def create_user(user: User, db: Session = Depends(get_db)):
    db_user = db.query(UserDB).filter(UserDB.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    if user.password is None:
        raise HTTPException(status_code=400, detail="Password is required")
    hashed_password = hash_password(user.password)
    db_user = UserDB(username=user.username, email=user.email, hashed_password=hashed_password, is_admin=user.is_admin)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    # Convert SQLAlchemy model instance to Pydantic model instance for the response
    return_user = User.model_validate(db_user)
    return_user.password = None  # Ensure password is not included in the response
    return return_user

# create group endpoint
@router.post("/groups/", response_model=GroupDB)
async def create_group(group: Group, db: Session = Depends(get_db), current_user: UserDB = Depends(get_current_admin)):
    db_group = db.query(GroupDB).filter(GroupDB.name == group.name).first()
    if db_group:
        raise HTTPException(status_code=400, detail="Group already exists")
    db_group = GroupDB(name=group.name)
    db.add(db_group)
    db.commit()
    db.refresh(db_group)
    return_group = Group.model_validate(db_group)
    return return_group


@router.post("/link-user-group", status_code=status.HTTP_201_CREATED)
async def link_user_to_group(
    request_body: dict, 
    db: Session = Depends(get_db), 
    _: UserDB = Depends(get_current_admin)  # Ensuring only admin can link users to groups
):
    username = request_body.get("username")
    group_name = request_body.get("group_name")

    if not username or not group_name:
        raise HTTPException(status_code=400, detail="Username and group name are required")

    user = db.query(UserDB).filter(UserDB.username == username).first()
    group = db.query(GroupDB).filter(GroupDB.name == group_name).first()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Check if the link already exists
    existing_link = db.query(UserGroup).filter_by(user_id=user.id, group_id=group.id).first()
    if existing_link:
        raise HTTPException(status_code=400, detail="User is already linked to this group")
    
    new_link = UserGroup(user_id=user.id, group_id=group.id)
    db.add(new_link)
    db.commit()
    return {"message": f"User {username} successfully linked to group {group_name}"}

@router.delete("/groups", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(
    group_name: str, 
    db: Session = Depends(get_db), 
    _: UserDB = Depends(get_current_admin)  # Ensuring only admin can delete groups
):
    group = db.query(GroupDB).filter(GroupDB.name == group_name).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Check if the group is linked to any user
    linked_users = db.query(UserGroup).filter(UserGroup.group_id == group.id).first()
    if linked_users:
        raise HTTPException(status_code=400, detail="Group is linked to users and cannot be deleted")

    db.delete(group)
    db.commit()
    return {"message": f"Group '{group_name}' successfully deleted"}



@router.delete("/users", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    username: str, 
    db: Session = Depends(get_db), 
    _: UserDB = Depends(get_current_admin)  # Ensuring only admin can delete users
):
    user = db.query(UserDB).filter(UserDB.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Optional: Perform any cleanup or checks before deleting the user

    db.delete(user)
    db.commit()
    return {"message": f"User '{username}' successfully deleted"}