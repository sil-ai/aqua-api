"""User / group / auth-token schemas (issue #729)."""

from typing import Optional

from pydantic import BaseModel, EmailStr


class User(BaseModel):
    id: Optional[int] = None
    username: str
    email: Optional[EmailStr] = None  # Assuming users have an email field
    is_admin: Optional[bool] = False

    class Config:
        orm_mode = True


# group pydantic model
class Group(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None

    class Config:
        orm_mode = True


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


__all__ = [
    "User",
    "Group",
    "Token",
    "TokenData",
]
