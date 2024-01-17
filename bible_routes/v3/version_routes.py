__version__ = 'v2'

import os
from typing import List
import re

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import psycopg2
from sqlalchemy.orm import Session

import queries
from key_fetch import get_secret
from models import VersionIn, VersionOut
from database.models import (
    Assessment as AssessmentModel, 
    BibleRevision as BibleRevisionModel, 
    UserDB as UserModel, 
    UserGroup,
    BibleVersion as BibleVersionModel  
    BibleVersionAccess,
)
from security_routes.utilities import (
    get_current_user, 
    api_key_auth, 
    is_user_authorized_for_bible_version
)
from database.dependencies import get_db, postgres_conn
router = fastapi.APIRouter()

@router.get("/version", response_model=List[VersionOut])
async def list_version(db: Session = Depends(get_db), current_user: UserModel = Depends(get_current_user)):
    """
    Get a list of all versions that the current user is authorized to access.
    """
    if current_user.is_admin:
        # Admin users can access all versions
        versions = db.query(BibleVersionModel).all()
    else:
        # Fetch the groups the user belongs to
        user_group_ids = db.query(UserGroup.group_id).filter(UserGroup.user_id == current_user.id).subquery()

        # Get versions that the user has access to through their groups
        versions = db.query(BibleVersionModel).join(
            BibleVersionAccess, BibleVersionModel.id == BibleVersionAccess.bible_version_id
        ).filter(
            BibleVersionAccess.group_id.in_(user_group_ids)
        ).all()

    return [VersionOut.from_orm(version) for version in versions]
]


@router.post("/version",response_model=VersionOut)
async def add_version(v: VersionIn = Depends(), db: Session = Depends(get_db)):
    """
    Create a new version.
    """
    new_version = VersionModel(
        name=v.name,
        iso_language=v.iso_language,
        iso_script=v.iso_script,
        abbreviation=v.abbreviation,
        rights=v.rights,
        forwardTranslation=v.forwardTranslation,
        backTranslation=v.backTranslation,
        machineTranslation=v.machineTranslation,
    )

    db.add(new_version)
    db.commit()
    db.refresh(new_version)

    return VersionOut.from_orm(new_version)



@router.delete("/version", dependencies=[Depends(api_key_auth)])
async def delete_version(id: int):
    """
    Delete a version and all associated revisions, text and assessments.
    """
    
    connection = postgres_con()
    cursor = connection.cursor()
    
    fetch_versions = queries.list_versions_query()
    fetch_revisions = queries.list_revisions_query()
    delete_version = queries.delete_bible_version()

    cursor.execute(fetch_versions)
    version_result = cursor.fetchall()

    version_list = []
    for version in version_result:
        version_list.append(version[0])

    cursor.execute(fetch_revisions, (id,))
    revision_result = cursor.fetchall()

    if id in version_list:
        cursor.execute(fetch_revisions, (id,))
        revision_result = cursor.fetchall()

        for revision in revision_result:
            delete_verses = queries.delete_verses_mutation()
            cursor.execute(delete_verses, (revision[0],))

            delete_revision = queries.delete_revision_mutation()
            cursor.execute(delete_revision, (revision[0],))

        cursor.execute(delete_version, (id,))
        version_delete_result = cursor.fetchone()
        connection.commit()

        delete_response = ("Version " +
            version_delete_result[0] +
            " successfully deleted."
        )

    else:
        cursor.close()
        connection.close()

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Version abbreviation invalid, version does not exist"
        )

    cursor.close()
    connection.close()

    return delete_response
