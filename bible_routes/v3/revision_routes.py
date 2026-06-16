import logging
import time
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bible_loading import async_text_dataframe, text_loading
from database.dependencies import get_db
from database.models import BibleRevision as BibleRevisionModel
from database.models import BibleVersion as BibleVersionModel
from database.models import UserDB as UserModel
from models import RevisionIn
from models import RevisionOut_v3 as RevisionOut
from security_routes.auth_routes import get_current_user
from security_routes.utilities import (
    get_authorized_revision_ids,
    is_user_authorized_for_bible_version,
)

router = APIRouter()

# Revision uploads are vref-aligned plain-text files. A full Bible plaintext
# (e.g. the bundled KJV fixture) is ~5MB, so 50MB gives ample headroom for
# verbose translations / encodings while still capping memory use per upload.
MAX_UPLOAD_BYTES = 50 * 1024 * 1024
# Read in 1MB chunks when streaming to enforce the cap without loading the
# whole file at once.
UPLOAD_READ_CHUNK_BYTES = 1024 * 1024
# Most HTTP clients send plain text as "text/plain"; curl / generic clients
# fall back to "application/octet-stream". Anything else (images, archives,
# HTML, etc.) is rejected with 415.
ALLOWED_CONTENT_TYPES = {"text/plain", "application/octet-stream"}


async def read_upload_with_limit(file: UploadFile, max_bytes: int) -> bytes:
    """Read ``file`` fully into memory, aborting if it exceeds ``max_bytes``.

    Streams in chunks so an oversized upload is rejected before the whole
    body is buffered. Raises ``HTTPException(413)`` if the cap is exceeded.
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(UPLOAD_READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"upload too large (limit {max_bytes} bytes)",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def create_revision_out(
    revision: BibleRevisionModel, version_map: dict[int, BibleVersionModel]
) -> RevisionOut:
    version = version_map.get(revision.bible_version_id)
    revision_out_data = revision.__dict__.copy()
    revision_out_data.pop("_sa_instance_state", None)

    revision_out_data["version_abbreviation"] = (
        version.abbreviation if version else None
    )
    revision_out_data["iso_language"] = version.iso_language if version else None

    return RevisionOut(**revision_out_data)


@router.get("/revision", response_model=List[RevisionOut])
async def list_revisions(
    version_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns a list of revisions.

    If version_id is provided, returns a list of revisions for that version, otherwise returns a list of all revisions.

    Input:
    - version_id: Optional[int] = None
    Description: The id of the version to which the revision belongs. If not provided, returns all revisions.

    Returns:
    Fields(Revision):
    - version_id: int
    Description: The id of the version to which the revision belongs.
    - name: str
    Description: The name of the revision.
    - published: bool
    Description: Whether the revision is published.
    - backTranslation: Optional[int] = None
    Description: The id of the back translation revision.
    - machineTranslation: Optional[int] = None
    Description: The id of the machine translation revision.
    - file: UploadFile
    Description: The file containing the revision text.
    """
    if version_id:
        # Step 1: Check version existence
        version = await db.scalar(
            select(BibleVersionModel).where(BibleVersionModel.id == version_id)
        )
        if not version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Version id is invalid",
            )

        # Step 2: Check authorization
        if not await is_user_authorized_for_bible_version(
            current_user.id, version_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to access this bible version.",
            )

        # Step 3: Load revisions for that version
        result = await db.execute(
            select(BibleRevisionModel).where(
                BibleRevisionModel.bible_version_id == version_id,
                BibleRevisionModel.deleted.is_(False),
            )
        )
        revisions = result.scalars().all()

    else:
        # Step 1: Load all undeleted revisions
        result = await db.execute(
            select(BibleRevisionModel).where(BibleRevisionModel.deleted.is_(False))
        )
        all_revisions = result.scalars().all()

        # Step 2: Get only those revisions the user is authorized for
        authorized_ids = await get_authorized_revision_ids(current_user.id, db)
        revisions = [r for r in all_revisions if r.id in authorized_ids]

    # Step 4: Convert to response models
    # Preload all versions for the revisions
    version_ids = {r.bible_version_id for r in revisions}
    version_map = {}

    if version_ids:
        result = await db.execute(
            select(BibleVersionModel).where(BibleVersionModel.id.in_(version_ids))
        )
        version_map = {v.id: v for v in result.scalars().all()}

    revision_out_list = [create_revision_out(rev, version_map) for rev in revisions]

    return revision_out_list


async def process_and_upload_revision(
    file_content: bytes, revision_id: int, db: AsyncSession
):
    text_content = file_content.decode("utf-8")

    has_text = False
    verses = []
    for line in text_content.splitlines():
        stripped = line.strip()
        if stripped:
            verses.append(line.replace("\n", ""))
            has_text = True
        else:
            verses.append(None)

    if not has_text:
        raise ValueError("File has no text.")

    verse_records = await async_text_dataframe(verses, revision_id)
    await text_loading(verse_records, db)


@router.post("/revision", response_model=RevisionOut)
async def upload_revision(
    revision: RevisionIn = Depends(),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Uploads a new revision.

    Input:
    Fields(Revision):
    - version_id: int
    Description: The id of the version to which the revision belongs.
    - name: str
    Description: The name of the revision.
    - published: bool
    Description: Whether the revision is published.
    - backTranslation: Optional[int] = None
    Description: The id of the back translation revision.
    - machineTranslation: Optional[int] = None
    Description: The id of the machine translation revision.
    - file: UploadFile
    Description: The file containing the revision text.
    """
    start_time = time.time()

    # Validate the upload up-front, before we touch the DB. Reject obviously
    # wrong content-types (415) and reject anything that already advertises a
    # size over the cap (413). This guards against the Starlette multipart
    # DoS where an authenticated user uploads a huge body to exhaust workers.
    # Strip any media-type parameters (e.g. "text/plain; charset=utf-8") so
    # well-formed clients that include a charset aren't falsely rejected.
    raw_content_type = file.content_type or ""
    media_type = raw_content_type.split(";", 1)[0].strip().lower()
    if media_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"unsupported content-type: {raw_content_type}",
        )
    if file.size is not None and file.size > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"upload too large (limit {MAX_UPLOAD_BYTES} bytes)",
        )

    logging.info(f"Uploading new revision: {revision.model_dump()}")
    result = await db.execute(
        select(BibleVersionModel).where(BibleVersionModel.id == revision.version_id)
    )
    version = result.scalars().first()

    if not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid"
        )
    # Check if user is authorized to upload revision for this version
    if not await is_user_authorized_for_bible_version(
        current_user.id, revision.version_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to upload revision for this version.",
        )
    # check if the versions is not del
    if version.deleted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Version is deleted"
        )

    new_revision = BibleRevisionModel(
        bible_version_id=revision.version_id,
        name=revision.name,
        date=date.today(),
        published=revision.published,
        back_translation_id=revision.backTranslation,
        machine_translation=revision.machineTranslation,
    )
    db.add(new_revision)
    # Flush (to assign new_revision.id for the verse FK) but don't commit
    # yet — keep the revision row and its verses in one transaction so
    # rollback on a parse error leaves no orphaned revision behind, and the
    # whole upload pays one WAL fsync at the end instead of one per batch.
    await db.flush()

    try:
        # Stream the upload with a byte-count cap; this enforces the limit
        # even when the client didn't send a Content-Length / file.size is
        # unset, so a chunked oversize body still fails fast.
        contents = await read_upload_with_limit(file, MAX_UPLOAD_BYTES)
        await process_and_upload_revision(contents, new_revision.id, db)
        # One commit covers the BibleRevision row + all VerseText inserts.
        await db.commit()
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    version = await db.scalar(
        select(BibleVersionModel).where(
            BibleVersionModel.id == new_revision.bible_version_id
        )
    )
    version_map = {new_revision.bible_version_id: version} if version else {}
    revision_out = create_revision_out(new_revision, version_map)

    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"Uploaded revision successfully in {processing_time:.2f} seconds.")

    return revision_out


@router.delete("/revision")
async def delete_revision(
    id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Deletes a revision.

    Input:
    - id: int
    Description: The id of the revision to delete.
    """
    start_time = time.time()  # Start timer

    # Check if the revision exists and if the user is authorized
    result = await db.execute(
        select(BibleRevisionModel, BibleVersionModel)
        .join(
            BibleVersionModel,
            BibleRevisionModel.bible_version_id == BibleVersionModel.id,
        )
        .where(BibleRevisionModel.id == id)
    )
    revision, bible_version = result.first()  # Here, we destructure the result

    # Check if the revision exists
    if not revision:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Revision id is invalid or does not exist.",
        )

    # Check if the user is authorized to perform action on the revision
    # Here, we use the fetched bible_version's owner_id directly without accessing through revision
    if not current_user.is_admin and (
        bible_version is None or bible_version.owner_id != current_user.id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to perform this action on the revision.",
        )

    # delete the revision by updating the boolean field deleted to True and the deletedAt field to the current time
    revision.deleted = True
    revision.deletedAt = date.today()
    await db.commit()
    end_time = time.time()  # End timer
    processing_time = end_time - start_time
    logging.info(
        f"Deleted revision {id} successfully in {processing_time:.2f} seconds."
    )

    return {"detail": f"Revision {id} deleted successfully."}


# rename a revision endpoint
@router.put("/revision")
async def rename_revision(
    id: int,
    new_name: str,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Rename a revision.

    Input:
    - id: int
    Description: The id of the revision to rename.
    - new_name: str
    Description: The new name for the revision.
    """
    # Check if the revision exists
    result = await db.execute(
        select(BibleRevisionModel, BibleVersionModel)
        .join(
            BibleVersionModel,
            BibleRevisionModel.bible_version_id == BibleVersionModel.id,
        )
        .where(BibleRevisionModel.id == id)
    )
    revision, bible_version = result.first()  # Here, we destructure the result

    if not revision:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Revision not found."
        )

    # Check if the user is authorized to rename the revision
    # Here, we use the fetched bible_version's owner_id directly without accessing through revision
    if not current_user.is_admin and (
        bible_version is None or bible_version.owner_id != current_user.id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to rename this revision.",
        )
    revision.name = new_name
    await db.commit()

    return {"detail": f"Revision {id} successfully renamed."}
