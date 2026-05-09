from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import WorkFile


def get_by_token(session: Session, file_token: str) -> WorkFile | None:
    token = str(file_token or "").strip()
    if not token:
        return None
    result = session.execute(
        select(WorkFile).where(WorkFile.file_token == token).limit(1)
    )
    return result.scalars().first()


def link_to_work_by_token(
    session: Session,
    *,
    file_token: str,
    work_id: int,
) -> WorkFile | None:
    work_file = get_by_token(session, file_token)
    if work_file is None:
        return None

    if work_file.work_id not in (None, work_id):
        return work_file

    changed = False
    if work_file.work_id != work_id:
        work_file.work_id = work_id
        changed = True
    if work_file.linked_at is None:
        work_file.linked_at = datetime.utcnow()
        changed = True
    if changed:
        session.flush()
    return work_file
