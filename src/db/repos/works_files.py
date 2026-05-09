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


def get_by_work_ids(session: Session, work_ids: list[int]) -> list[WorkFile]:
    """Получение файлов, привязанных к списку работ."""
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for raw_id in work_ids:
        try:
            work_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if work_id <= 0 or work_id in seen:
            continue
        seen.add(work_id)
        normalized_ids.append(work_id)

    if not normalized_ids:
        return []

    result = session.execute(
        select(WorkFile)
        .where(WorkFile.work_id.in_(normalized_ids))
        .order_by(WorkFile.work_id, WorkFile.id)
    )
    return list(result.scalars().all())


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
