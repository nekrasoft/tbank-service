from __future__ import annotations

import re

WAYBILL_TOKEN_RE = re.compile(
    r"(?:^|\s)\[?ПЛ:(?P<token>[A-Za-z0-9_-]{8,64})\]?",
    re.IGNORECASE,
)


def extract_waybill_token(note: str | None) -> tuple[str, str | None]:
    raw = str(note or "").strip()
    if not raw:
        return "", None

    match = WAYBILL_TOKEN_RE.search(raw)
    if not match:
        return raw, None

    clean_note = (raw[: match.start()] + raw[match.end() :]).strip()
    clean_note = re.sub(r"\s{2,}", " ", clean_note)
    clean_note = clean_note.strip(" ;,")
    return clean_note, match.group("token")
