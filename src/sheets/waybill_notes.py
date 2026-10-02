from __future__ import annotations

import re

WAYBILL_TOKEN_RE = re.compile(
    r"(?:^|\s)\[?ПЛ:(?P<token>[A-Za-z0-9_-]{8,64})\]?",
    re.IGNORECASE,
)


def extract_waybill_tokens(note: str | None) -> tuple[str, list[str]]:
    raw = str(note or "").strip()
    if not raw:
        return "", []

    tokens = [match.group("token") for match in WAYBILL_TOKEN_RE.finditer(raw)]
    if not tokens:
        return raw, []

    clean_note = WAYBILL_TOKEN_RE.sub("", raw).strip()
    clean_note = re.sub(r"\s{2,}", " ", clean_note)
    clean_note = clean_note.strip(" ;,")
    return clean_note, tokens


def extract_waybill_token(note: str | None) -> tuple[str, str | None]:
    clean_note, tokens = extract_waybill_tokens(note)
    return clean_note, tokens[0] if tokens else None
