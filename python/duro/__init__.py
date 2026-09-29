"""Small Python writer for Duro's canonical PostgreSQL event ledger."""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

import psycopg
from psycopg.types.json import Jsonb

__all__ = ["AppendError", "StoredEvent", "append_event"]

_WHITESPACE = "\u0009\u000a\u000b\u000c\u000d\u0020\u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"


@dataclass(frozen=True)
class StoredEvent:
    """One event exactly as PostgreSQL committed it."""

    id: str
    received_at: datetime
    event_type: str
    actor: str
    content: Mapping[str, Any]
    refs: Mapping[str, Any]


class AppendError(RuntimeError):
    """An append failure with a confirmed or unknown commit outcome."""

    def __init__(self, outcome: str, cause: BaseException):
        self.outcome = outcome
        self.cause = cause
        super().__init__(f"duro: append {outcome}: {cause}")


def append_event(
    event_type: str,
    *,
    content: Optional[Mapping[str, Any]] = None,
    refs: Optional[Mapping[str, Any]] = None,
    dsn: Optional[str] = None,
) -> StoredEvent:
    """Append one event and return its database-assigned receipt.

    ``dsn`` falls back to ``DURO_POSTGRES_DSN``. ``content`` and ``refs``
    must be JSON objects; omitted values store as ``{}``. PostgreSQL assigns
    ``id``, ``received_at``, and ``actor``. This function never retries.
    """

    _validate_event_type(event_type)
    content = _json_object("content", content)
    refs = _json_object("refs", refs)
    dsn = dsn or os.environ.get("DURO_POSTGRES_DSN")
    if not dsn:
        raise ValueError("duro: dsn is required (or set DURO_POSTGRES_DSN)")

    try:
        connection = psycopg.connect(dsn)
    except psycopg.Error as error:
        raise AppendError("not_committed", error) from error

    try:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.events (event_type, content, refs)
                    VALUES (%s, %s, %s)
                    RETURNING id, received_at, event_type, actor, content, refs
                    """,
                    (event_type, Jsonb(content), Jsonb(refs)),
                )
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise AppendError("not_committed", error) from error

        try:
            connection.commit()
        except psycopg.Error as error:
            raise AppendError(_commit_outcome(error), error) from error
    finally:
        connection.close()

    return StoredEvent(
        id=str(row[0]),
        received_at=row[1],
        event_type=row[2],
        actor=row[3],
        content=row[4],
        refs=row[5],
    )


def _validate_event_type(event_type: str) -> None:
    if not isinstance(event_type, str):
        raise TypeError("duro: event_type must be a string")
    try:
        byte_length = len(event_type.encode("utf-8"))
    except UnicodeEncodeError as error:
        raise ValueError("duro: event_type must be valid UTF-8") from error
    if not event_type.strip(_WHITESPACE):
        raise ValueError("duro: event_type is required (nonblank)")
    if byte_length > 255:
        raise ValueError("duro: event_type exceeds 255 UTF-8 bytes")


def _json_object(name: str, value: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"duro: {name} must be a JSON object")
    value = dict(value)
    try:
        json.dumps(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"duro: {name} must be JSON-serializable") from error
    return value


def _commit_outcome(error: psycopg.Error) -> str:
    sqlstate = error.sqlstate
    severity = getattr(getattr(error, "diag", None), "severity", None)
    if (
        severity in {"FATAL", "PANIC"}
        or (sqlstate and sqlstate.startswith("08"))
        or sqlstate in {"57P01", "57P02", "57P03"}
        or not sqlstate
    ):
        return "unknown"
    return "not_committed"
