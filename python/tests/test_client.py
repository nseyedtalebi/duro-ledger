import os
import pathlib
import subprocess
import unittest
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import psycopg
from psycopg import conninfo, sql

from duro import AppendError, append_event


class AppendEventTests(unittest.TestCase):
    @patch("duro.psycopg.connect")
    def test_append_event_commits_and_returns_database_row(self, connect):
        cursor = MagicMock()
        cursor.fetchone.return_value = (
            uuid.UUID("01923456-789a-7bcd-8123-456789abcdef"),
            datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc),
            "rador.etl.completed",
            "duro_writer",
            {"run_id": "run-1"},
            {"source": "rador"},
        )
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connect.return_value = connection

        event = append_event(
            "rador.etl.completed",
            content={"run_id": "run-1"},
            refs={"source": "rador"},
            dsn="postgresql://duro_writer@localhost/duro",
        )

        self.assertEqual(event.id, "01923456-789a-7bcd-8123-456789abcdef")
        self.assertEqual(event.actor, "duro_writer")
        self.assertEqual(event.content, {"run_id": "run-1"})
        connection.commit.assert_called_once_with()
        sql, params = cursor.execute.call_args.args
        self.assertIn("INSERT INTO public.events", sql)
        self.assertEqual(params[0], "rador.etl.completed")
        self.assertEqual(params[1].obj, {"run_id": "run-1"})
        self.assertEqual(params[2].obj, {"source": "rador"})

    @patch("duro.psycopg.connect")
    def test_append_event_reports_unknown_when_commit_confirmation_is_lost(self, connect):
        cursor = MagicMock()
        cursor.fetchone.return_value = (
            uuid.UUID("01923456-789a-7bcd-8123-456789abcdef"),
            datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc),
            "rador.etl.completed",
            "duro_writer",
            {},
            {},
        )
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connection.commit.side_effect = psycopg.OperationalError("connection lost")
        connect.return_value = connection

        with self.assertRaisesRegex(AppendError, "append unknown") as raised:
            append_event("rador.etl.completed", dsn="postgresql://duro_writer@localhost/duro")

        self.assertEqual(raised.exception.outcome, "unknown")
        connection.close.assert_called_once_with()


class LiveAppendEventTests(unittest.TestCase):
    def test_append_event_uses_restricted_writer_against_postgres(self):
        admin_dsn = os.environ.get("DURO_POSTGRES_TEST_DSN")
        if not admin_dsn:
            self.fail("DURO_POSTGRES_TEST_DSN is not set: live integration is required")

        suffix = uuid.uuid4().hex
        database = f"duro_python_{suffix}"
        role = f"duro_python_{suffix}"
        password = "duro-test-password"
        root = pathlib.Path(__file__).resolve().parents[2]

        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )

        database_dsn = conninfo.make_conninfo(admin_dsn, dbname=database)
        writer_dsn = conninfo.make_conninfo(database_dsn, user=role, password=password)
        try:
            initialized = subprocess.run(
                [
                    "go",
                    "run",
                    "./cmd/duro",
                    "init",
                    "--postgres",
                    database_dsn,
                    "--writer",
                    role,
                ],
                check=False,
                cwd=root,
                capture_output=True,
                text=True,
            )
            self.assertEqual(initialized.returncode, 0, initialized.stderr)

            event = append_event(
                "python.client.appended",
                content={"producer": "python"},
                refs={"test": "live"},
                dsn=writer_dsn,
            )
            self.assertEqual(event.actor, role)
            self.assertEqual(event.event_type, "python.client.appended")
            self.assertEqual(event.content, {"producer": "python"})
            self.assertEqual(event.refs, {"test": "live"})
        finally:
            with psycopg.connect(admin_dsn, autocommit=True) as admin:
                admin.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(database)
                    )
                )
                admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


if __name__ == "__main__":
    unittest.main()
