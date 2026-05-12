"""
Unit tests for src.db.repos.works
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, call

import pytest
from sqlalchemy.orm import Session

from src.db.repos import works as works_repo
from src.db.models import Work


# Helper to create a mock Work instance
def make_work(
    wid: int,
    counterparty_name: str = "Test Counterparty",
    work_date: date = date(2026, 5, 10),
    invoice_id: int | None = None,
    sheet_row_hash: str = "hash123",
) -> Work:
    w = Work()
    w.id = wid
    w.counterparty_name = counterparty_name
    w.date = work_date
    w.invoice_id = invoice_id
    w.sheet_row_hash = sheet_row_hash
    # other fields not used in tests can stay as defaults
    return w


def test_get_uninvoiced_by_counterparty():
    session = MagicMock(spec=Session)
    # mock result scalars .all()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [make_work(1), make_work(2)]
    session.execute.return_value = mock_result

    result = works_repo.get_uninvoiced_by_counterparty(session, "Test Counterparty")

    assert len(result) == 2
    # verify that session.execute was called with a select statement
    session.execute.assert_called_once()
    # check that the where clauses include invoice_id.is_(None) and counterparty_name
    # We can inspect the call args but for simplicity just ensure no exception


def test_get_by_counterparty():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [make_work(1, invoice_id=5)]
    session.execute.return_value = mock_result

    result = works_repo.get_by_counterparty(session, "Test Counterparty")
    assert len(result) == 1
    assert result[0].invoice_id == 5
    session.execute.assert_called_once()


def test_get_uninvoiced_by_counterparty_for_update():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [make_work(1)]
    session.execute.return_value = mock_result

    result = works_repo.get_uninvoiced_by_counterparty_for_update(session, "Test Counterparty")
    assert len(result) == 1
    session.execute.assert_called_once()


def test_get_all_uninvoiced_counterparties():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.all.return_value = [("CounterA",), ("CounterB",)]
    session.execute.return_value = mock_result

    result = works_repo.get_all_uninvoiced_counterparties(session)
    assert result == ["CounterA", "CounterB"]
    session.execute.assert_called_once()


def test_count_uninvoiced_before_date():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalar.return_value = 7
    session.execute.return_value = mock_result

    count = works_repo.count_uninvoiced_before_date(session, "Test Counterparty", before_date=date(2026, 5, 1))
    assert count == 7
    session.execute.assert_called_once()


def test_get_max_date():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalar.return_value = date(2026, 5, 15)
    session.execute.return_value = mock_result

    max_date = works_repo.get_max_date(session)
    assert max_date == date(2026, 5, 15)
    session.execute.assert_called_once()


def test_exists_by_hash_true():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = make_work(1)  # any object triggers not None
    session.execute.return_value = mock_result

    exists = works_repo.exists_by_hash(session, "somehash")
    assert exists is True
    session.execute.assert_called_once()


def test_exists_by_hash_false():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = None
    session.execute.return_value = mock_result

    exists = works_repo.exists_by_hash(session, "somehash")
    assert exists is False
    session.execute.assert_called_once()


def test_get_by_hash_found():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = make_work(42)
    session.execute.return_value = mock_result

    work = works_repo.get_by_hash(session, "hashx")
    assert work is not None
    assert work.id == 42
    session.execute.assert_called_once()


def test_get_by_hash_not_found():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = None
    session.execute.return_value = mock_result

    work = works_repo.get_by_hash(session, "hashx")
    assert work is None
    session.execute.assert_called_once()


def test_create():
    session = MagicMock(spec=Session)
    # mock session.add, flush, refresh
    session.add = MagicMock()
    session.flush = MagicMock()
    session.refresh = MagicMock()

    work = works_repo.create(
        session,
        date=date(2026, 5, 10),
        counterparty_name="Test CP",
        note="note",
        structure="struct",
        operation="op",
        object_count="2",
        volume=Decimal("1.5"),
        revenue=Decimal("100.0"),
        sheet_row_hash="abcd1234",
    )

    # ensure add, flush, refresh called
    session.add.assert_called_once()
    session.flush.assert_called_once()
    session.refresh.assert_called_once()
    # check that work id is not set (since we didn't set it) but we can assert attributes
    assert work.counterparty_name == "Test CP"
    assert work.note == "note"
    assert work.volume == Decimal("1.5")


def test_update_revenue_by_hash_if_uninvoiced():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.rowcount = 3
    session.execute.return_value = mock_result

    rc = works_repo.update_revenue_by_hash_if_uninvoiced(session, sheet_row_hash="hash", revenue=Decimal("50"))
    assert rc == 3
    session.execute.assert_called_once()


def test_update_revenue_by_hash():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.rowcount = 1
    session.execute.return_value = mock_result

    rc = works_repo.update_revenue_by_hash(session, sheet_row_hash="hash", revenue=Decimal("50"))
    assert rc == 1
    session.execute.assert_called_once()


def test_update_volume_by_hash():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.rowcount = 0
    session.execute.return_value = mock_result

    rc = works_repo.update_volume_by_hash(session, sheet_row_hash="hash", volume=Decimal("2"))
    assert rc == 0
    session.execute.assert_called_once()


def test_update_invoice_id():
    session = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.rowcount = 2
    session.execute.return_value = mock_result

    rc = works_repo.update_invoice_id(session, work_ids=[10, 11], invoice_id=5)
    assert rc == 2
    session.execute.assert_called_once()


def test_update_invoice_id_empty_list():
    session = MagicMock(spec=Session)
    rc = works_repo.update_invoice_id(session, work_ids=[], invoice_id=5)
    assert rc == 0
    session.execute.assert_not_called()