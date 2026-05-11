"""
Unit tests for invoice splitter logic.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


def make_mock_work(note: str | None, work_id: int = 1) -> MagicMock:
    """Создаёт мок-объект Work с нужными полями."""
    work = MagicMock()
    work.id = work_id
    work.date = date(2026, 3, 15)
    work.counterparty_name = "Тестовый контрагент"
    work.note = note
    work.structure = None
    work.operation = None
    work.object_count = "1"
    work.volume = Decimal("8.0")
    work.revenue = Decimal("1000.00")
    work.sheet_row_hash = f"hash_{work_id}"
    work.invoice_id = None
    return work


class TestNormalizeGroupRules:
    """Tests for _normalize_group_rules internal function."""

    def test_empty_input(self):
        with patch("src.invoice.splitter._load_split_rules", return_value={}):
            from src.invoice.splitter import split_works_for_counterparty
            result = split_works_for_counterparty(
                counterparty_short_name="Test",
                works=[],
            )
            assert result == []

    def test_no_rules_returns_single_group(self):
        """Если нет правил — всё в одну группу."""
        with patch("src.invoice.splitter._load_split_rules", return_value={}):
            from src.invoice.splitter import split_works_for_counterparty
            works = [make_mock_work("note 1"), make_mock_work("note 2")]
            result = split_works_for_counterparty(
                counterparty_short_name="Test",
                works=works,
            )
            assert len(result) == 1
            assert result[0].key == "default"
            assert len(result[0].works) == 2

    def test_split_by_note_contains(self):
        """Разбиение по note_contains_any."""
        rules = {
            "counterparties": {
                "Тестовый": {
                    "groups": [
                        {
                            "key": "innograd",
                            "label": "Инноград",
                            "email": "test@innograd.ru",
                            "note_contains_any": ["инноград"],
                        },
                        {
                            "key": "other",
                            "label": "Остальное",
                            "default": True,
                        },
                    ]
                }
            }
        }
        with patch("src.invoice.splitter._load_split_rules", return_value=rules):
            from src.invoice.splitter import split_works_for_counterparty
            works = [
                make_mock_work("Вывоз из Иннограда", work_id=1),
                make_mock_work("Обычный вывоз", work_id=2),
                make_mock_work("Ещё из иннограда", work_id=3),
            ]
            result = split_works_for_counterparty(
                counterparty_short_name="Тестовый",
                works=works,
            )
            # Должно быть 2 группы
            assert len(result) == 2
            # Группа innograd
            innograd = next(g for g in result if g.key == "innograd")
            assert len(innograd.works) == 2
            assert innograd.email == "test@innograd.ru"
            # Группа default
            other = next(g for g in result if g.key == "other")
            assert len(other.works) == 1

    def test_case_insensitive_matching(self):
        """Поиск по note нечувствителен к регистру."""
        rules = {
            "counterparties": {
                "Тестовый": {
                    "groups": [
                        {
                            "key": "test",
                            "label": "Тест",
                            "note_contains_any": ["ТЕСТ"],
                        },
                    ]
                }
            }
        }
        with patch("src.invoice.splitter._load_split_rules", return_value=rules):
            from src.invoice.splitter import split_works_for_counterparty
            works = [
                make_mock_work("это тестовая работа", work_id=1),
                make_mock_work("ТЕСТОВАЯ", work_id=2),
            ]
            result = split_works_for_counterparty(
                counterparty_short_name="Тестовый",
                works=works,
            )
            test_group = next(g for g in result if g.key == "test")
            assert len(test_group.works) == 2

    def test_counterparty_name_case_insensitive(self):
        """Поиск контрагента нечувствителен к регистру."""
        rules = {
            "counterparties": {
                "тестовый": {
                    "groups": [
                        {
                            "key": "test",
                            "note_contains_any": ["test"],
                        },
                    ]
                }
            }
        }
        with patch("src.invoice.splitter._load_split_rules", return_value=rules):
            from src.invoice.splitter import split_works_for_counterparty
            works = [make_mock_work("test work")]
            result = split_works_for_counterparty(
                counterparty_short_name="ТЕСТОВЫЙ",  # Верхний регистр
                works=works,
            )
            assert len(result) == 1

    def test_default_group_added_if_missing(self):
        """Если default=true не задан — создаётся автоматически."""
        rules = {
            "counterparties": {
                "Тестовый": {
                    "groups": [
                        {
                            "key": "special",
                            "label": "Спец",
                            "note_contains_any": ["спец"],
                        },
                    ]
                }
            }
        }
        with patch("src.invoice.splitter._load_split_rules", return_value=rules):
            from src.invoice.splitter import split_works_for_counterparty
            works = [
                make_mock_work("обычная работа", work_id=1),
            ]
            result = split_works_for_counterparty(
                counterparty_short_name="Тестовый",
                works=works,
            )
            assert len(result) == 2
            default_group = next(g for g in result if g.key == "default")
            assert default_group.label == "Остальное"

    def test_multiple_emails_in_group(self):
        """Группа может иметь несколько email."""
        rules = {
            "counterparties": {
                "Тестовый": {
                    "groups": [
                        {
                            "key": "multi",
                            "label": "Мульти",
                            "email": ["a@test.ru", "b@test.ru"],
                        },
                    ]
                }
            }
        }
        with patch("src.invoice.splitter._load_split_rules", return_value=rules):
            from src.invoice.splitter import split_works_for_counterparty
            works = [make_mock_work("work")]
            result = split_works_for_counterparty(
                counterparty_short_name="Тестовый",
                works=works,
            )
            group = result[0]
            assert group.email == ["a@test.ru", "b@test.ru"]
