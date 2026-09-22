from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.db.models import Work
from src.invoice.builder import build_invoice_items


def test_fractional_container_quantity_is_multiplied_by_price() -> None:
    work = Work(
        id=1,
        date=date(2026, 9, 22),
        counterparty_name="Заказчик",
        structure="ЮЛ - Контейнеры",
        operation="Поступление по основной деятельности",
        object_count="1,5",
        sheet_row_hash="fractional-pickup",
    )
    price = SimpleNamespace(price=Decimal("1000.00"), vat="None")

    with patch(
        "src.invoice.builder.prices_repo.get_by_counterparty_and_operation",
        return_value=price,
    ):
        items = build_invoice_items(MagicMock(), [work], counterparty_id=1)

    assert items[0]["amount"] == 1.5
    assert Decimal(str(items[0]["price"])) * Decimal(str(items[0]["amount"])) == Decimal("1500.00")
