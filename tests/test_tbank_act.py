from datetime import date
from decimal import Decimal
from unittest.mock import Mock, patch

import httpx
import pytest

from src.tbank.client import send_provided_services_act


@pytest.mark.parametrize("status", [200, 403])
def test_send_act_payload_and_errors(monkeypatch, status):
    monkeypatch.setenv("TBANK_TOKEN", "test-token")
    monkeypatch.setenv("TBANK_API_URL", "https://bank.example/api/v1/")
    response = httpx.Response(
        status, json={"documentId": "act-id"},
        request=httpx.Request("POST", "https://bank.example/api/v1/providedServicesAct/submit"),
    )
    client = Mock()
    client.post.return_value = response
    with patch("src.tbank.client.httpx.Client") as factory:
        factory.return_value.__enter__.return_value = client
        kwargs = dict(
            document_number="123", act_date=date(2026, 11, 10),
            buyer_name="Покупатель", buyer_inn="1234567890", buyer_kpp="invalid",
            items=[{"name": "Услуга", "price": Decimal("12.50"), "amount": Decimal("1.5")}],
            email="buyer@example.com; BUYER@example.com", contact_phone=" +79991234567 ",
            header_comment="К счёту №123 от 10.11.2026",
        )
        if status == 200:
            assert send_provided_services_act(**kwargs) == {"documentId": "act-id"}
        else:
            with pytest.raises(httpx.HTTPStatusError):
                send_provided_services_act(**kwargs)
    args, call_kwargs = client.post.call_args
    assert args == ("https://bank.example/api/v1/providedServicesAct/submit",)
    assert call_kwargs["headers"]["Authorization"] == "Bearer test-token"
    assert call_kwargs["json"] == {
        "documentNumber": "123", "date": "2026-11-10",
        "buyer": {"name": "Покупатель", "inn": "1234567890"},
        "items": [{"name": "Услуга", "price": 12.5, "amount": 1.5, "unit": "шт", "vat": "None"}],
        "contacts": [{"email": "buyer@example.com"}],
        "contactPhone": "+79991234567", "headerComment": "К счёту №123 от 10.11.2026",
    }


@pytest.mark.parametrize("failure", [None, "invoice", "save"])
def test_cron_act_creation_is_disabled(monkeypatch, failure):
    from src.cli import cron
    from src.notifications import bitrix_task, invoice_reminder_email, max as max_notifications, telegram
    from src.sheets import sync, writer
    from src.tbank import client

    prepared = {
        "invoice_id": 1, "invoice_number": "123", "invoice_date": date(2026, 11, 10),
        "due_date": date(2026, 11, 20), "payer_name": "Покупатель", "payer_inn": "1234567890",
        "payer_kpp": "123456789", "items": [{"name": "Услуга", "price": 100, "amount": 1}],
        "email": "buyer@example.com", "contact_phone": "+79991234567", "comment": "Работы",
        "counterparty_name": "Покупатель", "counterparty_short_name": "Покупатель",
        "bitrix_company_id": None, "sheet_row_hashes": ["row"],
    }
    monkeypatch.setattr(cron, "DEBUG_FORCE_EMAIL", "debug@example.com")
    monkeypatch.setattr(sync, "sync_sheets_to_mysql", Mock())
    monkeypatch.setattr(cron, "_get_uninvoiced_counterparties", lambda: ["Покупатель"])
    monkeypatch.setattr(cron, "_prepare_pending_invoices", lambda *args: [prepared])
    monkeypatch.setattr(cron.time, "sleep", Mock())
    events = Mock()
    send_invoice = Mock(return_value={"invoiceId": "invoice-id"})
    save_invoice = Mock()
    send_act = Mock(return_value={"documentId": "act-id"})
    failed_invoice = Mock()
    notification = Mock()
    for name, mock in (("invoice", send_invoice), ("save", save_invoice), ("act", send_act)):
        events.attach_mock(mock, name)
        if failure == name:
            mock.side_effect = RuntimeError(name)
    monkeypatch.setattr(client, "send_invoice", send_invoice)
    monkeypatch.setattr(client, "send_provided_services_act", send_act)
    monkeypatch.setattr(cron, "_mark_invoice_issued", save_invoice)
    monkeypatch.setattr(cron, "_mark_invoice_failed", failed_invoice)
    monkeypatch.setattr(writer, "mark_document_in_sheet", Mock(return_value=1))
    monkeypatch.setattr(telegram, "send_invoice_notification_bytes", notification)
    monkeypatch.setattr(bitrix_task, "create_invoice_task_with_meta", Mock(return_value=None))
    monkeypatch.setattr(max_notifications, "send_invoice_notification", Mock())
    monkeypatch.setattr(invoice_reminder_email, "send_invoice_work_files", Mock())

    if failure:
        with pytest.raises(SystemExit) as exc:
            cron.main()
        assert exc.value.code == 1
    else:
        cron.main()
    assert [call[0] for call in events.mock_calls] == {
        None: ["invoice", "save"], "invoice": ["invoice"],
        "save": ["invoice", "save"],
    }[failure]
    assert failed_invoice.call_count == (1 if failure == "invoice" else 0)
    assert notification.call_count == (1 if failure is None else 0)
    send_act.assert_not_called()
