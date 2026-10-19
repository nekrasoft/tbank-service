"""Тесты email-уведомлений по счетам."""
from __future__ import annotations

from io import BytesIO

from PIL import Image

from src.notifications.invoice_reminder_email import _normalize_email_attachments


def _build_image_bytes(image_format: str) -> bytes:
    output = BytesIO()
    Image.new("RGB", (2, 2), "red").save(output, format=image_format)
    return output.getvalue()


def test_webp_work_file_is_converted_to_jpeg_attachment() -> None:
    attachments = _normalize_email_attachments(
        [
            {
                "file_name": "waybill.webp",
                "content_type": "image/webp",
                "file_data": _build_image_bytes("WEBP"),
            }
        ]
    )

    assert len(attachments) == 1
    attachment = attachments[0]
    assert attachment["file_name"] == "waybill.jpg"
    assert attachment["maintype"] == "image"
    assert attachment["subtype"] == "jpeg"
    assert attachment["file_data"].startswith(b"\xff\xd8\xff")


def test_png_work_file_keeps_original_format() -> None:
    png_data = _build_image_bytes("PNG")

    attachments = _normalize_email_attachments(
        [
            {
                "file_name": "waybill.png",
                "content_type": "image/png",
                "file_data": png_data,
            }
        ]
    )

    assert attachments == [
        {
            "file_name": "waybill.png",
            "file_data": png_data,
            "maintype": "image",
            "subtype": "png",
        }
    ]


def test_work_files_email_contains_only_tickets_even_when_both_are_jpegs():
    from datetime import date
    from unittest.mock import patch
    from src.notifications.invoice_reminder_email import send_invoice_work_files
    jpeg = _build_image_bytes('JPEG')
    attachments = [
        {'kind': 'site_photo', 'file_name': 'site.jpg', 'content_type': 'image/jpeg', 'file_data': jpeg},
        {'kind': 'container_waybill', 'file_name': 'ticket.jpg', 'content_type': 'image/jpeg', 'file_data': jpeg},
        {'kind': 'container_waybill', 'file_name': 'ticket.pdf', 'content_type': 'application/pdf', 'file_data': b'%PDF-1.4'},
    ]
    with patch('src.notifications.invoice_reminder_email._send_invoice_email') as send:
        send_invoice_work_files(recipients='test@example.com', invoice_number='1', counterparty_name='Клиент',
            invoice_date=date(2026, 10, 19), payment_link=None, pdf_url=None, attachments=attachments)
    assert [item['file_name'] for item in send.call_args.kwargs['attachments']] == ['ticket.jpg', 'ticket.pdf']
    with patch('src.notifications.invoice_reminder_email._send_invoice_email') as send:
        send_invoice_work_files(recipients='test@example.com', invoice_number='1', counterparty_name='Клиент',
            invoice_date=date(2026, 10, 19), payment_link=None, pdf_url=None, attachments=attachments[:1])
    send.assert_not_called()
