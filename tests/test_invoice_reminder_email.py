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
