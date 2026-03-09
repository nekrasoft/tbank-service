"""
Генерация акта выполненных работ (PDF).
"""
from __future__ import annotations

import io
import logging
from datetime import date
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

logger = logging.getLogger(__name__)


def generate_act_pdf(
    *,
    counterparty_name: str,
    invoice_number: str,
    invoice_date: date,
    items: list[dict[str, Any]],
) -> bytes:
    """
    Генерация PDF акта выполненных работ.

    :param counterparty_name: Наименование контрагента
    :param invoice_number: Номер счёта
    :param invoice_date: Дата счёта
    :param items: Позиции [{name, price, amount, unit, vat}]
    :return: PDF в виде bytes
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=20 * mm,
        leftMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )
    styles = getSampleStyleSheet()

    story = []
    story.append(Paragraph("Акт выполненных работ", styles["Title"]))
    story.append(Spacer(1, 12))

    story.append(Paragraph(
        f"Контрагент: {counterparty_name}",
        styles["Normal"],
    ))
    story.append(Paragraph(
        f"Номер счёта: {invoice_number}",
        styles["Normal"],
    ))
    story.append(Paragraph(
        f"Дата: {invoice_date.strftime('%d.%m.%Y')}",
        styles["Normal"],
    ))
    story.append(Spacer(1, 16))

    # Таблица позиций
    table_data = [["№", "Наименование", "Кол-во", "Ед.", "Цена", "Сумма"]]
    total = 0.0
    for i, item in enumerate(items, 1):
        amount = float(item.get("amount", 0))
        price = float(item.get("price", 0))
        sum_row = round(amount * price, 2)
        total += sum_row
        table_data.append([
            str(i),
            str(item.get("name", ""))[:80],
            str(amount),
            str(item.get("unit", "ед.")),
            f"{price:.2f}",
            f"{sum_row:.2f}",
        ])
    table_data.append(["", "", "", "", "Итого:", f"{total:.2f}"])

    t = Table(table_data, colWidths=[20 * mm, 70 * mm, 25 * mm, 20 * mm, 30 * mm, 35 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (1, 0), (1, -1), "LEFT"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("BACKGROUND", (0, 1), (-1, -2), colors.beige),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(t)

    doc.build(story)
    return buffer.getvalue()
