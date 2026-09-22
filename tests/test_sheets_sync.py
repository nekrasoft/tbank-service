from decimal import Decimal

from src.sheets.sync import _prepare_work_note_and_volume


def test_prepare_tender_work_extracts_volume_from_note():
    note, volume = _prepare_work_note_and_volume(
        {
            "structure": "Тендеры - Вывоз мусора",
            "note": "Полигон, Объем: 154 м3",
            "object_count": "1",
        }
    )

    assert note == "Полигон"
    assert volume == Decimal("154.00")


def test_prepare_container_work_uses_fractional_object_count_for_volume():
    note, volume = _prepare_work_note_and_volume(
        {
            "structure": "ЮЛ - Контейнеры",
            "note": "Знак # 1,2",
            "object_count": "1,5",
        }
    )

    assert note == "Знак # 1,2"
    assert volume == Decimal("12.00")
