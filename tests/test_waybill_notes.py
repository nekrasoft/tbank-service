from src.sheets.waybill_notes import extract_waybill_tokens


def test_extracts_multiple_waybill_tokens() -> None:
    note, tokens = extract_waybill_tokens(
        "Знак # 1,2 [ПЛ:wb_12345678] [ПЛ:wb_87654321]"
    )

    assert note == "Знак # 1,2"
    assert tokens == ["wb_12345678", "wb_87654321"]


def test_delivery_marker_does_not_appear_in_invoice_note() -> None:
    note, tokens = extract_waybill_tokens(
        "Знак # 1,2 [ПЛ:wb_12345678] [ВЫВОЗ:12345678-1234-1234-1234-123456789abc]"
    )
    assert note == "Знак # 1,2"
    assert tokens == ["wb_12345678"]
    assert extract_waybill_tokens("Знак [ВЫВОЗ:12345678-1234-1234-1234-123456789abc]") == ("Знак", [])
