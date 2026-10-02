from src.sheets.waybill_notes import extract_waybill_tokens


def test_extracts_multiple_waybill_tokens() -> None:
    note, tokens = extract_waybill_tokens(
        "Знак # 1,2 [ПЛ:wb_12345678] [ПЛ:wb_87654321]"
    )

    assert note == "Знак # 1,2"
    assert tokens == ["wb_12345678", "wb_87654321"]
