"""Tests for DAPR LGA summary CSV indexing and join."""

from __future__ import annotations

from dapr_portal.dapr_tables import index_lga_summary_by_type, join_lga_rows


def test_index_and_join_lga() -> None:
    csv_text = "LGA,Type,Value\nCASEY,Commercial,10\nCASEY,Residential,20\n"
    idx = index_lga_summary_by_type(csv_text)
    rows, ok = join_lga_rows(idx, "casey")
    assert ok is True
    assert rows is not None
    assert len(rows) == 2
    assert rows[0]["Type"] == "Commercial"


def test_join_no_match() -> None:
    csv_text = "LGA,Type\nFOO,BAR\n"
    idx = index_lga_summary_by_type(csv_text)
    rows, ok = join_lga_rows(idx, "CASEY")
    assert ok is False
    assert rows is None


def test_index_strips_utf8_bom_on_lga_column() -> None:
    csv_text = "\ufeffLGA,Type\nCASEY,X\n"
    idx = index_lga_summary_by_type(csv_text)
    rows, ok = join_lga_rows(idx, "CASEY")
    assert ok is True
    assert rows is not None
    assert len(rows) == 1
