"""Tests for bundled LGA code lookup."""

from __future__ import annotations

from dapr_portal.vic_admin import (
    lga_name_for_code,
    lga_name_from_planning_candidate,
    normalize_lga_key,
)


def test_lga_name_for_code_known() -> None:
    assert lga_name_for_code("312") == "CASEY"
    assert lga_name_for_code(312) == "CASEY"


def test_lga_name_for_code_missing() -> None:
    assert lga_name_for_code("99999") is None
    assert lga_name_for_code(None) is None


def test_normalize_lga_key() -> None:
    assert normalize_lga_key("  casey  ") == "CASEY"
    assert normalize_lga_key(None) is None


def test_lga_name_from_planning_candidate() -> None:
    assert lga_name_from_planning_candidate("GREATER DANDENONG") == "GREATER DANDENONG"
