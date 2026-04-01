"""Rosetta line layer discovery from portal string table (offline)."""

from __future__ import annotations

from dapr_portal.portal import portal_map_line_layer_hints, rosetta_line_txt_from_portal_strings


def test_rosetta_line_txt_from_portal_strings_extracts_basenames() -> None:
    strings = [
        "noise",
        "https://content.rosettaanalytics.com.au/citipower_powercor_layers_serve_2025/22kV_Powercor_Lines.txt?112345678",
        "https://content.rosettaanalytics.com.au/citipower_powercor_layers_serve_2030/11kV_CitiPower_Powercor_Lines.txt",
        "duplicate",
        "https://content.rosettaanalytics.com.au/citipower_powercor_layers_serve_2025/22kV_Powercor_Lines.txt?x=1",
    ]
    out = rosetta_line_txt_from_portal_strings(strings)
    assert out == [
        "11kV_CitiPower_Powercor_Lines.txt",
        "22kV_Powercor_Lines.txt",
    ]


def test_portal_map_line_layer_hints_excludes_downloadable() -> None:
    dl = {"22kV_Powercor_Lines.txt", "22kV_CitiPower_Lines.txt"}
    strings = [
        "22kV_Powercor_Lines",
        "66kV_CitiPower_Powercor_Lines",
        "#66kV_CitiPower_Powercor_Lines",
        "500kV_Victorian_Transmission_Lines",
        "./powercor_data/x.csv",
        "https://example.com/66kV_Foo_Lines.txt",
    ]
    hints = portal_map_line_layer_hints(strings, downloadable_txt_basenames=dl)
    assert "66kV_CitiPower_Powercor_Lines" in hints
    assert "500kV_Victorian_Transmission_Lines" in hints
    assert "22kV_Powercor_Lines" not in hints
