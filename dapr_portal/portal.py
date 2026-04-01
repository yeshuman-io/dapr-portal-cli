"""Fetch portal HTML, parse embedded crypto material, and retrieve assets."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import Iterable

import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

DEFAULT_BASE_URL = "https://dapr.powercor.com.au"
ROSETTA_LAYER_BASE = (
    "https://content.rosettaanalytics.com.au/citipower_powercor_layers_serve_2025/"
)

_ARRAY_RE = re.compile(r"var _0x7b70=\[(.*?)\];", re.DOTALL)
_SERVE_TS_RE = re.compile(
    r'const t="/serve\.php\?file=",e="&timestamp=(\d+)"'
)
_ALT_SERVE_TS_RE = re.compile(
    r'includes\("\.csv"\)\)\{[^}]*e="&timestamp=(\d+)"'
)


def _parse_js_string_array(arr_body: str) -> list[str]:
    strings: list[str] = []
    i = 0
    body = arr_body
    while i < len(body):
        if body[i] == '"':
            i += 1
            parts: list[str] = []
            while i < len(body):
                if body[i] == "\\":
                    if i + 1 < len(body) and body[i + 1] == "x" and i + 3 < len(body):
                        parts.append(chr(int(body[i + 2 : i + 4], 16)))
                        i += 4
                        continue
                    if i + 1 < len(body):
                        parts.append(body[i + 1])
                        i += 2
                        continue
                if body[i] == '"':
                    strings.append("".join(parts))
                    i += 1
                    break
                parts.append(body[i])
                i += 1
            continue
        i += 1
    return strings


def parse_portal_strings(html: str) -> list[str]:
    m = _ARRAY_RE.search(html)
    if not m:
        raise ValueError("portal HTML missing expected string table (_0x7b70)")
    return _parse_js_string_array(m.group(1))


def extract_aes_material(strings: list[str]) -> tuple[str, str]:
    if len(strings) <= 700:
        raise ValueError("string table too short for AES key material")
    key_hex, iv_hex = strings[699], strings[700]
    if len(key_hex) != 64 or len(iv_hex) != 32:
        raise ValueError("unexpected AES key or IV length in portal page")
    return key_hex, iv_hex


def extract_serve_timestamp(html: str) -> str:
    m = _SERVE_TS_RE.search(html)
    if m:
        return m.group(1)
    m2 = _ALT_SERVE_TS_RE.search(html)
    if m2:
        return m2.group(1)
    raise ValueError("could not find serve.php timestamp in portal HTML")


def iter_csv_paths(strings: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for s in strings:
        if "powercor_data/" in s and ".csv" in s.lower() and s.startswith("./"):
            if s not in seen:
                seen.add(s)
                out.append(s)
    return sorted(out)


def iter_static_powercor_links(html: str) -> list[str]:
    """Paths like ./powercor_data/foo.pdf from anchor hrefs."""
    paths: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(
        r'href="(\./powercor_data/[^"?#]+)(?:\?[^"#]*)?"', html, re.IGNORECASE
    ):
        p = m.group(1)
        if p not in seen:
            seen.add(p)
            paths.append(p)
    return sorted(paths)


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        raise ValueError("empty plaintext")
    pad = data[-1]
    if pad < 1 or pad > 16:
        raise ValueError("invalid PKCS#7 padding")
    if data[-pad:] != bytes([pad]) * pad:
        raise ValueError("invalid PKCS#7 padding")
    return data[:-pad]


def decrypt_aes_cbc_b64(ciphertext_b64: str, key_hex: str, iv_hex: str) -> str:
    key = bytes.fromhex(key_hex)
    iv = bytes.fromhex(iv_hex)
    ct = base64.b64decode(ciphertext_b64.strip())
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    dec = cipher.decryptor()
    pt = dec.update(ct) + dec.finalize()
    return _pkcs7_unpad(pt).decode("utf-8")


def build_serve_url(
    csv_portal_path: str, base_url: str, serve_timestamp: str
) -> str:
    """Match the jQuery hook: /serve.php?file=<path with ? -> &>&timestamp=<serve_ts>."""
    inner = csv_portal_path.replace("?", "&")
    return f"{base_url.rstrip('/')}/serve.php?file={inner}&timestamp={serve_timestamp}"


@dataclass
class PortalSession:
    base_url: str
    key_hex: str
    iv_hex: str
    serve_timestamp: str
    strings: list[str]

    @classmethod
    def from_html(cls, html: str, base_url: str = DEFAULT_BASE_URL) -> PortalSession:
        strings = parse_portal_strings(html)
        key_hex, iv_hex = extract_aes_material(strings)
        serve_ts = extract_serve_timestamp(html)
        return cls(
            base_url=base_url,
            key_hex=key_hex,
            iv_hex=iv_hex,
            serve_timestamp=serve_ts,
            strings=strings,
        )

    def csv_paths(self) -> list[str]:
        return iter_csv_paths(self.strings)


def fetch_portal_html(client: httpx.Client, base_url: str = DEFAULT_BASE_URL) -> str:
    r = client.get(base_url.rstrip("/") + "/", headers={"User-Agent": _ua()})
    r.raise_for_status()
    return r.text


def fetch_decrypted_csv(
    client: httpx.Client,
    session: PortalSession,
    csv_portal_path: str,
) -> str:
    url = build_serve_url(csv_portal_path, session.base_url, session.serve_timestamp)
    r = client.get(url, headers={"User-Agent": _ua()})
    r.raise_for_status()
    body = r.text.strip()
    if body.startswith("File not found") or "denied" in body.lower():
        raise RuntimeError(f"serve.php rejected request: {body[:200]}")
    return decrypt_aes_cbc_b64(body, session.key_hex, session.iv_hex)


def fetch_static(
    client: httpx.Client,
    session: PortalSession,
    path: str,
) -> bytes:
    path = path.strip()
    if not path.startswith("./"):
        path = "./" + path.lstrip("/")
    url = session.base_url.rstrip("/") + "/" + path[2:]
    r = client.get(url, headers={"User-Agent": _ua()})
    r.raise_for_status()
    return r.content


def fetch_rosetta_layer(
    client: httpx.Client,
    filename: str,
    query: str | None = None,
) -> bytes:
    """Plain-text (or other) layer files on the Rosetta CDN used by the map."""
    name = filename.lstrip("/")
    q = f"?{query}" if query else ""
    url = f"{ROSETTA_LAYER_BASE}{name}{q}"
    r = client.get(url, headers={"User-Agent": _ua()})
    r.raise_for_status()
    return r.content


def _ua() -> str:
    return "dapr-portal-cli/0.1 (+https://dapr.powercor.com.au/)"
