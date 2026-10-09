"""Binance public archive (data.binance.vision) for USD-M futures: discovery, mirror, verification, parsing.

Public data only: no credentials, no account, no order endpoint. Three properties matter for research:

* DISCOVERY comes from the bucket listing, not from today's exchange symbol list, so contracts that were
  delisted are found the same way as contracts that still trade;
* every data file is checked against the archive's own ``.CHECKSUM`` (SHA-256) before it is used, and a file
  that fails is deleted, never parsed;
* the mirror is resumable and incremental: a verified file is not downloaded again, an interrupted download
  leaves only a ``.part`` file that is discarded.

Parsing returns exactly what the file holds. Nothing is filled, repaired or reordered here; the dataset
builder reports what it finds.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

ARCHIVE_URL = "https://data.binance.vision"
LISTING_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
KLINES_ROOT = "data/futures/um/monthly/klines/"
FUNDING_ROOT = "data/futures/um/monthly/fundingRate/"
ARCHIVE_CLIENT_VERSION = "binance-archive-client-1"
DAY_MS = 86_400_000
MAX_CSV_BYTES = 64 * 1024 * 1024
_S3 = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
_MONTH = re.compile(r"-(\d{4})-(\d{2})\.zip$")

Getter = Callable[[str], bytes]


class ArchiveError(RuntimeError):
    pass


class ChecksumMismatch(ArchiveError):
    pass


@dataclass(frozen=True)
class ArchiveObject:
    key: str
    size: int
    last_modified: str

    @property
    def name(self) -> str:
        return self.key.rsplit("/", 1)[-1]

    @property
    def month(self) -> Optional[str]:
        m = _MONTH.search(self.key)
        return f"{m.group(1)}-{m.group(2)}" if m else None


_local = threading.local()


def http_get(url: str, *, timeout: float = 60.0, retries: int = 6) -> bytes:
    """GET with bounded retry. 404 is an answer (``ArchiveError``), not something to retry."""
    import requests

    session = getattr(_local, "session", None)
    if session is None:
        session = _local.session = requests.Session()
    last: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 404:
                raise ArchiveError(f"not in the archive: {url}")
            if r.status_code == 200:
                return r.content
            last = ArchiveError(f"HTTP {r.status_code} for {url}")
        except ArchiveError as exc:
            if "not in the archive" in str(exc):
                raise
            last = exc
        except Exception as exc:  # network errors are retried, then reported
            last = exc
        time.sleep(min(30.0, 1.5 ** attempt))
    raise ArchiveError(f"could not fetch {url}: {last}")


def list_prefix(prefix: str, *, get: Getter = http_get) -> Tuple[List[str], List[ArchiveObject]]:
    """One level of the bucket: ``(sub-prefixes, objects)``, every page followed."""
    marker, prefixes, objects = "", [], []
    while True:
        query = {"prefix": prefix, "delimiter": "/"}
        if marker:
            query["marker"] = marker
        root = ET.fromstring(get(LISTING_URL + "?" + urllib.parse.urlencode(query)))
        prefixes += [e.find("s:Prefix", _S3).text for e in root.findall("s:CommonPrefixes", _S3)]
        for c in root.findall("s:Contents", _S3):
            objects.append(ArchiveObject(c.find("s:Key", _S3).text, int(c.find("s:Size", _S3).text),
                                         c.find("s:LastModified", _S3).text))
        if (root.findtext("s:IsTruncated", default="false", namespaces=_S3) or "").lower() != "true":
            return prefixes, objects
        nxt = root.findtext("s:NextMarker", default="", namespaces=_S3)
        marker = nxt or (objects[-1].key if objects else prefixes[-1])


def archive_symbols(root: str = KLINES_ROOT, *, get: Getter = http_get) -> List[str]:
    """Every symbol the archive has ever published under ``root``, delisted ones included."""
    prefixes, _ = list_prefix(root, get=get)
    return sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in prefixes)


def symbol_files(symbol: str, kind: str, *, first_month: str, last_month: str,
                 get: Getter = http_get) -> List[ArchiveObject]:
    """The monthly ``.zip`` files of one symbol inside ``[first_month, last_month]`` (``YYYY-MM``)."""
    prefix = f"{KLINES_ROOT}{symbol}/1d/" if kind == "klines" else f"{FUNDING_ROOT}{symbol}/"
    _, objects = list_prefix(prefix, get=get)
    return sorted((o for o in objects if o.key.endswith(".zip") and o.month and first_month <= o.month <= last_month),
                  key=lambda o: o.key)


def parse_checksum(text: str) -> str:
    """``<sha256>  <file name>`` as the archive publishes it."""
    token = text.strip().split()[0] if text.strip() else ""
    if not re.fullmatch(r"[0-9a-fA-F]{64}", token):
        raise ArchiveError("malformed .CHECKSUM file")
    return token.lower()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class ArchiveMirror:
    """A local, verified copy: ``<root>/<archive key>`` beside ``<archive key>.CHECKSUM``."""

    def __init__(self, root: Path, *, get: Getter = http_get):
        self.root = Path(root)
        self._get = get

    def local(self, key: str) -> Path:
        return self.root / key

    def verified_sha256(self, key: str) -> Optional[str]:
        """The SHA-256 of the local file when it matches its local checksum file; otherwise None."""
        data, check = self.local(key), self.local(key + ".CHECKSUM")
        if not (data.is_file() and check.is_file()):
            return None
        try:
            expected = parse_checksum(check.read_text(encoding="utf-8", errors="replace"))
        except ArchiveError:
            return None
        return expected if sha256_file(data) == expected else None

    def ensure(self, key: str) -> Dict[str, str]:
        """Make ``key`` present and verified. Returns its hash and whether it was already there."""
        have = self.verified_sha256(key)
        if have:
            return {"key": key, "sha256": have, "status": "CACHED"}
        data, check = self.local(key), self.local(key + ".CHECKSUM")
        data.parent.mkdir(parents=True, exist_ok=True)
        for attempt in (1, 2):
            expected = parse_checksum(self._get(f"{ARCHIVE_URL}/{key}.CHECKSUM").decode("utf-8", errors="replace"))
            body = self._get(f"{ARCHIVE_URL}/{key}")
            if hashlib.sha256(body).hexdigest() == expected:
                for path, payload in ((data, body), (check, f"{expected}  {data.name}\n".encode())):
                    part = path.with_name(path.name + ".part")
                    part.write_bytes(payload)
                    os.replace(part, path)          # a crash leaves a .part, never a half file under the real name
                return {"key": key, "sha256": expected, "status": "DOWNLOADED"}
            if attempt == 2:
                for path in (data, check):
                    path.unlink(missing_ok=True)
                raise ChecksumMismatch(f"{key}: downloaded bytes do not match the archive checksum")
        raise AssertionError("unreachable")

    def sync(self, keys: Sequence[str], *, workers: int = 16,
             progress: Optional[Callable[[int, int], None]] = None) -> List[Dict[str, str]]:
        """``ensure`` for many keys. A failure is returned as ``status: FAILED`` -- it never stops the others
        and is never silently dropped."""
        done, lock, results = [0], threading.Lock(), [None] * len(keys)

        def one(i: int) -> None:
            try:
                results[i] = self.ensure(keys[i])
            except Exception as exc:
                results[i] = {"key": keys[i], "sha256": "", "status": "FAILED", "error": str(exc)[:200]}
            with lock:
                done[0] += 1
                if progress and (done[0] % 500 == 0 or done[0] == len(keys)):
                    progress(done[0], len(keys))

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            list(pool.map(one, range(len(keys))))
        return [r for r in results if r is not None]


# ---------------------------------------------------------------------- parsing
def normalize_timestamp_ms(value: object) -> int:
    """Epoch MILLISECONDS. The archive has published seconds, milliseconds and microseconds at different
    times; anything else is refused rather than guessed."""
    n = int(str(value).strip())
    digits = len(str(abs(n)))
    if digits == 13:
        return n
    if digits == 16:
        if n % 1000:
            raise ArchiveError(f"microsecond timestamp {n} is not a whole millisecond")
        return n // 1000
    if digits == 10:
        return n * 1000
    raise ArchiveError(f"timestamp {value!r} is not epoch seconds, milliseconds or microseconds")


def _rows(path: Path) -> Iterable[List[str]]:
    with zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ArchiveError(f"{path.name}: expected exactly one CSV, found {len(names)}")
        if zf.getinfo(names[0]).file_size > MAX_CSV_BYTES:      # a monthly file is kilobytes; refuse a bomb
            raise ArchiveError(f"{path.name}: CSV member is implausibly large")
        with zf.open(names[0]) as fh:                           # read in memory: nothing is extracted to disk
            for row in csv.reader(io.TextIOWrapper(fh, encoding="utf-8", newline="")):
                if row:
                    yield row


def read_kline_zip(path: Path) -> List[Tuple[int, float, float, float, float, float, float, int]]:
    """``(open_time_ms, open, high, low, close, volume, quote_volume, trades)`` per row, in file order.
    A header row (present in newer files, absent in older ones) is skipped."""
    out = []
    for row in _rows(path):
        if not row[0].strip().lstrip("-").isdigit():
            continue
        if len(row) < 9:
            raise ArchiveError(f"{path.name}: kline row has {len(row)} columns")
        out.append((normalize_timestamp_ms(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]),
                    float(row[5]), float(row[7]), int(float(row[8]))))
    return out


def read_funding_zip(path: Path) -> List[Tuple[int, int, float]]:
    """``(calc_time_ms, funding_interval_hours, funding_rate)`` per row, in file order."""
    out = []
    for row in _rows(path):
        if not row[0].strip().lstrip("-").isdigit():
            continue
        if len(row) < 3:
            raise ArchiveError(f"{path.name}: funding row has {len(row)} columns")
        out.append((normalize_timestamp_ms(row[0]), int(float(row[1])), float(row[2])))
    return out


__all__ = ["ARCHIVE_URL", "LISTING_URL", "KLINES_ROOT", "FUNDING_ROOT", "ARCHIVE_CLIENT_VERSION", "DAY_MS",
           "ArchiveError", "ChecksumMismatch", "ArchiveObject", "ArchiveMirror", "http_get", "list_prefix",
           "archive_symbols", "symbol_files", "parse_checksum", "sha256_file", "normalize_timestamp_ms",
           "read_kline_zip", "read_funding_zip"]
