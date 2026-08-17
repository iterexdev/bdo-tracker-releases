"""Published price snapshot: the file FORMAT plus the app side of consuming it.

WHY this exists (DB measured 2026-08-16): the shipped tracker.sqlite classifies
1,630 items — 1,264 market / 245 vendor / 121 untradeable. A fresh install has
zero `prices` rows, so the first session start has to make up to 1,264 arsha
calls at 6 workers (market.py's deliberate WAF-safe cap), and even an
established install had 787 of its 1,264 market items past the 24 h TTL at the
time of writing. One publisher fetches once for everybody; every install
downloads a ~16 KB file instead. The per-item arsha path in market.py is
UNCHANGED and stays the fallback — nothing here replaces it.

──────────────────────────── FILE FORMAT (schema 1) ────────────────────────────
One JSON object, columnar. Parallel arrays, not per-item objects — MEASURED
2026-08-16 by encoding this install's 1,111 real cached prices both ways:

    columnar (this format)          16,474 B raw /  6,363 B gz
    per-item objects, ISO as_of     94,991 B raw /  8,547 B gz   (5.8x / 1.3x)
    columnar without age_min        13,254 B raw /  4,925 B gz

So per-item as-of stamps cost +3.2 KB raw / +1.4 KB gz over a bare id→price
file — the price of never stomping a fresher local row, and cheap at that.
Columns keep each value's overhead to its digits plus one comma, and put like
next to like so gzip's window sees long runs. Projected to a full ~4,000-item
category sweep the file is still only ~58 KB raw / ~22 KB gz.

    {
      "schema": 1,                              int, see SCHEMA_VERSION
      "region": "na",                           arsha region, must match config
      "generated_at": "2026-08-16T04:07:11+00:00",   ISO 8601, UTC, aware
      "count": 1264,                            MUST equal len(ids)
      "source": "arsha.io/v2/na",               free-text provenance, optional
      "ids":    [4001, 4002, 4003, ...],        ascending, unique, int
      "prices": [1234, 0, 55000, ...],          silver, >= 0, parallel to ids
      "age_min":[0, 12, 3, ...],                >= 0, parallel to ids
      "not_marketable": [4002, ...]             subset of ids, ascending
    }

  * `age_min[i]` is how many WHOLE MINUTES before `generated_at` that item's
    price was observed, so per-item as-of = generated_at - age_min minutes.
    Relative minutes because they are 1–3 characters instead of a 25-character
    absolute timestamp, and because a publish run that takes an hour to walk
    1,264 items must not claim every price is equally fresh. One-minute
    resolution is far finer than the hour-scale TTL that consumes it.
  * An id listed in `not_marketable` MUST carry price 0 — that is exactly the
    row shape market.py's `_store_refusal` writes, so a cached refusal survives
    the round trip as a first-class row rather than as a fake price of 0.
  * Transport may be gzip: `parse()` sniffs the gzip magic bytes and inflates,
    so the same bytes work whether the asset is `.json` or `.json.gz`. What we
    WRITE to the data dir is always the inflated JSON — a tester should be able
    to open it.

Forward compatibility: an app refuses any `schema` outside
[MIN_SCHEMA_VERSION, SCHEMA_VERSION] with SnapshotSchemaError. Older apps
therefore decline a newer publish cleanly and keep using arsha; they never
crash and never half-apply. Adding OPTIONAL keys is a non-breaking change and
must not bump `schema`; changing the meaning of an existing key must.

Rule 5 (never silently drop): every refusal, every skipped entry and every
network failure is counted and reported — through the typed errors below for
the low-level calls, and through SnapshotStatus.detail plus price_snapshot.log
for the two swallow-everything orchestrators.
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from bdo_tracker.config import PricingConfig

SCHEMA_VERSION = 1  # newest schema this app can write
MIN_SCHEMA_VERSION = 1  # oldest schema this app can still read

# Sanity-gate constants. A truncated or empty publish must never overwrite good
# local prices, so the gate compares the snapshot's item count against what we
# already know locally. The ratio is deliberately loose: the two populations are
# not identical (the snapshot covers market-classified items; `prices` also
# holds cached refusals and lags behind newly added items), so a tight ratio
# would reject honest publishes. Halving is not a rounding difference — it is a
# broken producer.
MIN_COUNT_RATIO = 0.5
# Absolute floor for a fresh install, where there is no local baseline at all.
# The smallest useful publish is one spot's worth of items (median spot needs 18
# market prices, measured 2026-08-16); 50 is comfortably below any real full
# publish and comfortably above a truncated one.
MIN_COUNT_ABSOLUTE = 50
# A publisher with a broken clock could stamp a snapshot in the future, which
# would keep every applied row inside the TTL long after the prices went stale.
# Six hours absorbs timezone/NTP slop without absorbing a wrong date.
MAX_FUTURE_SKEW_HOURS = 6.0
# 1,264 items measured 16.3 KB raw (2026-08-16). 8 MB is ~500x headroom and
# caps both the download and the gzip inflate (a compression bomb otherwise
# expands into memory before we ever look at it).
MAX_BODY_BYTES = 8 * 1024 * 1024
_HTTP_TIMEOUT = 20.0

LOG_NAME = "price_snapshot.log"
ETAG_SUFFIX = ".etag"
# Shipped seed lives in the app dir, same idiom as ui/dashboard.py:53 — resolves
# in dev AND in frozen builds (PyInstaller puts assets under _internal/assets).
_ASSETS = Path(__file__).resolve().parents[2] / "assets"

# Load-bearing outcome vocabulary, in the spirit of Price.source: these strings
# are logged and rendered, so callers may branch on them.
#   applied      snapshot parsed, passed the gate, rows written
#   not-modified server answered 304; the disk copy is already current
#   up-to-date   the fetched snapshot is no newer than the one on disk
#   refused      parsed/downloaded but failed the sanity gate (detail says why)
#   unavailable  nothing on disk / network or IO failure / httpx missing
#   disabled     pricing.snapshot_enabled is false
_OK_OUTCOMES = frozenset({"applied", "not-modified", "up-to-date"})


class SnapshotError(Exception):
    """Base for every snapshot refusal — callers can catch just this."""


class SnapshotParseError(SnapshotError):
    """The bytes are not a snapshot we can read (garbage, truncated, invalid)."""


class SnapshotSchemaError(SnapshotParseError):
    """Well-formed, but its `schema` is outside what this app understands.

    Separate from SnapshotParseError on purpose: this one means "the publisher
    is ahead of this build", which is a nudge-to-update, not a corruption."""


class SnapshotRejected(SnapshotError):
    """Parsed cleanly but failed the sanity gate (wrong region, collapsed
    item count, implausible timestamp). Never applied."""


def _log(msg: str) -> None:
    """price_snapshot.log in the data dir (= cwd in frozen builds). The two
    orchestrators below swallow every failure by design — same reasoning as
    ui/updater.py's log: this is the only way to debug a tester's install."""
    try:
        with Path(LOG_NAME).open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {msg}\n")
    except OSError:
        pass  # logging is best-effort — never the reason a launch dies


def _utc(value: str, field: str) -> datetime:
    """ISO 8601 -> aware UTC. Accepts a trailing 'Z' and treats a naive stamp
    as UTC (the publisher may not be Python)."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as err:
        raise SnapshotParseError(f"{field} is not ISO 8601: {value!r}") from err
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now(now: datetime | None) -> datetime:
    return now or datetime.now(timezone.utc)


@dataclass(frozen=True)
class SnapshotEntry:
    item_id: int
    price: int  # silver; 0 when not_marketable
    as_of: datetime  # aware UTC — when this price was observed, not published
    not_marketable: bool = False


@dataclass(frozen=True)
class PriceSnapshot:
    schema: int
    region: str
    generated_at: datetime  # aware UTC
    count: int  # == len(entries); cross-checked at parse
    entries: tuple[SnapshotEntry, ...]  # ascending by item_id
    source: str = ""  # free-text provenance, e.g. "arsha.io/v2/na"

    def age_hours(self, now: datetime | None = None) -> float:
        """How old the PUBLISH is. Per-item ages can be older still — that is
        what the per-entry as_of carries."""
        return (_now(now) - self.generated_at).total_seconds() / 3600.0

    def is_stale(self, max_age_hours: float, now: datetime | None = None) -> bool:
        """Past the point where the caller should stop trusting it and let the
        per-item arsha path take over (pricing.snapshot_max_age_hours)."""
        return self.age_hours(now) > max_age_hours


@dataclass(frozen=True)
class ApplyResult:
    """What actually landed in the `prices` table. Every entry is in exactly
    one bucket — the buckets sum to `total` (rule 5: nothing vanishes)."""

    total: int  # entries in the snapshot
    applied: int  # rows written
    skipped_newer: int  # local row is NEWER than the snapshot's as_of
    skipped_unknown: int  # item_id absent from the items table
    skipped_zero: int  # price <= 0 without not_marketable (see below)
    generated_at: datetime
    age_hours: float

    def summary(self) -> str:
        return (
            f"{self.applied}/{self.total} applied"
            f" (+{self.skipped_newer} local newer,"
            f" {self.skipped_unknown} unknown item,"
            f" {self.skipped_zero} zero-priced),"
            f" snapshot {self.age_hours:.1f} h old"
        )


@dataclass(frozen=True)
class SnapshotStatus:
    """One result type for both orchestrators. `detail` is always populated."""

    outcome: str  # see the vocabulary above
    detail: str
    source: str = ""  # "download" | "seed" | "network" | ""
    result: ApplyResult | None = None
    stale: bool = False  # older than pricing.snapshot_max_age_hours
    generated_at: datetime | None = None
    age_hours: float | None = None

    @property
    def ok(self) -> bool:
        return self.outcome in _OK_OUTCOMES


# ─────────────────────────────── format I/O ───────────────────────────────


def build(
    region: str,
    entries: Iterable[SnapshotEntry],
    *,
    generated_at: datetime | None = None,
    source: str = "",
) -> PriceSnapshot:
    """Producer-side constructor: sorts, de-dupes-or-raises, stamps the count.
    Kept here so the publisher and the consumer can never disagree about the
    format — there is exactly one writer of these bytes."""
    stamped = _now(generated_at)
    ordered = sorted(entries, key=lambda e: e.item_id)
    ids = [e.item_id for e in ordered]
    if len(set(ids)) != len(ids):
        raise SnapshotParseError("duplicate item_id in entries")
    return PriceSnapshot(
        schema=SCHEMA_VERSION,
        region=region.lower(),
        generated_at=stamped.astimezone(timezone.utc),
        count=len(ordered),
        entries=tuple(ordered),
        source=source,
    )


def encode(snapshot: PriceSnapshot, *, compress: bool = False) -> bytes:
    """Canonical serializer. `compress=True` gzips with mtime=0 so an unchanged
    price set re-publishes to BYTE-IDENTICAL output — a gzip header carrying the
    build time would change the ETag on every cron run and turn every client's
    conditional GET into a full download."""
    gen = snapshot.generated_at.astimezone(timezone.utc)
    ids: list[int] = []
    prices: list[int] = []
    age_min: list[int] = []
    not_marketable: list[int] = []
    for entry in snapshot.entries:
        ids.append(entry.item_id)
        prices.append(0 if entry.not_marketable else int(entry.price))
        # Clamp: a per-item clock ahead of the publish stamp would encode a
        # negative age, which the parser rejects. Treat it as "just now".
        minutes = int((gen - entry.as_of.astimezone(timezone.utc)).total_seconds() // 60)
        age_min.append(max(0, minutes))
        if entry.not_marketable:
            not_marketable.append(entry.item_id)
    payload = {
        "schema": snapshot.schema,
        "region": snapshot.region,
        "generated_at": gen.isoformat(),
        "count": len(ids),
        "source": snapshot.source,
        "ids": ids,
        "prices": prices,
        "age_min": age_min,
        "not_marketable": not_marketable,
    }
    raw = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
    return gzip.compress(raw, mtime=0) if compress else raw


def _inflate(data: bytes) -> bytes:
    """Transparent gunzip, bounded. Sniffing the magic bytes lets the same
    parser read a `.json` asset and a `.json.gz` one."""
    if data[:2] != b"\x1f\x8b":
        return data
    obj = zlib.decompressobj(31)  # 16 + MAX_WBITS = gzip wrapper
    try:
        out = obj.decompress(data, MAX_BODY_BYTES + 1)
    except zlib.error as err:
        raise SnapshotParseError(f"gzip inflate failed: {err}") from err
    if not obj.eof or len(out) > MAX_BODY_BYTES:
        raise SnapshotParseError(
            f"gzip payload is truncated or exceeds {MAX_BODY_BYTES} bytes"
        )
    return out


def _int_list(payload: dict, key: str, expected: int | None = None) -> list[int]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise SnapshotParseError(f"{key} must be a list")
    out: list[int] = []
    for item in value:
        # bool is an int subclass; a JSON true in a numeric column is a
        # producer bug, not a 1.
        if isinstance(item, bool) or not isinstance(item, int):
            raise SnapshotParseError(f"{key} must hold integers, got {item!r}")
        out.append(item)
    if expected is not None and len(out) != expected:
        raise SnapshotParseError(
            f"{key} has {len(out)} values, expected {expected} (arrays must be parallel)"
        )
    return out


def parse(data: bytes | str) -> PriceSnapshot:
    """Bytes (optionally gzipped) or text -> PriceSnapshot. Validates shape
    only; region/freshness/collapse are the sanity gate's job (`check`).

    Raises SnapshotSchemaError for a version we do not understand and
    SnapshotParseError for everything else malformed. Never returns a partial
    snapshot."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    if not isinstance(data, (bytes, bytearray)):
        raise SnapshotParseError(f"expected bytes or str, got {type(data).__name__}")
    if len(data) > MAX_BODY_BYTES:
        raise SnapshotParseError(f"snapshot exceeds {MAX_BODY_BYTES} bytes")
    if not data.strip():
        raise SnapshotParseError("snapshot is empty")

    raw = _inflate(bytes(data))
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as err:
        raise SnapshotParseError(f"not valid JSON: {err}") from err
    if not isinstance(payload, dict):
        raise SnapshotParseError("top level must be a JSON object")

    schema = payload.get("schema")
    if isinstance(schema, bool) or not isinstance(schema, int):
        raise SnapshotParseError(f"schema must be an integer, got {schema!r}")
    if not MIN_SCHEMA_VERSION <= schema <= SCHEMA_VERSION:
        raise SnapshotSchemaError(
            f"snapshot schema {schema} is outside this build's supported range "
            f"{MIN_SCHEMA_VERSION}..{SCHEMA_VERSION} — update the app"
        )

    region = payload.get("region")
    if not isinstance(region, str) or not region.strip():
        raise SnapshotParseError(f"region must be a non-empty string, got {region!r}")

    generated_at_raw = payload.get("generated_at")
    if not isinstance(generated_at_raw, str):
        raise SnapshotParseError("generated_at must be an ISO 8601 string")
    generated_at = _utc(generated_at_raw, "generated_at")

    ids = _int_list(payload, "ids")
    count = payload.get("count")
    if isinstance(count, bool) or not isinstance(count, int):
        raise SnapshotParseError(f"count must be an integer, got {count!r}")
    if count != len(ids):
        # The truncation detector: a producer that died mid-write leaves a
        # count that no longer matches the arrays it managed to emit.
        raise SnapshotParseError(f"count {count} != {len(ids)} ids — truncated publish?")
    prices = _int_list(payload, "prices", len(ids))
    age_min = _int_list(payload, "age_min", len(ids))
    nm_ids = set(_int_list(payload, "not_marketable"))
    source = payload.get("source", "")
    if not isinstance(source, str):
        raise SnapshotParseError("source must be a string")

    if len(set(ids)) != len(ids):
        raise SnapshotParseError("ids contains duplicates")
    unknown_nm = nm_ids - set(ids)
    if unknown_nm:
        raise SnapshotParseError(
            f"not_marketable lists {len(unknown_nm)} id(s) absent from ids"
        )

    entries: list[SnapshotEntry] = []
    for item_id, price, age in zip(ids, prices, age_min):
        if price < 0:
            raise SnapshotParseError(f"item {item_id} has negative price {price}")
        if age < 0:
            raise SnapshotParseError(f"item {item_id} has negative age_min {age}")
        flagged = item_id in nm_ids
        if flagged and price != 0:
            # Mirrors market.py's _store_refusal row shape exactly; a refusal
            # carrying a price would read as a real market price downstream.
            raise SnapshotParseError(
                f"item {item_id} is not_marketable but carries price {price}"
            )
        entries.append(
            SnapshotEntry(
                item_id=item_id,
                price=price,
                as_of=generated_at - timedelta(minutes=age),
                not_marketable=flagged,
            )
        )
    entries.sort(key=lambda e: e.item_id)
    return PriceSnapshot(
        schema=schema,
        region=region.strip().lower(),
        generated_at=generated_at,
        count=len(entries),
        entries=tuple(entries),
        source=source,
    )


# ──────────────────────────────── sanity gate ────────────────────────────────


def check(
    snapshot: PriceSnapshot,
    *,
    region: str,
    baseline_count: int = 0,
    now: datetime | None = None,
    min_ratio: float = MIN_COUNT_RATIO,
    min_count: int = MIN_COUNT_ABSOLUTE,
) -> None:
    """Refuse anything that must not be allowed to touch the prices table.

    Returns None on pass; raises SnapshotRejected with a human-readable reason
    otherwise. Deliberately NOT a bool: the reason has to reach the log and the
    UI (rule 5), and a bare False loses it.

    `baseline_count` is what we already know locally — see `baseline_count()`.
    Pass 0 on a fresh install to fall back to the absolute floor."""
    if snapshot.region.lower() != region.strip().lower():
        raise SnapshotRejected(
            f"snapshot region {snapshot.region!r} != configured region {region!r}"
        )
    ahead = (snapshot.generated_at - _now(now)).total_seconds() / 3600.0
    if ahead > MAX_FUTURE_SKEW_HOURS:
        raise SnapshotRejected(
            f"generated_at is {ahead:.1f} h in the future — publisher clock is wrong"
        )
    if snapshot.count < min_count:
        raise SnapshotRejected(
            f"only {snapshot.count} items, below the {min_count}-item floor"
        )
    floor = int(baseline_count * min_ratio)
    if baseline_count and snapshot.count < floor:
        raise SnapshotRejected(
            f"item count collapsed: {snapshot.count} vs {baseline_count} known locally"
            f" (floor {floor})"
        )


def baseline_count(conn: sqlite3.Connection) -> int:
    """Rows already in `prices` — the collapse gate's local yardstick. Not the
    market-item count from `items`: a fresh install has every item classified
    but zero prices, and gating a first publish against 1,264 would reject it."""
    try:
        return int(conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0])
    except sqlite3.Error:
        return 0


# ──────────────────────────────── apply ────────────────────────────────


def apply_snapshot(
    conn: sqlite3.Connection, snapshot: PriceSnapshot, *, now: datetime | None = None
) -> ApplyResult:
    """Write the snapshot into `prices`, honouring PER-ITEM as-of stamps.

    Three hard rules, all of them load-bearing:

    1. `fetched_at` gets the entry's own as_of, never the apply time. That is
       what makes market.py's existing TTL math (`_fresh_price`,
       `stale_market_count`) do the right thing with zero changes: an item the
       publisher read 30 h ago is still stale here and still gets an individual
       arsha call.
    2. A local row NEWER than the snapshot's entry is left alone. The per-item
       arsha fallback can legitimately have fetched something a minute ago; a
       snapshot published this morning must not stomp it back.
    3. `not_marketable` carries through as a first-class row (base_price 0,
       flag 1) — the same shape market.py's `_store_refusal` writes.

    Everything skipped is counted, never dropped (rule 5)."""
    stamp = _now(now)
    known = {
        int(row[0]) for row in conn.execute("SELECT item_id FROM items")
    }
    local: dict[int, str | None] = {
        int(row[0]): row[1]
        for row in conn.execute("SELECT item_id, fetched_at FROM prices")
    }

    rows: list[tuple[int, int, str, int]] = []
    skipped_newer = skipped_unknown = skipped_zero = 0
    for entry in snapshot.entries:
        if entry.item_id not in known:
            # prices.item_id has an FK to items; a snapshot from a newer
            # reference DB can legitimately name items this install has not
            # received yet. Counted, reported, refetched per-item as usual.
            skipped_unknown += 1
            continue
        if entry.price <= 0 and not entry.not_marketable:
            # market.py refuses to CACHE a market price of 0 (audit 2026-07-09)
            # because it would serve an unflagged 0 for the whole TTL. A
            # snapshot must not smuggle one in through the side door.
            skipped_zero += 1
            continue
        existing = local.get(entry.item_id)
        if existing is not None and _local_is_newer(existing, entry.as_of):
            skipped_newer += 1
            continue
        rows.append(
            (
                entry.item_id,
                int(entry.price),
                # timespec forced so every row we write has the same textual
                # layout as market.py's datetime.now(...).isoformat() rows —
                # stale_market_count compares fetched_at as a STRING.
                entry.as_of.astimezone(timezone.utc).isoformat(timespec="microseconds"),
                1 if entry.not_marketable else 0,
            )
        )

    if rows:
        with conn:
            conn.executemany(
                "INSERT OR REPLACE INTO prices"
                " (item_id, base_price, fetched_at, not_marketable)"
                " VALUES (?, ?, ?, ?)",
                rows,
            )
    return ApplyResult(
        total=len(snapshot.entries),
        applied=len(rows),
        skipped_newer=skipped_newer,
        skipped_unknown=skipped_unknown,
        skipped_zero=skipped_zero,
        generated_at=snapshot.generated_at,
        age_hours=snapshot.age_hours(stamp),
    )


def _local_is_newer(fetched_at: str | None, as_of: datetime) -> bool:
    """True when the row already in `prices` was fetched after the snapshot saw
    that item. An unparseable/absent local stamp counts as NOT newer: an
    unreadable timestamp is worse evidence than the snapshot's explicit one."""
    if not fetched_at:
        return False
    try:
        local = _utc(fetched_at, "fetched_at")
    except SnapshotParseError:
        return False
    return local > as_of


# ──────────────────────────────── disk ────────────────────────────────


def snapshot_filename(region: str) -> str:
    """Region in the name so switching regions in Settings does not leave the
    old region's file fighting the gate forever."""
    return f"price_snapshot_{region.strip().lower()}.json"


def data_path(region: str, data_dir: Path | str | None = None) -> Path:
    """The DOWNLOADED snapshot, in the data dir. Frozen builds chdir into
    %LOCALAPPDATA%/BDOLootTracker-Data (tools/launch_gui.py), so cwd is right
    by construction and this survives Velopack replacing the app folder."""
    return Path(data_dir or Path.cwd()) / snapshot_filename(region)


def etag_path(region: str, data_dir: Path | str | None = None) -> Path:
    path = data_path(region, data_dir)
    return path.with_name(path.name + ETAG_SUFFIX)


def seed_path(region: str, assets_dir: Path | None = None) -> Path | None:
    """The SEED snapshot shipped in the app dir, or None. `.json.gz` first —
    the installer carries the compressed one; `parse()` reads either."""
    assets = assets_dir or _ASSETS
    name = snapshot_filename(region)
    for candidate in (assets / (name + ".gz"), assets / name):
        if candidate.exists():
            return candidate
    return None


def read_etag(region: str, data_dir: Path | str | None = None) -> str | None:
    """The ETag of the bytes currently on disk — without it a 304 is impossible
    and every launch re-downloads the whole file."""
    try:
        text = etag_path(region, data_dir).read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return text or None


def save_snapshot(
    body: bytes,
    etag: str | None,
    region: str,
    data_dir: Path | str | None = None,
) -> Path:
    """Persist the INFLATED JSON plus its ETag, atomically (temp + os.replace,
    same reasoning as Config.save: a crash mid-write must not leave a truncated
    snapshot that refuses to parse at next launch).

    Raises OSError on failure — callers swallow-and-report."""
    path = data_path(region, data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(_inflate(body))
    os.replace(tmp, path)

    epath = etag_path(region, data_dir)
    if etag:
        etmp = epath.with_name(epath.name + ".tmp")
        etmp.write_text(etag, encoding="utf-8")
        os.replace(etmp, epath)
    else:
        # No ETag on this response: a leftover one would make the server 304 us
        # against bytes we just replaced, and we would never see the update.
        try:
            epath.unlink()
        except OSError:
            pass
    return path


def load_from_disk(
    region: str,
    data_dir: Path | str | None = None,
    assets_dir: Path | None = None,
) -> list[tuple[str, Path, PriceSnapshot | None, str]]:
    """Every snapshot present on disk, newest publish first.

    Returns (source, path, snapshot_or_None, error_detail) per candidate —
    source is "download" or "seed". A candidate that fails to parse is RETURNED
    with its reason rather than dropped, so the caller can log why the file it
    can see on disk was not used (rule 5)."""
    out: list[tuple[str, Path, PriceSnapshot | None, str]] = []
    candidates: list[tuple[str, Path]] = []
    downloaded = data_path(region, data_dir)
    if downloaded.exists():
        candidates.append(("download", downloaded))
    seed = seed_path(region, assets_dir)
    if seed is not None:
        candidates.append(("seed", seed))
    for source, path in candidates:
        try:
            snapshot = parse(path.read_bytes())
        except (SnapshotError, OSError) as err:
            out.append((source, path, None, f"{type(err).__name__}: {err}"))
        else:
            out.append((source, path, snapshot, ""))
    # Newest publish wins regardless of where it came from: a seed newer than a
    # months-old download happens right after an app update.
    out.sort(
        key=lambda row: row[2].generated_at if row[2] else datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return out


def newest_on_disk(
    region: str,
    data_dir: Path | str | None = None,
    assets_dir: Path | None = None,
) -> tuple[PriceSnapshot | None, str]:
    """Read-only freshness probe: (newest parseable snapshot or None, detail).

    Touches no database and writes nothing, so a UI note ("prices from a
    snapshot 3.2 h old") can call it on repaint, and the live worker can ask
    whether the snapshot is still inside pricing.snapshot_max_age_hours before
    deciding to lean on market.py's per-item path instead. The applied-result
    freshness in SnapshotStatus needs an apply to exist; this does not.

    `detail` explains an empty answer, and lists the reason for every candidate
    that failed to parse even when one succeeded (rule 5)."""
    rows = load_from_disk(region, data_dir, assets_dir)
    problems = [f"{src} ({p.name}): {err}" for src, p, snap, err in rows if snap is None]
    for _src, _path, snap, _err in rows:
        if snap is not None:
            note = f"snapshot from {snap.generated_at.isoformat()}"
            if problems:
                note += " | unusable: " + "; ".join(problems)
            return snap, note
    return None, "; ".join(problems) or "no price snapshot on disk"


# ──────────────────────────────── network ────────────────────────────────


@dataclass(frozen=True)
class DownloadResult:
    status: int  # HTTP status; 304 means "nothing to do"
    body: bytes  # empty on 304
    etag: str | None


def _default_fetcher(url: str, etag: str | None) -> DownloadResult:
    """Conditional GET via httpx — same injectable-fetcher policy as
    pricing/market.py, and httpx is already a hidden-import in the frozen build.

    No shared keep-alive client here (market.py has one because prefetch makes
    hundreds of calls): this runs once or twice per launch, and the request
    redirects to a CDN host anyway, so a pooled connection buys nothing.
    follow_redirects is REQUIRED — GitHub answers a release-asset download with
    a redirect to its object host."""
    import httpx

    headers = {"If-None-Match": etag} if etag else {}
    resp = httpx.get(url, headers=headers, timeout=_HTTP_TIMEOUT, follow_redirects=True)
    if resp.status_code == 304:
        return DownloadResult(304, b"", etag)
    resp.raise_for_status()
    body = resp.content
    if len(body) > MAX_BODY_BYTES:
        raise SnapshotParseError(
            f"snapshot download is {len(body)} bytes, over the {MAX_BODY_BYTES} cap"
        )
    return DownloadResult(resp.status_code, body, resp.headers.get("ETag"))


def snapshot_url(config: PricingConfig) -> str:
    """The configured URL with {region} filled in. A URL without the
    placeholder is used as-is, so a tester can point at a local file server."""
    return config.snapshot_url.replace("{region}", config.region.strip().lower())


# ─────────────────────────── orchestration ───────────────────────────
# The two calls a UI loader makes. Neither raises: a price snapshot is an
# optimisation, and nothing here may ever be the reason a launch fails.


def apply_best_available(
    conn: sqlite3.Connection,
    config: PricingConfig,
    *,
    now: datetime | None = None,
    data_dir: Path | str | None = None,
    assets_dir: Path | None = None,
) -> SnapshotStatus:
    """DISK-FIRST, zero network: apply the newest snapshot on disk that passes
    the gate. This is what lets a fresh offline install price everything at
    launch from the shipped seed."""
    if not config.snapshot_enabled:
        return SnapshotStatus("disabled", "price snapshots are turned off in settings")
    stamp = _now(now)
    try:
        candidates = load_from_disk(config.region, data_dir, assets_dir)
    except Exception as err:  # pragma: no cover - defensive, IO already guarded
        _log(f"disk scan failed: {err!r}")
        return SnapshotStatus("unavailable", f"could not read the snapshot dir: {err}")
    if not candidates:
        return SnapshotStatus("unavailable", "no price snapshot on disk")

    problems: list[str] = []
    # Unparseable candidates are collected BEFORE the apply loop, not inside it
    # (bug found 2026-08-16). load_from_disk sorts unparseable rows LAST, and
    # the loop returns on the first candidate that applies — so a corrupt
    # download beside a good seed was named nowhere: not in the returned
    # detail, not in the log. That is exactly what `problems` exists to
    # prevent (rule 5: never silently drop anything).
    for source, path, snapshot, error in candidates:
        if snapshot is None:
            problems.append(f"{source} ({path.name}): {error}")
            _log(f"unusable {source} snapshot {path}: {error}")

    base = baseline_count(conn)
    for source, path, snapshot, error in candidates:
        if snapshot is None:
            continue  # already reported in the pre-pass above
        try:
            check(snapshot, region=config.region, baseline_count=base, now=stamp)
        except SnapshotRejected as err:
            problems.append(f"{source} ({path.name}): {err}")
            _log(f"refused {source} snapshot {path}: {err}")
            continue
        try:
            result = apply_snapshot(conn, snapshot, now=stamp)
        except sqlite3.Error as err:
            problems.append(f"{source} ({path.name}): sqlite {err}")
            _log(f"apply failed for {path}: {err!r}")
            continue
        stale = snapshot.is_stale(config.snapshot_max_age_hours, stamp)
        detail = result.summary() + (" — STALE, per-item fetches take over" if stale else "")
        if problems:
            detail += " | skipped: " + "; ".join(problems)
        _log(f"applied {source} snapshot {path.name}: {detail}")
        return SnapshotStatus(
            outcome="applied",
            detail=detail,
            source=source,
            result=result,
            stale=stale,
            generated_at=snapshot.generated_at,
            age_hours=result.age_hours,
        )
    return SnapshotStatus(
        outcome="refused" if problems else "unavailable",
        detail="; ".join(problems) or "no usable price snapshot on disk",
    )


def _local_copy_parses(region: str, data_dir: Path | str | None = None) -> bool:
    """Is the DOWNLOADED snapshot on disk something we could actually serve?

    Guards the conditional GET's ETag: a 304 is only a correct answer when we
    still hold the bytes it refers to. Deliberately re-parses rather than just
    checking existence — a truncated file exists too, and the whole point is
    that an unusable local copy must earn a full download."""
    path = data_path(region, data_dir)
    if not path.exists():
        return False
    try:
        parse(path.read_bytes())
    except (SnapshotError, OSError):
        return False
    return True


def refresh_from_network(
    conn: sqlite3.Connection,
    config: PricingConfig,
    *,
    fetcher: Callable[[str, str | None], DownloadResult] | None = None,
    now: datetime | None = None,
    data_dir: Path | str | None = None,
    can_write: Callable[[], bool] | None = None,
) -> SnapshotStatus:
    """Conditional GET -> gate -> save to disk -> apply.

    `can_write` is re-asked AFTER the download and BEFORE any write. The caller's
    own pre-flight check cannot cover this: the GET runs on a 20 s timeout, and a
    live tracking session started inside that window would otherwise meet a
    ~1,200-row INSERT OR REPLACE against the same sqlite file its event writer is
    using (connect() is plain sqlite3 — no WAL, 5 s lock timeout). Returning
    False here abandons the apply and reports it; the download still lands on
    disk, so the next launch applies it for free.

    A 304 means the disk copy is already the published one and we do nothing.
    Never raises: offline, DNS dead, 404 because the publisher has not run yet,
    httpx missing in a stripped build — all of it lands in the returned status
    and the log, and the app carries on with whatever disk gave it."""
    if not config.snapshot_enabled:
        return SnapshotStatus("disabled", "price snapshots are turned off in settings")
    url = snapshot_url(config)
    if not url:
        return SnapshotStatus("disabled", "no snapshot URL configured")
    stamp = _now(now)
    fetch = fetcher or _default_fetcher
    # Send If-None-Match only when the file that ETag describes is actually
    # usable (bug found 2026-08-16). read_etag() used to be unconditional, so a
    # deleted or truncated price_snapshot_{region}.json beside a surviving
    # .etag made the server answer 304 forever — and this refresh was the ONLY
    # path that could have repaired it, so it permanently suppressed its own
    # repair for the life of the install. Reachable by the obvious support
    # instruction ("delete the snapshot file and relaunch"), which names the
    # .json while the .json.etag sits beside it and survives.
    etag = read_etag(config.region, data_dir) if _local_copy_parses(config.region, data_dir) else None
    try:
        download = fetch(url, etag)
    except Exception as err:
        _log(f"download failed ({url}): {err!r}")
        return SnapshotStatus("unavailable", f"could not download the snapshot: {err}")

    if download.status == 304:
        _log("304 — disk snapshot is current")
        return SnapshotStatus("not-modified", "already have the published snapshot", "network")

    try:
        snapshot = parse(download.body)
    except SnapshotError as err:
        _log(f"downloaded snapshot unusable: {err}")
        return SnapshotStatus("refused", f"downloaded snapshot rejected: {err}", "network")
    try:
        check(
            snapshot,
            region=config.region,
            baseline_count=baseline_count(conn),
            now=stamp,
        )
    except SnapshotRejected as err:
        _log(f"downloaded snapshot refused by the gate: {err}")
        return SnapshotStatus("refused", f"downloaded snapshot rejected: {err}", "network")

    # Only now is it safe to overwrite the disk copy — a snapshot that failed
    # the gate must not become next launch's disk-first answer.
    existing = data_path(config.region, data_dir)
    if existing.exists():
        try:
            current = parse(existing.read_bytes())
        except (SnapshotError, OSError):
            current = None  # unreadable local copy: replacing it is an upgrade
        if current is not None and current.generated_at >= snapshot.generated_at:
            return SnapshotStatus(
                "up-to-date",
                f"published snapshot is not newer than the local one"
                f" ({current.generated_at.isoformat()})",
                "network",
                generated_at=current.generated_at,
                age_hours=current.age_hours(stamp),
            )
    try:
        save_snapshot(download.body, download.etag, config.region, data_dir)
    except OSError as err:
        # Applying without saving is still worth it — we just re-download next
        # launch. Report it rather than aborting.
        _log(f"could not persist snapshot to disk: {err!r}")

    # Re-ask right before the DB write, not before the GET (bug found
    # 2026-08-16). Saving to disk above is safe during a session — it is a file
    # in the data dir, not the tracker DB — and persisting it means the next
    # launch applies it for free instead of re-downloading. Only the sqlite
    # write has to yield.
    if can_write is not None and not can_write():
        _log("apply skipped: tracking started during the download")
        return SnapshotStatus(
            "deferred",
            "downloaded, but tracking started before it could be applied —"
            " it lands at next launch",
            "network",
            generated_at=snapshot.generated_at,
            age_hours=snapshot.age_hours(stamp),
        )
    try:
        result = apply_snapshot(conn, snapshot, now=stamp)
    except sqlite3.Error as err:
        _log(f"apply failed after download: {err!r}")
        return SnapshotStatus("unavailable", f"could not write prices: {err}", "network")
    stale = snapshot.is_stale(config.snapshot_max_age_hours, stamp)
    detail = result.summary() + (" — STALE, per-item fetches take over" if stale else "")
    _log(f"downloaded + applied: {detail}")
    return SnapshotStatus(
        outcome="applied",
        detail=detail,
        source="network",
        result=result,
        stale=stale,
        generated_at=snapshot.generated_at,
        age_hours=result.age_hours,
    )
