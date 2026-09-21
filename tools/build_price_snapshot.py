"""PRODUCE a published price snapshot (bdo_tracker/pricing/snapshot.py, schema 1).

This is the publisher side of the snapshot. One machine walks arsha once an
hour; every install downloads the resulting ~20 KB file instead of making up to
1,264 per-item calls at session start. The app side (parse / gate / apply) lives
in bdo_tracker/pricing/snapshot.py and is NOT duplicated here — this script
emits through snapshot.build() + snapshot.encode() so producer and consumer can
never drift about the format (snapshot invariant 1).

Two ways it runs:
  * hourly in the GitHub Action on the PUBLIC releases repo, with no app DB —
    item ids come from the JSON list written by tools/export_price_item_ids.py
  * locally on the dev box, to generate assets/price_snapshot_na.json.gz, the
    SEED that ships inside the installer. The seed must be the assets file and
    never rows in the shipped tracker.sqlite: tools/build_release.py runs
    `DELETE FROM prices` + VACUUM before packaging, so seeded rows are deleted
    at build time.

========================== WHY TWO PASSES (measured) ==========================
Looping market.py's per-item fetcher over 1,264 ids is ~1,264 requests against a
free community API behind a WAF. arsha also serves whole main categories in one
call, which is the reason this script exists:

    GET /v2/{region}/GetWorldMarketList?mainCategory={N}&lang=en

MEASURED 2026-08-16 (this machine, region na, all 17 main categories
1,5,10,…,80):
    17/17 categories answered on the first sweep -> 11,095 unique items
    a second sweep minutes later: 16/17, mainCategory=80 returned non-JSON
      (the known transient Imperva/WAF behaviour, retryable — market.py's
      backoff policy is reused verbatim below)
    of those 11,095: 2,067 fully decidable from the listing alone,
      9,028 carry currentStock == 0
    against this install's 1,264 market-classified items:
      971 appear in the sweep (602 decidable outright, 369 with zero stock),
      293 never appear in any category listing at all
(An earlier pass recorded 14/17 and 3,301 items; the numbers above are a fresh
re-measurement today and are what the defaults are sized against. Category
coverage clearly varies run to run, which is exactly why a failed category is
non-fatal.)

PASS 1 — breadth. 17 requests cover the market. The listing carries
name/id/currentStock/totalTrades/basePrice, which is enough to reproduce
market.py's verdict EXACTLY for any item with stock > 0.

PASS 2 — fidelity tail. The listing does NOT carry lastSoldPrice, and
market.py's _default_fetcher prefers lastSoldPrice when currentStock == 0 (thin
markets, where price discovery is hardest and a basePrice answer would be the
worst one). So every zero-stock item needs a single-item call to stay
indistinguishable from what the app would have computed itself. The whole-market
tail is 9,028 items — nobody should ask arsha for that hourly — so pass 2 is
budgeted and PRIORITISED: the target set's tail first (369 measured), then the
target ids the sweep never returned (293 measured, single-fetching them caches
the NotMarketable refusal for everybody), then optional non-target extras.
662 calls covers this install's whole target set; at 6 workers and market.py's
cited ~0.5 s per call that is roughly a minute.

Per-item fidelity is the hard requirement: same NotMarketable-on-all-zeros rule,
same lastSoldPrice-when-zero-stock rule, same refusal to ever record a market
price of 0. Anything the two passes cannot decide faithfully is NOT published as
a guess — it falls through to the merge rule below and is reported.

============================== MERGE, NOT REPLACE ==============================
The single most important rule in this file. An item fetched successfully takes
its new price and a fresh as-of stamp. An item whose fetch FAILED — dead
category, exhausted retries, budget cap — keeps its PREVIOUS price AND its
PREVIOUS as-of stamp, so its staleness stays honest and visible all the way
through to the app's TTL math. One bad run must never be able to publish holes
to every user.

A consequence worth stating, because it changes what the sanity gate is FOR:
when a baseline is supplied, published coverage can never regress — every
unfetched baseline id is carried forward, so count >= baseline count by
construction (verified end to end with --max-tail 0 against a 5,000-id baseline,
2026-08-16). snapshot.check()'s collapse arm is therefore a backstop, not the
primary defence; the arms that actually fire in practice are the absolute
50-item floor on the FIRST publish (no baseline to carry), the region check, and
the clock-skew check. The gate still runs on every publish — a producer bug that
broke the merge would have to get past it.

Usage (one command per line on purpose — the dev box is PowerShell, where a
trailing backslash is not a continuation):
  # local: regenerate the shipped seed from the app DB + the live market
  python tools/build_price_snapshot.py --db tracker.sqlite --previous assets/price_snapshot_na.json.gz

  # first ever publish (no baseline exists yet) — omit --previous deliberately
  python tools/build_price_snapshot.py --db tracker.sqlite --out snap.json.gz

  # in the Action: no DB, ids from the committed list, baseline = the asset
  # currently published under the `prices` tag
  python tools/build_price_snapshot.py --ids price_item_ids.json --out price_snapshot_na.json.gz --previous https://github.com/goldstargamingtv-droid/bdo-tracker-releases/releases/download/prices/price_snapshot_na.json.gz

  python tools/build_price_snapshot.py --db tracker.sqlite --dry-run

Exit codes: 0 published (or dry-run OK), 1 sanity gate refused the result,
2 bad inputs (unreadable baseline, no ids, nothing fetched). Anything non-zero
must fail the Action loudly rather than publish garbage.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bdo_tracker.pricing import snapshot  # noqa: E402

# Reusing market.py's private fetcher is deliberate, and the same thing
# tools/bdolytics_cooking_manifest.py already does. It is safe HERE because this
# script already hard-depends on bdo_tracker.pricing.snapshot for build()/
# encode() (invariant 1), and market.py adds nothing on top of that but stdlib
# plus bdo_tracker.config — no sqlite requirement, no Config file on disk. So
# any environment that can run this script at all can import it, the Action
# included, and there is exactly ONE implementation of "what price would the app
# have recorded for this item".
from bdo_tracker.pricing.market import (  # noqa: E402
    _PREFETCH_WORKERS,
    _RETRIES,
    NotMarketable,
    _default_fetcher,
    _http_client,
)

ROOT = Path(__file__).resolve().parent.parent

# The 17 BDO Central Market main categories. All 17 answered on the 2026-08-16
# sweep; the gaps in the numbering are the game's, not ours.
MAIN_CATEGORIES = (1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80)
LIST_URL = "https://api.arsha.io/v2/{region}/GetWorldMarketList?mainCategory={cat}&lang=en"
# The listing is one big response, not a per-item call: mainCategory=25 measured
# 117 KB and mainCategory=55 returns 7,080 items. market.py's shared client is
# built with timeout=15, which is right for singles and tight for these, so the
# listing overrides it per request.
_LIST_TIMEOUT = 60.0

# Pass-2 budget. Sized against the 2026-08-16 measurement: 369 zero-stock target
# items + 293 target items missing from the listings = 662 calls to cover this
# install's whole market set, so 1,500 leaves room for the item DB to roughly
# double before the cap starts biting. It exists to stop a pathological run (say
# a failed category dumping 895 items into the "never seen" bucket) from turning
# an hourly job into thousands of requests.
DEFAULT_MAX_TAIL = 1500
# Non-target zero-stock items — 8,659 of them measured 2026-08-16. Off by
# default: the 1,465 non-target items pass 1 can decide for free already give
# new-item coverage ahead of a release, and paying for the rest hourly is not a
# reasonable thing to ask of a free API. Raise it if broader coverage is worth
# the requests.
DEFAULT_EXTRA_TAIL = 0

SOURCE_LABEL = "arsha.io/v2/{region} GetWorldMarketList + item"


@dataclass(frozen=True)
class Observation:
    """One item priced THIS run, exactly as market.py would have priced it."""

    item_id: int
    price: int  # 0 iff not_marketable
    not_marketable: bool
    as_of: datetime  # when observed, not when published
    via: str  # "list" (pass 1) | "item" (pass 2)


# ── inputs ────────────────────────────────────────────────────────────────────


def load_previous(spec: str) -> snapshot.PriceSnapshot:
    """The baseline, from a path or an http(s) URL. Raises on failure — see
    `--previous` in main(): a baseline that was ASKED for and could not be read
    is fatal, because merging against nothing silently drops every id this run
    fails to fetch."""
    if spec.startswith(("http://", "https://")):
        import httpx

        # follow_redirects is required: GitHub answers a release-asset download
        # with a redirect to its object host (same note as snapshot.py's
        # _default_fetcher).
        resp = httpx.get(spec, timeout=snapshot._HTTP_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        body = resp.content
    else:
        body = Path(spec).read_bytes()
    return snapshot.parse(body)


def ids_from_db(db_path: Path) -> set[int]:
    """Market-classified ids from the app DB. READ-ONLY on purpose: the
    publisher has no business writing to the tracker DB, and a ro connection
    also means it never creates -wal/-shm files beside a DB another process may
    be using."""
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        return {
            int(row[0])
            for row in conn.execute(
                "SELECT item_id FROM items WHERE price_source = 'market'"
            )
        }
    finally:
        conn.close()


def ids_from_file(path: Path) -> set[int]:
    """The id list tools/export_price_item_ids.py writes (and which the Action
    consumes, having no DB). Accepts the bare JSON array too, so a hand-made
    list works without ceremony."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload.get("ids") if isinstance(payload, dict) else payload
    if not isinstance(raw, list):
        raise ValueError(f"{path}: expected a JSON list of ids, or an object with an 'ids' list")
    out: set[int] = set()
    for value in raw:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{path}: id list must hold integers, got {value!r}")
        out.add(value)
    return out


# ── pass 1: category sweep ────────────────────────────────────────────────────


def sweep_categories(
    region: str, categories: tuple[int, ...]
) -> tuple[dict[int, tuple[dict, datetime]], list[tuple[int, str]], dict[int, int]]:
    """(rows by item id, [(category, error)], {category: item count}).

    A category that cannot be fetched after retries is NOT fatal — its items
    simply fall through to their previous snapshot values in the merge, and the
    failure is reported in the summary (rule 5: never silently drop anything).
    Sequential: 17 requests is nothing next to pass 2, and walking them one at a
    time is the gentlest thing to do to a WAF that already 103s under load."""
    rows: dict[int, tuple[dict, datetime]] = {}
    failures: list[tuple[int, str]] = []
    counts: dict[int, int] = {}
    client = _http_client()
    for cat in categories:
        url = LIST_URL.format(region=region, cat=cat)
        last_err: Exception | None = None
        for attempt in range(_RETRIES):
            try:
                resp = client.get(url, timeout=_LIST_TIMEOUT)
                data = resp.json()
                # Same transient-103 handling as market.py's _default_fetcher:
                # the WAF answers HTTP 500 (or plain non-JSON) with code 103.
                if isinstance(data, dict) and data.get("code") == 103:
                    raise ConnectionError("arsha transient 103")
                resp.raise_for_status()
                if not isinstance(data, list):
                    raise ValueError(f"expected a list, got {type(data).__name__}")
            except Exception as err:  # transient: retry with market.py's backoff
                last_err = err
                if attempt < _RETRIES - 1:
                    time.sleep(1.5 * (attempt + 1))
                continue
            # One stamp per response: every item in this listing was observed
            # when the response arrived, and a sweep that takes minutes must not
            # claim its first category is as fresh as its last.
            seen_at = datetime.now(timezone.utc)
            counts[cat] = len(data)
            for row in data:
                try:
                    item_id = int(row["id"])
                except (KeyError, TypeError, ValueError):
                    continue  # malformed row; the item just falls to pass 2/merge
                rows[item_id] = (row, seen_at)
            break
        else:
            failures.append((cat, f"{type(last_err).__name__}: {last_err}"))
    return rows, failures, counts


def classify_listing(item_id: int, row: dict, seen_at: datetime) -> Observation | None:
    """market.py's _default_fetcher verdict, computed from a listing row.
    Returns None when the listing cannot decide it and a single-item call is
    required (currentStock == 0 -> lastSoldPrice, which the listing omits).

    Mirrors _default_fetcher's branch order exactly; keep the two in step."""
    try:
        base = int(row.get("basePrice", 0))
        trades = int(row.get("totalTrades", 0))
        stock = int(row.get("currentStock", 0))
    except (TypeError, ValueError):
        return None  # unreadable row: let pass 2 ask properly
    if base == 0 and trades == 0:
        # Not marketable — decided without a call, and decided BEFORE the
        # zero-stock branch, exactly as _default_fetcher does.
        return Observation(item_id, 0, True, seen_at, "list")
    if stock == 0:
        return None  # needs lastSoldPrice
    if base == 0:
        # Traded-but-zero (trades > 0, base 0). market.py refuses to cache a
        # market price of 0 (audit 2026-07-09) and so must the publisher.
        return Observation(item_id, 0, True, seen_at, "list")
    return Observation(item_id, base, False, seen_at, "list")


# ── pass 2: single-item fidelity tail ─────────────────────────────────────────


def fetch_tail(
    region: str, item_ids: list[int], workers: int
) -> tuple[dict[int, Observation], list[tuple[int, str]]]:
    """Single-item calls through market.py's own fetcher, so these entries are
    byte-for-byte the verdict the app would have reached on its own. Only the
    network call is concurrent, and the pool stays at market.py's deliberate
    WAF-safe width."""
    out: dict[int, Observation] = {}
    failures: list[tuple[int, str]] = []
    if not item_ids:
        return out, failures
    executor = ThreadPoolExecutor(max_workers=min(workers, len(item_ids)))
    futures = {executor.submit(_default_fetcher, region, i): i for i in item_ids}
    try:
        for future in as_completed(futures):
            item_id = futures[future]
            stamp = datetime.now(timezone.utc)
            try:
                price = future.result()
            except NotMarketable:
                out[item_id] = Observation(item_id, 0, True, stamp, "item")
            except Exception as err:
                # Reported, never dropped — the merge keeps this id's previous
                # price and previous as-of stamp.
                failures.append((item_id, f"{type(err).__name__}: {err}"))
            else:
                out[item_id] = Observation(item_id, int(price), False, stamp, "item")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return out, failures


# ── merge ─────────────────────────────────────────────────────────────────────


@dataclass
class MergeReport:
    fetched: int = 0  # priced this run
    refused: int = 0  # NotMarketable this run (price 0 + flag, a real answer)
    kept_stale: int = 0  # fetch failed -> previous price AND previous as-of
    no_data: int = 0  # never priced, by us or by the baseline — omitted
    kept_ids: list[int] = field(default_factory=list)
    missing_ids: list[int] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Entries that will be published. no_data is deliberately excluded —
        it is the fifth bucket, counted and listed but not written."""
        return self.fetched + self.refused + self.kept_stale


def merge(
    candidates: set[int],
    observed: dict[int, Observation],
    previous: snapshot.PriceSnapshot | None,
) -> tuple[list[snapshot.SnapshotEntry], MergeReport]:
    """Fresh observation wins; otherwise the previous entry is carried over
    UNCHANGED, price and as-of together. Carrying the old as-of is the whole
    point: the app's TTL math then still sees that item as N hours old and gives
    it its own arsha call, instead of a failed publish laundering stale prices
    into fresh-looking ones."""
    prior = {e.item_id: e for e in previous.entries} if previous else {}
    report = MergeReport()
    entries: list[snapshot.SnapshotEntry] = []
    for item_id in sorted(candidates):
        obs = observed.get(item_id)
        if obs is not None:
            entries.append(
                snapshot.SnapshotEntry(
                    item_id=item_id,
                    price=obs.price,
                    as_of=obs.as_of,
                    not_marketable=obs.not_marketable,
                )
            )
            if obs.not_marketable:
                report.refused += 1
            else:
                report.fetched += 1
            continue
        carried = prior.get(item_id)
        if carried is not None:
            entries.append(carried)  # price AND as_of, untouched
            report.kept_stale += 1
            report.kept_ids.append(item_id)
            continue
        report.no_data += 1
        report.missing_ids.append(item_id)
    return entries, report


# ── output ────────────────────────────────────────────────────────────────────


def write_atomic(path: Path, body: bytes) -> None:
    """temp + os.replace, same reasoning as Config.save and
    snapshot.save_snapshot: a crash mid-write must not leave a truncated file
    that the next run reads as its baseline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(body)
    os.replace(tmp, path)


def _preview(ids: list[int], limit: int = 12) -> str:
    head = ", ".join(str(i) for i in ids[:limit])
    return head + (f", … (+{len(ids) - limit} more)" if len(ids) > limit else "")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--region", default="na", help="arsha region (default: na)")
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output file; .gz suffix selects gzip (default: assets/price_snapshot_{region}.json.gz)",
    )
    parser.add_argument(
        "--previous",
        default=None,
        metavar="PATH_OR_URL",
        help="baseline snapshot to merge against. OMIT only for a first publish: "
        "if given and unreadable this exits non-zero rather than publish a "
        "snapshot with no baseline to fall back on.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("tracker.sqlite"),
        help="app DB to read market item ids from (read-only). A missing file is "
        "reported and skipped — the Action has no DB and uses --ids.",
    )
    parser.add_argument(
        "--ids",
        type=Path,
        default=None,
        help="JSON id list from tools/export_price_item_ids.py",
    )
    parser.add_argument(
        "--max-tail",
        type=int,
        default=DEFAULT_MAX_TAIL,
        help=f"cap on pass-2 single-item calls for TARGET ids (default {DEFAULT_MAX_TAIL}; "
        "662 measured as the full requirement on 2026-08-16)",
    )
    parser.add_argument(
        "--extra-tail",
        type=int,
        default=DEFAULT_EXTRA_TAIL,
        help=f"additional pass-2 calls for NON-target zero-stock items "
        f"(default {DEFAULT_EXTRA_TAIL}; 8,659 such items measured 2026-08-16)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=_PREFETCH_WORKERS,
        help=f"pass-2 concurrency (default {_PREFETCH_WORKERS}, market.py's WAF-safe width)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="do everything except write the file"
    )
    args = parser.parse_args()

    region = args.region.strip().lower()
    out = args.out or (ROOT / "assets" / f"price_snapshot_{region}.json.gz")

    # ── baseline ──
    previous: snapshot.PriceSnapshot | None = None
    if args.previous:
        try:
            previous = load_previous(args.previous)
        except Exception as err:
            print(
                f"FATAL: could not read the baseline snapshot {args.previous!r}: "
                f"{type(err).__name__}: {err}\n"
                "       Publishing without the baseline would drop every id this "
                "run fails to fetch. Omit --previous only for a first publish.",
                file=sys.stderr,
            )
            return 2
        if previous.region != region:
            print(
                f"FATAL: baseline is region {previous.region!r}, publishing {region!r}",
                file=sys.stderr,
            )
            return 2
        print(
            f"baseline  {args.previous}\n"
            f"          {previous.count} ids, generated {previous.generated_at.isoformat()}"
            f" ({previous.age_hours():.1f} h ago)"
        )
    else:
        print("baseline  NONE — first publish; nothing can be carried over on failure")

    # ── target ids: union of every source available, so an id can never
    # silently fall out of coverage between releases ──
    target: set[int] = set()
    from_db = from_file = from_prev = 0
    if args.db and args.db.exists():
        try:
            db_ids = ids_from_db(args.db)
        except sqlite3.Error as err:
            print(f"FATAL: could not read {args.db}: {err}", file=sys.stderr)
            return 2
        from_db = len(db_ids)
        target |= db_ids
    elif args.db:
        print(f"          (no DB at {args.db} — id list comes from --ids/baseline)")
    if args.ids:
        try:
            file_ids = ids_from_file(args.ids)
        except (OSError, ValueError, json.JSONDecodeError) as err:
            print(f"FATAL: could not read {args.ids}: {err}", file=sys.stderr)
            return 2
        from_file = len(file_ids)
        target |= file_ids
    if previous is not None:
        prev_ids = {e.item_id for e in previous.entries}
        from_prev = len(prev_ids)
        target |= prev_ids
    if not target:
        print(
            "FATAL: no item ids to price (need --db, --ids, or --previous)",
            file=sys.stderr,
        )
        return 2
    print(
        f"target    {len(target)} ids"
        f"  (db {from_db} | id-file {from_file} | baseline {from_prev})"
    )

    # ── pass 1 ──
    # MENA's direct category response uses a different wire format. Walk the
    # known item ids through the same per-item reader the app uses instead.
    categories = () if region == "mena" else MAIN_CATEGORIES
    rows, cat_failures, counts = sweep_categories(region, categories)
    observed: dict[int, Observation] = {}
    tail: list[int] = []
    for item_id, (row, seen_at) in rows.items():
        obs = classify_listing(item_id, row, seen_at)
        if obs is None:
            tail.append(item_id)
        else:
            observed[item_id] = obs
    ok_cats = len(categories) - len(cat_failures)
    print(
        f"pass 1    {ok_cats}/{len(categories)} categories, {len(rows)} items seen"
        f" -> {len(observed)} decided, {len(tail)} zero-stock"
    )
    for cat, err in cat_failures:
        print(f"          ! mainCategory={cat} unavailable after {_RETRIES} tries: {err}")
    if cat_failures:
        print(
            "          ! those items fall back to their baseline values in the merge"
        )

    # ── pass 2, priority-ordered under the budget ──
    swept = set(rows)
    tail_set = set(tail)
    target_tail = sorted(target & tail_set)
    target_unseen = sorted(target - swept)  # never listed: often a bound item
    extra_tail = sorted(tail_set - target)
    wanted = target_tail + target_unseen
    # Clamp to >= 0 like --extra-tail beside it. A negative --max-tail used to
    # reach the slice as-is, and `wanted[:-1]` trims from the END rather than
    # meaning "none": with 10 wanted ids, --max-tail -1 planned NINE calls
    # while the warning below reported 11 unfetched (measured 2026-08-16).
    # Fail-safe either way — a dropped id keeps its baseline price and as-of —
    # but the log line was simply lying.
    max_tail = max(0, args.max_tail)
    dropped_target = max(0, len(wanted) - max_tail)
    plan = wanted[:max_tail]
    plan += extra_tail[: max(0, args.extra_tail)]
    print(
        f"pass 2    {len(plan)} single-item calls at {args.workers} workers"
        f"  (target tail {len(target_tail)}, target unlisted {len(target_unseen)},"
        f" extras {min(len(extra_tail), max(0, args.extra_tail))}/{len(extra_tail)})"
    )
    if dropped_target:
        print(
            f"          ! --max-tail {args.max_tail} left {dropped_target} target id(s)"
            " unfetched; they keep their baseline price and as-of"
        )
    tail_obs, tail_failures = fetch_tail(region, plan, args.workers)
    observed.update(tail_obs)
    if tail_failures:
        print(f"          ! {len(tail_failures)} single-item fetch(es) failed:")
        for item_id, err in tail_failures[:5]:
            print(f"            {item_id}: {err}")
        if len(tail_failures) > 5:
            print(f"            … (+{len(tail_failures) - 5} more)")

    # ── merge ──
    candidates = target | set(observed)
    entries, report = merge(candidates, observed, previous)
    print(
        f"merge     fetched {report.fetched} | refused {report.refused}"
        f" | kept-stale {report.kept_stale} | total {report.total}"
        f"  (+{report.no_data} with no data anywhere, omitted)"
    )
    if report.kept_ids:
        print(f"          kept-stale ids: {_preview(report.kept_ids)}")
    if report.missing_ids:
        print(f"          no-data ids:    {_preview(report.missing_ids)}")

    if not observed:
        # Not a coverage question — the pipeline is down. Republishing the
        # baseline verbatim would be honest (kept as-ofs keep ageing) but it
        # would also hide a total outage behind a green Action.
        print(
            "FATAL: not a single item was fetched this run — arsha is unreachable",
            file=sys.stderr,
        )
        return 2

    if region == "mena" and previous is None and report.no_data:
        print("FATAL: first MENA snapshot must cover every target id", file=sys.stderr)
        return 2

    # ── build + gate ──
    generated_at = datetime.now(timezone.utc)
    snap = snapshot.build(
        region,
        entries,
        generated_at=generated_at,
        source=("Pearl Abyss MENA /Trademarket/GetWorldMarketSubList"
                if region == "mena" else SOURCE_LABEL.format(region=region)),
    )
    try:
        snapshot.check(
            snap,
            region=region,
            baseline_count=previous.count if previous else 0,
            now=generated_at,
        )
    except snapshot.SnapshotRejected as err:
        print(f"FATAL: sanity gate refused this snapshot: {err}", file=sys.stderr)
        return 1

    body = snapshot.encode(snap, compress=out.suffix == ".gz")
    # Publish only what the consumer can read: parse our own bytes back through
    # the exact code every install will run before letting them out the door.
    try:
        snapshot.parse(body)
    except snapshot.SnapshotError as err:
        print(f"FATAL: encoded snapshot does not parse: {err}", file=sys.stderr)
        return 1

    print(
        f"gate      ok — {snap.count} entries"
        + (f", baseline was {previous.count}" if previous else "")
    )
    if args.dry_run:
        print(f"dry-run   would write {out} ({len(body)} bytes)")
        return 0
    write_atomic(out, body)
    print(f"wrote     {out}  ({len(body)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
