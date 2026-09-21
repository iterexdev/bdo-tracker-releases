"""Per-item silver valuation (SPEC §A3.4).

Price source is data, not code (items.price_source):
  market      -> Central Market price via the community API (api.arsha.io),
                 cached in the prices table with a TTL
  vendor      -> NPC sell price from items.vendor_price (static, untaxed)
  untradeable -> 0

The HTTP fetcher is injectable for tests; the default uses httpx.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

from bdo_tracker.config import PricingConfig

ARSHA_URL = "https://api.arsha.io/v2/{region}/item?id={item_id}&lang=en"
MENA_URL = "https://tr-trade.blackdesert.pearlabyss.com/Trademarket/GetWorldMarketSubList"
_RETRIES = 4
# Concurrent fetches during prefetch. Modest on purpose: arsha is a free
# community API fronted by a WAF that already 103s under load — hammering
# it trades our speedup for more retries. Batch requests are NOT the
# answer: multi-id queries fail unless every sub-request clears the WAF,
# so under pressure a batch is less reliable than singles (measured
# 2026-07-08, re-confirming the one-id-per-request policy).
_PREFETCH_WORKERS = 6

_client = None
_client_lock = threading.Lock()


def _http_client():
    """One shared keep-alive client for all price fetches — a fresh TLS
    handshake per request was a big slice of each call's ~0.5 s. Thread-safe
    (httpx.Client supports concurrent requests; prefetch relies on it)."""
    global _client
    with _client_lock:
        if _client is None:
            import httpx

            _client = httpx.Client(timeout=15)
        return _client


class NotMarketable(Exception):
    """The API answered, and the item cannot be listed on the market."""


@dataclass
class Price:
    unit: int
    source: str  # "market" | "market (last sold)" | "market (stale cache)" | "vendor" | "untradeable" | "unavailable"
    taxed: bool  # market sales pay tax; vendor sales don't


def _mena_item(client, item_id: int, sid: int = 0) -> dict:
    """Read-only Pearl Abyss endpoint; measured 2026-09-21 while Arsha MENA
    failed. Normalize its item/level rows to the existing pricing fields.
    A malformed/error response is a failed fetch, never a cached refusal.
    """
    resp = client.post(MENA_URL, json={"keyType": 0, "mainKey": item_id},
                       headers={"User-Agent": "BlackDesert"})
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or data.get("resultCode") != 0:
        raise ValueError("MENA market returned an error")
    raw = data.get("resultMsg")
    if raw == "0":  # measured response for an item absent from this market
        return {}
    if not isinstance(raw, str) or not raw:
        raise ValueError("MENA market returned no item data")
    selected = None
    for row in raw.rstrip("|").split("|"):
        fields = row.split("-")
        if len(fields) != 10:
            raise ValueError("MENA market returned a malformed item row")
        ident, low, high, base, stock, trades, floor, ceiling, last, stamp = map(int, fields)
        if ident != item_id or low > high:
            raise ValueError("MENA market returned a different item or invalid level range")
        if low <= sid <= high:
            if selected is not None:
                raise ValueError("MENA market returned overlapping level ranges")
            selected = {"id": ident, "basePrice": base, "currentStock": stock,
                        "totalTrades": trades, "lastSoldPrice": last}
    return selected or {}


def _default_fetcher(region: str, item_id: int) -> int:
    """arsha.io quirks (verified 2026-07-03): transient per-request 500s
    with code 103 (upstream WAF) -> retry with backoff; not-marketable
    items return HTTP 200 with all price fields 0 -> NotMarketable, never
    price 0; zero-stock thin markets -> lastSoldPrice beats basePrice.
    Query one ID per request so a transient failure can't poison a batch."""
    import time

    last_err: Exception | None = None
    for attempt in range(_RETRIES):
        try:
            if region == "mena":
                data = _mena_item(_http_client(), item_id)
            else:
                resp = _http_client().get(ARSHA_URL.format(region=region, item_id=item_id))
                data = resp.json()
                if isinstance(data, dict) and data.get("code") == 103:
                    raise ConnectionError("arsha transient 103")
                resp.raise_for_status()
            if isinstance(data, list):
                data = data[0]
            base = int(data.get("basePrice", 0))
            trades = int(data.get("totalTrades", 0))
            stock = int(data.get("currentStock", 0))
            last_sold = int(data.get("lastSoldPrice", 0))
            if base == 0 and trades == 0:
                raise NotMarketable(str(item_id))
            if stock == 0 and last_sold > 0:
                return last_sold
            if base == 0:
                # Traded-but-zero answers exist (trades>0, base 0, nothing
                # sold recently). A market price of 0 must never be CACHED
                # as real — it would serve an unflagged 0 for the full TTL
                # (audit 2026-07-09); refusing routes it to the flagged
                # fallback chain instead.
                raise NotMarketable(str(item_id))
            return base
        except NotMarketable:
            raise
        except Exception as err:  # transient: retry with backoff
            last_err = err
            time.sleep(1.5 * (attempt + 1))
    raise ConnectionError(f"market API failed after {_RETRIES} tries: {last_err}")


def cached_acquisition_unit(conn: sqlite3.Connection, item_id: int) -> int | None:
    """Acquisition cost per unit from LOCAL data only — no network, for UI
    that must not block (the Ingredients loadout panel repricing on every
    keystroke): npc_buy_price -> cached market base price (stale allowed —
    it beats nothing for a planning hint; Fetch Prices refreshes it) ->
    vendor_price -> None (caller shows '?' and leaves it uncounted)."""
    row = conn.execute(
        "SELECT price_source, vendor_price, npc_buy_price FROM items WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    if row is None:
        return None
    _source, vendor_price, npc_buy = row
    if npc_buy:
        return int(npc_buy)
    cached = conn.execute(
        "SELECT base_price, not_marketable FROM prices WHERE item_id = ?", (item_id,)
    ).fetchone()
    if cached and not cached[1] and cached[0]:
        return int(cached[0])
    if vendor_price:
        return int(vendor_price)
    return None


def stale_market_count(
    conn: sqlite3.Connection, config: PricingConfig, item_ids: set[int] | None
) -> tuple[int, int]:
    """(needs_fetch, market_total) among `item_ids` (None/empty = every item,
    matching the live worker's unfiltered prefetch): how many market-priced
    items have no cache row fresher than the TTL — i.e. how many network
    calls a session start would have to make right now. Vendor/untradeable
    items never fetch and are excluded from both counts."""
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=config.cache_ttl_hours)
    ).isoformat()
    where = "WHERE i.price_source = 'market'"
    params: list = [cutoff]
    if item_ids:
        where += f" AND i.item_id IN ({','.join('?' * len(item_ids))})"
        params += sorted(item_ids)
    total, stale = conn.execute(
        "SELECT COUNT(*),"
        " COALESCE(SUM(CASE WHEN p.fetched_at IS NULL OR p.fetched_at <= ?"
        "                   THEN 1 ELSE 0 END), 0)"
        f" FROM items i LEFT JOIN prices p ON p.item_id = i.item_id {where}",
        params,
    ).fetchone()
    return int(stale), int(total)


class PriceService:
    def __init__(
        self,
        conn: sqlite3.Connection,
        config: PricingConfig,
        fetcher: Callable[[str, int], int] | None = None,
        ttl_hours: float | None = None,
    ) -> None:
        """`ttl_hours` overrides the config cache TTL — the Fetch Prices
        warm-up passes a short horizon so near-expiry rows get refreshed
        instead of going stale minutes into the session."""
        self._conn = conn
        self._config = config
        self._fetch = fetcher or _default_fetcher
        self._ttl_hours = config.cache_ttl_hours if ttl_hours is None else ttl_hours

    def price(self, item_id: int) -> Price:
        row = self._conn.execute(
            "SELECT price_source, vendor_price FROM items WHERE item_id = ?", (item_id,)
        ).fetchone()
        source, vendor_price = row if row else ("market", None)

        if source == "vendor":
            return Price(unit=int(vendor_price or 0), source="vendor", taxed=False)
        if source == "untradeable":
            return Price(unit=0, source="untradeable", taxed=False)
        return self._market_price(item_id, vendor_price)

    def acquisition_price(self, item_id: int) -> Price:
        """Cost to (re)acquire one unit — how production mode prices
        CONSUMED lines. Buying pays the full listed price, so nothing here
        is taxed: items.npc_buy_price (vendor-bought mats like Mineral
        Water) beats the market base price beats vendor_price (a sell-price
        proxy, better than nothing) beats 0 (caller flags the line)."""
        row = self._conn.execute(
            "SELECT price_source, vendor_price, npc_buy_price FROM items WHERE item_id = ?",
            (item_id,),
        ).fetchone()
        source, vendor_price, npc_buy = row if row else ("market", None, None)
        if npc_buy:
            return Price(unit=int(npc_buy), source="npc buy", taxed=False)
        if source == "market":
            market = self._market_price(item_id, vendor_price)
            return Price(unit=market.unit, source=market.source, taxed=False)
        if vendor_price:
            return Price(unit=int(vendor_price), source="vendor (sell-price proxy)", taxed=False)
        return Price(unit=0, source="unavailable", taxed=False)

    def prefetch(
        self,
        item_ids: Iterable[int],
        progress: Callable[[int, int], None] | None = None,
        max_consecutive_failures: int | None = None,
    ) -> dict[int, Price]:
        """Price many items at once. Vendor/untradeable items and fresh
        cache rows resolve locally; the rest fetch concurrently — ONLY the
        network call runs in the pool, every sqlite touch stays on this
        thread. Per-item outcomes (cache write, refusal, stale fallback)
        are exactly price()'s, so the two paths can't disagree.

        `progress(done, total)` fires per resolved item. When
        `max_consecutive_failures` transient failures land in a row
        (completion order — the API is down, stop burning retries), pending
        fetches are cancelled and the leftovers take the same fallback a
        failed fetch would."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        targets = sorted(set(item_ids))
        total = len(targets)
        out: dict[int, Price] = {}
        pending: list[tuple[int, int | None, tuple | None]] = []
        for item_id in targets:
            row = self._conn.execute(
                "SELECT price_source, vendor_price FROM items WHERE item_id = ?",
                (item_id,),
            ).fetchone()
            source, vendor_price = row if row else ("market", None)
            if source == "vendor":
                out[item_id] = Price(unit=int(vendor_price or 0), source="vendor", taxed=False)
                continue
            if source == "untradeable":
                out[item_id] = Price(unit=0, source="untradeable", taxed=False)
                continue
            cached = self._cached_row(item_id)
            fresh = self._fresh_price(cached, vendor_price)
            if fresh:
                out[item_id] = fresh
                continue
            pending.append((item_id, vendor_price, cached))
        if progress and out:
            progress(len(out), total)
        if not pending:
            return out

        consecutive = 0
        executor = ThreadPoolExecutor(max_workers=min(_PREFETCH_WORKERS, len(pending)))
        futures = {
            executor.submit(self._fetch, self._config.region, item_id): (
                item_id,
                vendor_price,
                cached,
            )
            for item_id, vendor_price, cached in pending
        }
        try:
            for future in as_completed(futures):
                item_id, vendor_price, cached = futures.pop(future)
                try:
                    fetched = future.result()
                except NotMarketable:
                    self._store_refusal(item_id)
                    out[item_id] = self._not_marketable_price(vendor_price)
                    consecutive = 0
                except Exception:
                    out[item_id] = self._fallback_price(cached, vendor_price)
                    consecutive += 1
                    if (
                        max_consecutive_failures
                        and consecutive >= max_consecutive_failures
                    ):
                        break
                else:
                    self._store(item_id, fetched)
                    out[item_id] = Price(unit=fetched, source="market", taxed=True)
                    consecutive = 0
                if progress:
                    progress(len(out), total)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        for item_id, vendor_price, cached in futures.values():  # breaker leftovers
            out[item_id] = self._fallback_price(cached, vendor_price)
        return out

    def prefetch_offline(self, item_ids: Iterable[int]) -> dict[int, Price]:
        """Every price resolvable with NO network call: vendor/untradeable
        classification, fresh cache rows, and — for the rest — exactly the
        fallback a failed fetch would take (stale cache, vendor, or an
        honest 0). Same per-item policy as prefetch(), minus the API.

        For offline analysis passes (evidence.py's bundle replay): those
        run long after the session, often while the user is back in game,
        and re-pricing at export time would make a bundle's live-vs-replay
        comparison a lie about prices rather than a fact about counts."""
        out: dict[int, Price] = {}
        for item_id in sorted(set(item_ids)):
            row = self._conn.execute(
                "SELECT price_source, vendor_price FROM items WHERE item_id = ?",
                (item_id,),
            ).fetchone()
            source, vendor_price = row if row else ("market", None)
            if source == "vendor":
                out[item_id] = Price(
                    unit=int(vendor_price or 0), source="vendor", taxed=False
                )
                continue
            if source == "untradeable":
                out[item_id] = Price(unit=0, source="untradeable", taxed=False)
                continue
            cached = self._cached_row(item_id)
            out[item_id] = self._fresh_price(cached, vendor_price) or (
                self._fallback_price(cached, vendor_price)
            )
        return out

    def _market_price(self, item_id: int, vendor_fallback: int | None) -> Price:
        cached = self._cached_row(item_id)
        fresh = self._fresh_price(cached, vendor_fallback)
        if fresh:
            return fresh
        try:
            fetched = self._fetch(self._config.region, item_id)
        except NotMarketable:
            self._store_refusal(item_id)
            return self._not_marketable_price(vendor_fallback)
        except Exception:
            return self._fallback_price(cached, vendor_fallback)
        self._store(item_id, fetched)
        return Price(unit=fetched, source="market", taxed=True)

    def _cached_row(self, item_id: int) -> tuple | None:
        return self._conn.execute(
            "SELECT base_price, fetched_at, not_marketable FROM prices WHERE item_id = ?",
            (item_id,),
        ).fetchone()

    def _fresh_price(self, cached: tuple | None, vendor_fallback: int | None) -> Price | None:
        """The cache's answer if it's within TTL, else None (fetch needed)."""
        if not cached:
            return None
        base_price, fetched_at, was_not_marketable = cached
        age_ok = datetime.now(timezone.utc) - datetime.fromisoformat(
            fetched_at
        ) < timedelta(hours=self._ttl_hours)
        if not age_ok:
            return None
        if was_not_marketable:  # cached refusal — no point refetching
            return self._not_marketable_price(vendor_fallback)
        return Price(unit=int(base_price), source="market", taxed=True)

    def _fallback_price(self, cached: tuple | None, vendor_fallback: int | None) -> Price:
        """Best local answer after a failed fetch."""
        if cached and not cached[2]:  # stale beats nothing, but say so
            return Price(unit=int(cached[0]), source="market (stale cache)", taxed=True)
        if cached:  # stale refusal — still better than serving 0 as market
            return self._not_marketable_price(vendor_fallback)
        if vendor_fallback:
            return Price(unit=int(vendor_fallback), source="vendor", taxed=False)
        return Price(unit=0, source="unavailable", taxed=False)

    def _store(self, item_id: int, base_price: int) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO prices"
                " (item_id, base_price, fetched_at, not_marketable)"
                " VALUES (?, ?, ?, 0)",
                (item_id, base_price, datetime.now(timezone.utc).isoformat()),
            )

    def _store_refusal(self, item_id: int) -> None:
        # Classified as market but the API says bound — cache the refusal
        # (else it refetches and reads as "never priced" forever) and fall
        # back; the source string surfaces the misclassification.
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO prices"
                " (item_id, base_price, fetched_at, not_marketable)"
                " VALUES (?, 0, ?, 1)",
                (item_id, datetime.now(timezone.utc).isoformat()),
            )

    @staticmethod
    def _not_marketable_price(vendor_fallback: int | None) -> Price:
        if vendor_fallback:
            return Price(unit=int(vendor_fallback), source="vendor (not marketable)", taxed=False)
        # Distinct from the intended "untradeable" classification: this is a
        # MARKET-classified item the API refused, valued 0 by failure, not
        # by design — callers flag unit-0 lines whose source isn't the
        # intended "untradeable" (audit 2026-07-09: both paths shared one
        # string, so the failure 0 rendered unflagged in saved reports).
        return Price(unit=0, source="unpriced (refused)", taxed=False)
