#!/usr/bin/env python3
"""
Kalshi Signal Collector

Standalone, non-trading data-collection bot.

Purpose
- Scan a broad Kalshi market universe (all open markets available from GET /markets)
- Apply hard safety filters only
- Log all candidate signals, not just executed trades
- Label those signals after a fixed forward window
- Produce a SQLite dataset suitable for ML training

This bot NEVER places orders.

Dependencies:
    pip install requests websockets cryptography

Environment:
    KALSHI_API_KEY_ID=...
    KALSHI_PRIVATE_KEY_PATH=/full/path/to/private_key.pem

Usage:
    python kalshi_signal_collector.py --config config_signal_collector.json
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import sqlite3
import ssl
import statistics
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ==============================
# Config
# ==============================

@dataclass
class CollectorConfig:
    rest_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    timeout_seconds: int = 20

    # Universe
    include_all_open_markets: bool = True
    markets_page_limit: int = 1000
    refresh_universe_every_seconds: int = 300

    # Hard filters only
    required_min_yes_price_cents: int = 85
    required_max_yes_price_cents: int = 97
    max_spread_cents: int = 4
    min_volume_contracts: float = 25.0
    min_open_interest_contracts: float = 10.0
    min_recent_history_points: int = 10
    min_hours_to_close: float = 0.10
    max_hours_to_close: float = 336.0  # 14 days

    # Candidate logging cadence
    per_market_signal_cooldown_seconds: int = 120
    label_horizon_seconds: int = 600
    log_all_filtered_candidates: bool = True

    # Outcome thresholds for hypothetical label
    target_return_pct: float = 0.02
    stop_return_pct: float = 0.03

    # Runtime
    db_path: str = "signal_collector.db"
    state_json_path: str = "signal_collector_state.json"
    runtime_log_path: str = "signal_collector_runtime.log"
    snapshot_every_seconds: int = 10
    summary_every_seconds: int = 60


# ==============================
# Helpers
# ==============================


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def now_ts() -> int:
    return int(time.time())


def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def normalize_score(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return clamp((x - lo) / (hi - lo), 0.0, 1.0)


def cents_from_dollars_str(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(round(float(v) * 100))
    except Exception:
        return None


def fp_to_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def iso_to_ts(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str:
        return None
    try:
        return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def json_dumps(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def log_line(cfg: CollectorConfig, msg: str) -> None:
    line = f"[{utc_now_iso()}] {msg}"
    print(line, flush=True)
    with open(cfg.runtime_log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ==============================
# DB
# ==============================


class CollectorDB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT UNIQUE,
                created_ts INTEGER NOT NULL,
                market_ticker TEXT NOT NULL,
                event_ticker TEXT,
                category TEXT,
                title TEXT,
                status TEXT DEFAULT 'pending',
                signal_type TEXT NOT NULL,
                hypothetical_side TEXT NOT NULL,
                hypothetical_entry_price_cents INTEGER NOT NULL,
                hypothetical_target_price_cents INTEGER NOT NULL,
                hypothetical_stop_price_cents INTEGER NOT NULL,
                close_ts INTEGER,
                hours_to_close REAL,

                yes_bid_cents INTEGER,
                yes_ask_cents INTEGER,
                no_bid_cents INTEGER,
                no_ask_cents INTEGER,
                spread_cents INTEGER,
                yes_bid_size REAL,
                yes_ask_size REAL,
                volume REAL,
                open_interest REAL,

                momentum_10 REAL,
                momentum_30 REAL,
                momentum_60 REAL,
                reversion_z_60 REAL,
                trade_pressure REAL,
                orderbook_pressure REAL,
                micro_imbalance REAL,
                volatility_60 REAL,
                rank_score REAL,
                reason_codes_json TEXT,
                metadata_json TEXT,

                label_ts INTEGER,
                label_result TEXT,
                label_hit_target_first INTEGER,
                label_hit_stop_first INTEGER,
                label_timed_out_profit INTEGER,
                label_timed_out_loss INTEGER,
                label_timed_out_flat INTEGER,
                label_max_favorable_move_cents INTEGER,
                label_max_adverse_move_cents INTEGER,
                label_final_move_cents INTEGER,
                label_final_return_pct REAL,
                label_quality INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signal_status_created
            ON signal_events(status, created_ts)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                market_ticker TEXT NOT NULL,
                yes_bid_cents INTEGER,
                yes_ask_cents INTEGER,
                no_bid_cents INTEGER,
                no_ask_cents INTEGER,
                spread_cents INTEGER,
                yes_bid_size REAL,
                yes_ask_size REAL,
                volume REAL,
                open_interest REAL,
                last_price_cents INTEGER,
                micro_imbalance REAL,
                trade_pressure REAL,
                orderbook_pressure REAL,
                momentum_10 REAL,
                momentum_30 REAL,
                momentum_60 REAL,
                reversion_z_60 REAL,
                volatility_60 REAL
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_snap_market_ts
            ON market_snapshots(market_ticker, ts)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS collector_stats (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def upsert_stat(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO collector_stats(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def insert_snapshot(self, row: Dict[str, Any]) -> None:
        keys = list(row.keys())
        vals = [row[k] for k in keys]
        placeholders = ",".join(["?"] * len(keys))
        sql = f"INSERT INTO market_snapshots({','.join(keys)}) VALUES({placeholders})"
        self.conn.execute(sql, vals)
        self.conn.commit()

    def insert_signal(self, row: Dict[str, Any]) -> None:
        keys = list(row.keys())
        vals = [row[k] for k in keys]
        placeholders = ",".join(["?"] * len(keys))
        sql = f"INSERT OR IGNORE INTO signal_events({','.join(keys)}) VALUES({placeholders})"
        self.conn.execute(sql, vals)
        self.conn.commit()

    def get_pending_signals_ready_for_label(self, cutoff_ts: int, limit: int = 1000) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        return cur.execute(
            """
            SELECT * FROM signal_events
            WHERE status = 'pending' AND created_ts <= ?
            ORDER BY created_ts ASC
            LIMIT ?
            """,
            (cutoff_ts, limit),
        ).fetchall()

    def get_snapshots_for_window(self, ticker: str, start_ts: int, end_ts: int) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        return cur.execute(
            """
            SELECT * FROM market_snapshots
            WHERE market_ticker = ? AND ts BETWEEN ? AND ?
            ORDER BY ts ASC
            """,
            (ticker, start_ts, end_ts),
        ).fetchall()

    def label_signal(self, signal_id: str, updates: Dict[str, Any]) -> None:
        keys = list(updates.keys())
        assigns = ", ".join([f"{k} = ?" for k in keys])
        vals = [updates[k] for k in keys] + [signal_id]
        self.conn.execute(f"UPDATE signal_events SET {assigns} WHERE signal_id = ?", vals)
        self.conn.commit()

    def summary_counts(self) -> Dict[str, int]:
        cur = self.conn.cursor()
        rows = cur.execute(
            "SELECT status, COUNT(*) AS n FROM signal_events GROUP BY status"
        ).fetchall()
        out = {"pending": 0, "labeled": 0}
        for r in rows:
            out[r["status"]] = r["n"]
        return out


# ==============================
# Kalshi auth / REST
# ==============================


class KalshiAuth:
    def __init__(self, api_key_id: str, private_key_path: str):
        self.api_key_id = api_key_id
        with open(private_key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None)

    def sign(self, timestamp_ms: str, method: str, path: str) -> str:
        msg = f"{timestamp_ms}{method.upper()}{path}".encode("utf-8")
        sig = self.private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def rest_headers(self, method: str, path: str) -> Dict[str, str]:
        ts_ms = str(int(time.time() * 1000))
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": self.sign(ts_ms, method, path),
        }

    def ws_headers(self, path: str = "/trade-api/ws/v2") -> Dict[str, str]:
        ts_ms = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": self.sign(ts_ms, "GET", path),
        }


class KalshiREST:
    def __init__(self, auth: KalshiAuth, base_url: str, timeout_seconds: int):
        self.auth = auth
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, auth_required: bool = False) -> Any:
        headers = self.auth.rest_headers(method, path) if auth_required else {"Accept": "application/json"}
        resp = self.session.request(
            method=method.upper(),
            url=f"{self.base_url}{path}",
            params=params,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()

    def get_markets_page(self, status: str = "open", limit: int = 1000, cursor: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/markets", params=params, auth_required=False)

    def get_all_open_markets(self, limit: int = 100000) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while len(out) < limit:
            batch = self.get_markets_page(status="open", limit=min(1000, limit - len(out)), cursor=cursor)
            markets = batch.get("markets", [])
            if not markets:
                break
            out.extend(markets)
            cursor = batch.get("cursor")
            if not cursor:
                break
        return out


# ==============================
# Realtime state
# ==============================


@dataclass
class MarketRealtimeState:
    ticker: str
    event_ticker: str = ""
    title: str = ""
    category: str = ""
    close_ts: Optional[int] = None

    yes_bid_cents: Optional[int] = None
    yes_ask_cents: Optional[int] = None
    no_bid_cents: Optional[int] = None
    no_ask_cents: Optional[int] = None
    last_price_cents: Optional[int] = None

    yes_bid_size: float = 0.0
    yes_ask_size: float = 0.0
    volume: float = 0.0
    open_interest: float = 0.0

    last_update_ts: int = 0
    last_logged_signal_ts: int = 0

    mid_history: deque = field(default_factory=lambda: deque(maxlen=1200))
    trade_prices: deque = field(default_factory=lambda: deque(maxlen=1200))
    trade_sizes: deque = field(default_factory=lambda: deque(maxlen=1200))
    ob_pressure_history: deque = field(default_factory=lambda: deque(maxlen=1200))

    def update_from_market_meta(self, market: Dict[str, Any]) -> None:
        self.event_ticker = market.get("event_ticker", self.event_ticker)
        self.title = market.get("title", self.title)
        self.category = market.get("category", self.category)
        self.close_ts = iso_to_ts(market.get("close_time")) or self.close_ts

        yb = market.get("yes_bid")
        ya = market.get("yes_ask")
        nb = market.get("no_bid")
        na = market.get("no_ask")
        lp = market.get("last_price")
        self.yes_bid_cents = safe_int(yb, self.yes_bid_cents or 0) if yb is not None else self.yes_bid_cents
        self.yes_ask_cents = safe_int(ya, self.yes_ask_cents or 0) if ya is not None else self.yes_ask_cents
        self.no_bid_cents = safe_int(nb, self.no_bid_cents or 0) if nb is not None else self.no_bid_cents
        self.no_ask_cents = safe_int(na, self.no_ask_cents or 0) if na is not None else self.no_ask_cents
        self.last_price_cents = safe_int(lp, self.last_price_cents or 0) if lp is not None else self.last_price_cents

        self.volume = max(self.volume, fp_to_float(market.get("volume")) or fp_to_float(market.get("volume_fp")))
        self.open_interest = max(self.open_interest, fp_to_float(market.get("open_interest")) or fp_to_float(market.get("open_interest_fp")))

        now = now_ts()
        self.last_update_ts = now
        mid = self.mid_price()
        if mid is not None:
            self.mid_history.append((now, mid))

    def update_from_ticker(self, msg: Dict[str, Any]) -> None:
        self.yes_bid_cents = cents_from_dollars_str(msg.get("yes_bid_dollars")) or self.yes_bid_cents
        self.yes_ask_cents = cents_from_dollars_str(msg.get("yes_ask_dollars")) or self.yes_ask_cents
        self.last_price_cents = cents_from_dollars_str(msg.get("price_dollars")) or self.last_price_cents
        self.volume = max(self.volume, fp_to_float(msg.get("volume_fp")))
        self.open_interest = max(self.open_interest, fp_to_float(msg.get("open_interest_fp")))
        self.yes_bid_size = fp_to_float(msg.get("yes_bid_size_fp") or msg.get("bid_size_fp"))
        self.yes_ask_size = fp_to_float(msg.get("yes_ask_size_fp") or msg.get("ask_size_fp"))
        if self.yes_bid_cents is not None:
            self.no_ask_cents = 100 - self.yes_bid_cents
        if self.yes_ask_cents is not None:
            self.no_bid_cents = 100 - self.yes_ask_cents
        now = now_ts()
        self.last_update_ts = now
        mid = self.mid_price()
        if mid is not None:
            self.mid_history.append((now, mid))

    def update_from_trade(self, msg: Dict[str, Any]) -> None:
        px = cents_from_dollars_str(msg.get("yes_price_dollars"))
        size = fp_to_float(msg.get("count_fp"))
        now = now_ts()
        if px is not None:
            self.trade_prices.append((now, px))
            self.trade_sizes.append((now, size))
            self.last_price_cents = px
            self.mid_history.append((now, float(px)))
        self.last_update_ts = now

    def update_from_orderbook_delta(self, msg: Dict[str, Any]) -> None:
        side = (msg.get("side") or "").lower()
        delta = fp_to_float(msg.get("delta_fp") or msg.get("count_delta") or 0.0)
        now = now_ts()
        pressure = delta if side == "yes" else (-delta if side == "no" else 0.0)
        self.ob_pressure_history.append((now, pressure))
        self.last_update_ts = now

    def spread_cents(self) -> Optional[int]:
        if self.yes_bid_cents is None or self.yes_ask_cents is None:
            return None
        return self.yes_ask_cents - self.yes_bid_cents

    def mid_price(self) -> Optional[float]:
        if self.yes_bid_cents is not None and self.yes_ask_cents is not None:
            return (self.yes_bid_cents + self.yes_ask_cents) / 2.0
        if self.last_price_cents is not None:
            return float(self.last_price_cents)
        return None

    def hours_to_close(self) -> Optional[float]:
        if self.close_ts is None:
            return None
        return max(0.0, (self.close_ts - now_ts()) / 3600.0)

    def _window_values(self, series: deque, lookback_seconds: int) -> List[float]:
        cutoff = now_ts() - lookback_seconds
        return [float(v) for ts, v in series if ts >= cutoff]

    def momentum(self, lookback_seconds: int) -> float:
        vals = self._window_values(self.mid_history, lookback_seconds)
        if len(vals) < 2:
            return 0.0
        return round(vals[-1] - vals[0], 4)

    def reversion_z(self, lookback_seconds: int) -> float:
        vals = self._window_values(self.mid_history, lookback_seconds)
        if len(vals) < 5:
            return 0.0
        mean_v = statistics.mean(vals)
        std_v = statistics.pstdev(vals)
        if std_v <= 0:
            return 0.0
        return round((vals[-1] - mean_v) / std_v, 4)

    def trade_pressure(self, lookback_seconds: int = 60) -> float:
        cutoff = now_ts() - lookback_seconds
        trades = [(ts, px, sz) for (ts, px), (_, sz) in zip(self.trade_prices, self.trade_sizes) if ts >= cutoff]
        if len(trades) < 2:
            return 0.0
        total_size = sum(sz for _, _, sz in trades)
        if total_size <= 0:
            return 0.0
        vwap = sum(px * sz for _, px, sz in trades) / total_size
        mid = self.mid_price()
        if mid is None:
            return 0.0
        return round(vwap - mid, 4)

    def orderbook_pressure(self, lookback_seconds: int = 60) -> float:
        cutoff = now_ts() - lookback_seconds
        vals = [v for ts, v in self.ob_pressure_history if ts >= cutoff]
        if not vals:
            return 0.0
        return round(sum(vals), 4)

    def micro_imbalance(self) -> float:
        denom = self.yes_bid_size + self.yes_ask_size
        if denom <= 0:
            return 0.0
        return round((self.yes_bid_size - self.yes_ask_size) / denom, 6)

    def volatility(self, lookback_seconds: int = 60) -> float:
        vals = self._window_values(self.mid_history, lookback_seconds)
        if len(vals) < 5:
            return 0.0
        return round(statistics.pstdev(vals), 6)


# ==============================
# Universe / ranking / labels
# ==============================


def compute_rank_score(state: MarketRealtimeState, cfg: CollectorConfig) -> float:
    spread = state.spread_cents()
    if spread is None:
        return -999.0
    liq = normalize_score(state.volume, cfg.min_volume_contracts, 25000.0)
    oi = normalize_score(state.open_interest, cfg.min_open_interest_contracts, 10000.0)
    spread_score = 1.0 - normalize_score(spread, 0.0, float(cfg.max_spread_cents))
    momentum_score = normalize_score(state.momentum(60), -3.0, 3.0)
    rev_score = normalize_score(-state.reversion_z(60), -3.0, 3.0)
    micro = normalize_score(state.micro_imbalance(), -1.0, 1.0)
    vol_penalty = 1.0 - normalize_score(state.volatility(60), 0.0, 3.5)
    htc = state.hours_to_close()
    time_score = 0.5
    if htc is not None:
        if 0.25 <= htc <= 24:
            time_score = 1.0
        elif 24 < htc <= cfg.max_hours_to_close:
            time_score = 0.6
        else:
            time_score = 0.25
    score = (
        0.22 * liq +
        0.15 * oi +
        0.18 * spread_score +
        0.15 * momentum_score +
        0.10 * rev_score +
        0.10 * micro +
        0.05 * vol_penalty +
        0.05 * time_score
    )
    return round(score, 6)


def build_reason_codes(state: MarketRealtimeState, cfg: CollectorConfig) -> List[str]:
    codes: List[str] = []
    spread = state.spread_cents()
    if spread is not None and spread >= max(2, cfg.max_spread_cents - 1):
        codes.append("spread_elevated")
    if state.volume < cfg.min_volume_contracts * 2:
        codes.append("thin_volume")
    if state.open_interest < cfg.min_open_interest_contracts * 2:
        codes.append("thin_open_interest")
    if state.momentum(60) >= 2.0:
        codes.append("upward_momentum")
    if state.momentum(60) <= -2.0:
        codes.append("downward_momentum")
    if state.reversion_z(60) <= -1.5:
        codes.append("oversold_reversion")
    if state.reversion_z(60) >= 1.5:
        codes.append("overbought")
    if state.micro_imbalance() >= 0.25:
        codes.append("bid_imbalance")
    if state.micro_imbalance() <= -0.25:
        codes.append("ask_imbalance")
    if state.volatility(60) >= 1.5:
        codes.append("high_short_term_volatility")
    htc = state.hours_to_close()
    if htc is not None and htc <= 1.0:
        codes.append("near_close")
    return codes


def hard_filter_passes(state: MarketRealtimeState, cfg: CollectorConfig) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ask = state.yes_ask_cents
    bid = state.yes_bid_cents
    spread = state.spread_cents()
    htc = state.hours_to_close()

    if ask is None:
        reasons.append("missing_yes_ask")
    if bid is None:
        reasons.append("missing_yes_bid")
    if ask is not None and ask < cfg.required_min_yes_price_cents:
        reasons.append("below_min_yes_price")
    if ask is not None and ask > cfg.required_max_yes_price_cents:
        reasons.append("above_max_yes_price")
    if spread is None or spread > cfg.max_spread_cents:
        reasons.append("spread_too_wide")
    if state.volume < cfg.min_volume_contracts:
        reasons.append("volume_too_low")
    if state.open_interest < cfg.min_open_interest_contracts:
        reasons.append("open_interest_too_low")
    if len(state.mid_history) < cfg.min_recent_history_points:
        reasons.append("insufficient_history")
    if htc is None or htc < cfg.min_hours_to_close:
        reasons.append("too_close_to_close")
    if htc is not None and htc > cfg.max_hours_to_close:
        reasons.append("too_far_from_close")
    return (len(reasons) == 0, reasons)


def choose_signal_type(state: MarketRealtimeState) -> str:
    mom = state.momentum(60)
    rev = state.reversion_z(60)
    if mom >= 1.0 and state.trade_pressure(60) >= 0:
        return "momentum_yes"
    if rev <= -1.0:
        return "reversion_yes"
    return "filtered_candidate_yes"


def compute_label_from_snapshots(signal_row: sqlite3.Row, snaps: List[sqlite3.Row]) -> Dict[str, Any]:
    entry = safe_int(signal_row["hypothetical_entry_price_cents"])
    target = safe_int(signal_row["hypothetical_target_price_cents"])
    stop = safe_int(signal_row["hypothetical_stop_price_cents"])

    hit_target_first = 0
    hit_stop_first = 0
    label_result = "timed_out_flat"

    max_favorable = -10**9
    max_adverse = 10**9
    final_move = 0

    for s in snaps:
        best_bid = s["yes_bid_cents"] if s["yes_bid_cents"] is not None else s["last_price_cents"]
        if best_bid is None:
            continue
        move = safe_int(best_bid) - entry
        max_favorable = max(max_favorable, move)
        max_adverse = min(max_adverse, move)

        if best_bid >= target:
            hit_target_first = 1
            label_result = "hit_target_first"
            final_move = move
            break
        if best_bid <= stop:
            hit_stop_first = 1
            label_result = "hit_stop_first"
            final_move = move
            break
        final_move = move

    if hit_target_first == 0 and hit_stop_first == 0:
        if final_move > 0:
            label_result = "timed_out_profit"
        elif final_move < 0:
            label_result = "timed_out_loss"
        else:
            label_result = "timed_out_flat"

    final_return_pct = (final_move / entry) if entry > 0 else 0.0
    quality = 1 if label_result == "hit_target_first" else 0

    if max_favorable == -10**9:
        max_favorable = 0
    if max_adverse == 10**9:
        max_adverse = 0

    return {
        "status": "labeled",
        "label_ts": now_ts(),
        "label_result": label_result,
        "label_hit_target_first": hit_target_first,
        "label_hit_stop_first": hit_stop_first,
        "label_timed_out_profit": 1 if label_result == "timed_out_profit" else 0,
        "label_timed_out_loss": 1 if label_result == "timed_out_loss" else 0,
        "label_timed_out_flat": 1 if label_result == "timed_out_flat" else 0,
        "label_max_favorable_move_cents": max_favorable,
        "label_max_adverse_move_cents": max_adverse,
        "label_final_move_cents": final_move,
        "label_final_return_pct": round(final_return_pct, 6),
        "label_quality": quality,
    }


# ==============================
# Collector app
# ==============================


import tempfile

class SignalCollectorApp:
    def __init__(self, cfg: CollectorConfig):
        self.cfg = cfg
        api_key = os.environ.get("KALSHI_API_KEY_ID", "").strip()
        key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "").strip()
        key_pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM", "").strip()

        if not api_key:
            raise SystemExit("Missing KALSHI_API_KEY_ID")

        if key_pem:
            tmp_dir = "/app/data" if os.path.exists("/app/data") else tempfile.gettempdir()
            temp_key_path = os.path.join(tmp_dir, "kalshi_private_key.pem")
            with open(temp_key_path, "w", encoding="utf-8") as f:
                f.write(key_pem.replace("\\n", "\n"))
            key_path = temp_key_path

        if not key_path or not os.path.exists(key_path):
            raise SystemExit(
                "Missing or invalid Kalshi private key. "
                "Use KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH."
            )

        self.auth = KalshiAuth(api_key, key_path)
        self.rest = KalshiREST(self.auth, cfg.rest_base_url, cfg.timeout_seconds)
        self.db = CollectorDB(cfg.db_path)

        self.markets_meta: Dict[str, Dict[str, Any]] = {}
        self.states: Dict[str, MarketRealtimeState] = {}
        self.ws = None
        self.last_universe_refresh = 0
        self.last_snapshot_flush = 0
        self.last_summary = 0
        self.run_started_ts = now_ts()
        
    def refresh_universe(self) -> None:
        markets = self.rest.get_all_open_markets(limit=3000)
        new_meta: Dict[str, Dict[str, Any]] = {}
        for m in markets:
            ticker = m.get("ticker")
            if not ticker:
                continue
            new_meta[ticker] = m
            if ticker not in self.states:
                self.states[ticker] = MarketRealtimeState(ticker=ticker)
            self.states[ticker].update_from_market_meta(m)
        self.markets_meta = new_meta
        self.last_universe_refresh = now_ts()
        self.db.upsert_stat("last_universe_market_count", str(len(self.markets_meta)))
        log_line(self.cfg, f"Universe refresh complete. Open markets tracked={len(self.markets_meta)}")

    async def connect_ws(self) -> None:
        headers = self.auth.ws_headers("/trade-api/ws/v2")
        self.ws = await websockets.connect(
            self.cfg.ws_url,
            additional_headers=headers,
            ssl=ssl.create_default_context(),
            ping_interval=20,
            ping_timeout=20,
            max_size=8 * 1024 * 1024,
        )

    async def subscribe_ws(self) -> None:
        if not self.ws:
            raise RuntimeError("WS not connected")
        market_tickers = list(self.markets_meta.keys())
        if not market_tickers:
            return

        # chunk subscriptions to avoid oversized messages
        chunk_size = 100
        chunks = [market_tickers[i:i + chunk_size] for i in range(0, len(market_tickers), chunk_size)]
        msg_id = 1
        for chunk in chunks:
subs = [
    {"id": msg_id, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": chunk}},
]
msg_id += 1
            for s in subs:
                await self.ws.send(json.dumps(s))
                await asyncio.sleep(0.05)
        log_line(self.cfg, f"WS subscribed across {len(chunks)} chunk(s) for {len(market_tickers)} market(s)")

    async def ws_loop(self) -> None:
        while True:
            try:
                await self.connect_ws()
                await self.subscribe_ws()
                async for raw in self.ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    msg_type = msg.get("type")
                    payload = msg.get("msg", {})
                    ticker = payload.get("market_ticker")
                    if not ticker:
                        continue
                    state = self.states.setdefault(ticker, MarketRealtimeState(ticker=ticker))
                    if msg_type == "ticker":
                        state.update_from_ticker(payload)
                    elif msg_type == "trade":
                        state.update_from_trade(payload)
                    elif msg_type in ("orderbook_delta", "orderbook_snapshot"):
                        if msg_type == "orderbook_delta":
                            state.update_from_orderbook_delta(payload)
            except Exception as e:
                log_line(self.cfg, f"WS loop error: {e}. reconnecting in 3s")
                await asyncio.sleep(3)

    def snapshot_row(self, ticker: str, state: MarketRealtimeState) -> Dict[str, Any]:
        return {
            "ts": now_ts(),
            "market_ticker": ticker,
            "yes_bid_cents": state.yes_bid_cents,
            "yes_ask_cents": state.yes_ask_cents,
            "no_bid_cents": state.no_bid_cents,
            "no_ask_cents": state.no_ask_cents,
            "spread_cents": state.spread_cents(),
            "yes_bid_size": state.yes_bid_size,
            "yes_ask_size": state.yes_ask_size,
            "volume": round(state.volume, 6),
            "open_interest": round(state.open_interest, 6),
            "last_price_cents": state.last_price_cents,
            "micro_imbalance": state.micro_imbalance(),
            "trade_pressure": state.trade_pressure(60),
            "orderbook_pressure": state.orderbook_pressure(60),
            "momentum_10": state.momentum(10),
            "momentum_30": state.momentum(30),
            "momentum_60": state.momentum(60),
            "reversion_z_60": state.reversion_z(60),
            "volatility_60": state.volatility(60),
        }

    def maybe_log_candidate(self, ticker: str, state: MarketRealtimeState) -> None:
        passes, fail_reasons = hard_filter_passes(state, self.cfg)
        if not passes:
            return

        now = now_ts()
        if now - state.last_logged_signal_ts < self.cfg.per_market_signal_cooldown_seconds:
            return

        ask = state.yes_ask_cents
        if ask is None:
            return
        target = min(99, int(math.ceil(ask * (1.0 + self.cfg.target_return_pct))))
        stop = max(1, int(math.floor(ask * (1.0 - self.cfg.stop_return_pct))))
        signal_type = choose_signal_type(state)
        rank_score = compute_rank_score(state, self.cfg)
        reason_codes = build_reason_codes(state, self.cfg)

        signal_id = str(uuid.uuid4())
        row = {
            "signal_id": signal_id,
            "created_ts": now,
            "market_ticker": ticker,
            "event_ticker": state.event_ticker,
            "category": state.category,
            "title": state.title,
            "status": "pending",
            "signal_type": signal_type,
            "hypothetical_side": "yes",
            "hypothetical_entry_price_cents": ask,
            "hypothetical_target_price_cents": target,
            "hypothetical_stop_price_cents": stop,
            "close_ts": state.close_ts,
            "hours_to_close": state.hours_to_close(),
            "yes_bid_cents": state.yes_bid_cents,
            "yes_ask_cents": state.yes_ask_cents,
            "no_bid_cents": state.no_bid_cents,
            "no_ask_cents": state.no_ask_cents,
            "spread_cents": state.spread_cents(),
            "yes_bid_size": state.yes_bid_size,
            "yes_ask_size": state.yes_ask_size,
            "volume": state.volume,
            "open_interest": state.open_interest,
            "momentum_10": state.momentum(10),
            "momentum_30": state.momentum(30),
            "momentum_60": state.momentum(60),
            "reversion_z_60": state.reversion_z(60),
            "trade_pressure": state.trade_pressure(60),
            "orderbook_pressure": state.orderbook_pressure(60),
            "micro_imbalance": state.micro_imbalance(),
            "volatility_60": state.volatility(60),
            "rank_score": rank_score,
            "reason_codes_json": json_dumps(reason_codes),
            "metadata_json": json_dumps({
                "filter_fail_reasons": fail_reasons,
                "collector_mode": "candidate_only_no_trade",
                "run_started_ts": self.run_started_ts,
            }),
        }
        self.db.insert_signal(row)
        state.last_logged_signal_ts = now

    def label_ready_signals(self) -> int:
        cutoff = now_ts() - self.cfg.label_horizon_seconds
        rows = self.db.get_pending_signals_ready_for_label(cutoff, limit=5000)
        labeled = 0
        for r in rows:
            snaps = self.db.get_snapshots_for_window(
                r["market_ticker"],
                safe_int(r["created_ts"]),
                safe_int(r["created_ts"]) + self.cfg.label_horizon_seconds,
            )
            if not snaps:
                continue
            updates = compute_label_from_snapshots(r, snaps)
            self.db.label_signal(r["signal_id"], updates)
            labeled += 1
        return labeled

    async def periodic_loop(self) -> None:
        while True:
            now = now_ts()
            if (now - self.last_universe_refresh) >= self.cfg.refresh_universe_every_seconds or not self.markets_meta:
                self.refresh_universe()
                if self.ws:
                    try:
                        await self.ws.close()
                    except Exception:
                        pass

            if (now - self.last_snapshot_flush) >= self.cfg.snapshot_every_seconds:
                snap_count = 0
                signal_count = 0
                for ticker, state in list(self.states.items()):
                    if state.last_update_ts <= 0:
                        continue
                    self.db.insert_snapshot(self.snapshot_row(ticker, state))
                    snap_count += 1
                    self.maybe_log_candidate(ticker, state)
                    if state.last_logged_signal_ts == now:
                        signal_count += 1
                self.last_snapshot_flush = now
                self.db.upsert_stat("last_snapshot_flush_ts", str(now))
                self.db.upsert_stat("last_snapshot_count", str(snap_count))
                if signal_count:
                    self.db.upsert_stat("last_signal_burst_count", str(signal_count))

            labeled = self.label_ready_signals()
            if labeled:
                self.db.upsert_stat("last_labeled_batch_count", str(labeled))

            if (now - self.last_summary) >= self.cfg.summary_every_seconds:
                counts = self.db.summary_counts()
                tracked = len(self.states)
                log_line(
                    self.cfg,
                    f"tracked_markets={tracked} pending={counts.get('pending',0)} labeled={counts.get('labeled',0)}"
                )
                self.last_summary = now

            await asyncio.sleep(1)

    async def run(self) -> None:
        self.refresh_universe()
        await asyncio.gather(
            self.ws_loop(),
            self.periodic_loop(),
        )


# ==============================
# CLI
# ==============================


def load_config(path: Optional[str]) -> CollectorConfig:
    cfg = CollectorConfig()
    if not path:
        return cfg
    with open(path, "r", encoding="utf-8") as f:
        user = json.load(f)
    for k, v in user.items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    return cfg

async def run(self) -> None:
    try:
        log_line(self.cfg, "collector starting")
        self.refresh_universe()
        log_line(self.cfg, f"collector universe loaded markets={len(self.markets_meta)}")
        await asyncio.gather(
            self.ws_loop(),
            self.periodic_loop(),
        )
    except Exception as e:
        log_line(self.cfg, f"collector fatal error: {e}")
        raise
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    cfg = load_config(args.config)
    app = SignalCollectorApp(cfg)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
