from __future__ import annotations

import asyncio
import json
import math
import os
import re
import statistics
import time
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import websockets
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import (
    create_engine, String, DateTime, Boolean, Integer, BigInteger, Float, Text,
    ForeignKey, UniqueConstraint, select, desc, func, text, delete
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

APP_NAME = "LAES · Leader-Anchored Adverse Excursion Scalping"
BINANCE_ROOT = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BINANCE_COPY_ROOT = os.getenv("BINANCE_COPY_ROOT", "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade").rstrip("/")
KOPYON_BASE_URL = os.getenv("KOPYON_BASE_URL", "https://api.kopyon.com/v5").rstrip("/")
KOPYON_API_KEY = os.getenv("KOPYON_API_KEY", "").strip()
KOPYON_ACTIVE = bool(KOPYON_API_KEY)
KOPYON_SOURCE = "KOPYON_API_V5"
# Kopyon's documented Free allowance is 10,000 requests per 30 days. These
# defaults use about 2,880 calls/30 days for discovery and position snapshots,
# leaving substantial headroom for user-requested profile/history loads.
if KOPYON_ACTIVE:
    DISCOVERY_INTERVAL = max(1800, int(os.getenv("KOPYON_DISCOVERY_INTERVAL_SECONDS", "7200")))
    POSITION_REFRESH_INTERVAL = max(300, int(os.getenv("KOPYON_POSITION_REFRESH_INTERVAL_SECONDS", "600")))
    POSITION_BATCH = max(1, min(20, int(os.getenv("KOPYON_POSITION_BATCH", "20"))))
    TRADER_DISCOVERY_PAGES = max(1, min(20, int(os.getenv("KOPYON_DISCOVERY_PAGES", "6"))))
else:
    DISCOVERY_INTERVAL = max(900, int(os.getenv("LAES_DISCOVERY_INTERVAL_SECONDS", "1800")))
    POSITION_REFRESH_INTERVAL = 300
    POSITION_BATCH = max(1, min(20, int(os.getenv("LAES_POSITION_BATCH", "20"))))
    TRADER_DISCOVERY_PAGES = max(1, min(20, int(os.getenv("LAES_TRADER_DISCOVERY_PAGES", "5"))))
BINANCE_REST_WEIGHT_BUDGET = max(10, min(1000, int(os.getenv("LAES_BINANCE_REST_WEIGHT_BUDGET_PER_MINUTE", "60"))))
LEADER_MIN_TRADING_DAYS = max(1, int(os.getenv("LAES_LEADER_MIN_TRADING_DAYS", "14")))
LEADER_MIN_30D_ROI_PCT = float(os.getenv("LAES_LEADER_MIN_30D_ROI_PCT", "0"))
LEADER_MIN_WIN_RATE_PCT = float(os.getenv("LAES_LEADER_MIN_WIN_RATE_PCT", "50"))
LEADER_MAX_30D_DRAWDOWN_PCT = float(os.getenv("LAES_LEADER_MAX_30D_DRAWDOWN_PCT", "40"))
LEADER_MIN_SHARPE = float(os.getenv("LAES_LEADER_MIN_SHARPE", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./laes_local.db").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
SQLITE = DATABASE_URL.startswith("sqlite")
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
    connect_args={"check_same_thread": False} if SQLITE else {"connect_timeout": 15}
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Trader(Base):
    __tablename__ = "traders"
    trader_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(240))
    avatar_url: Mapped[str | None] = mapped_column(Text)
    portfolio_type: Mapped[str | None] = mapped_column(String(40))
    visibility: Mapped[str] = mapped_column(String(32), default="unknown")
    roi: Mapped[float | None] = mapped_column(Float)
    pnl: Mapped[float | None] = mapped_column(Float)
    aum: Mapped[float | None] = mapped_column(Float)
    max_drawdown: Mapped[float | None] = mapped_column(Float)
    win_rate: Mapped[float | None] = mapped_column(Float)
    copier_pnl: Mapped[float | None] = mapped_column(Float)
    sharpe_ratio: Mapped[float | None] = mapped_column(Float)
    raw_json: Mapped[str] = mapped_column(Text, default="{}")
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class PositionSnapshot(Base):
    __tablename__ = "position_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trader_id: Mapped[str] = mapped_column(ForeignKey("traders.trader_id"), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    source_fetched_at: Mapped[str | None] = mapped_column(String(80))
    hidden: Mapped[bool] = mapped_column(Boolean, default=False)
    position_count: Mapped[int] = mapped_column(Integer, default=0)
    raw_json: Mapped[str] = mapped_column(Text, default="{}")

class PositionObservation(Base):
    __tablename__ = "position_observations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("position_snapshots.id"), index=True)
    trader_id: Mapped[str] = mapped_column(String(160), index=True)
    position_key: Mapped[str] = mapped_column(String(360), index=True)
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(12))
    size: Mapped[float | None] = mapped_column(Float)
    entry_price: Mapped[float | None] = mapped_column(Float)
    mark_price: Mapped[float | None] = mapped_column(Float)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float)
    leverage: Mapped[float | None] = mapped_column(Float)
    margin_mode: Mapped[str | None] = mapped_column(String(20))
    liquidation_price: Mapped[float | None] = mapped_column(Float)
    roi: Mapped[float | None] = mapped_column(Float)
    raw_json: Mapped[str] = mapped_column(Text, default="{}")

class SourceCheck(Base):
    __tablename__ = "source_checks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(80), index=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    ok: Mapped[bool] = mapped_column(Boolean)
    http_status: Mapped[int | None] = mapped_column(Integer)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    detail: Mapped[str] = mapped_column(Text, default="")

class AppSetting(Base):
    __tablename__ = "app_settings"
    key: Mapped[str] = mapped_column(String(120), primary_key=True)
    value: Mapped[str] = mapped_column(Text)

class TraderProfileCache(Base):
    __tablename__ = "trader_profile_cache"
    trader_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    raw_profile: Mapped[str] = mapped_column(Text, default="{}")
    raw_performance: Mapped[str] = mapped_column(Text, default="{}")

class HistoryPosition(Base):
    __tablename__ = "history_positions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trader_id: Mapped[str] = mapped_column(String(160), index=True)
    position_id: Mapped[str | None] = mapped_column(String(180), index=True)
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(12))
    status: Mapped[str | None] = mapped_column(String(40))
    opened_at_ms: Mapped[int | None] = mapped_column(BigInteger)
    closed_at_ms: Mapped[int | None] = mapped_column(BigInteger)
    entry_price: Mapped[float | None] = mapped_column(Float)
    close_price: Mapped[float | None] = mapped_column(Float)
    roi: Mapped[float | None] = mapped_column(Float)
    gross_pnl: Mapped[float | None] = mapped_column(Float)
    net_pnl: Mapped[float | None] = mapped_column(Float)
    raw_json: Mapped[str] = mapped_column(Text, default="{}")

class MarketInsight(Base):
    __tablename__ = "market_insights"
    symbol: Mapped[str] = mapped_column(String(80), primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    raw_json: Mapped[str] = mapped_column(Text, default="{}")

class PaperTradePath(Base):
    __tablename__ = "paper_trade_paths"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    paper_trade_id: Mapped[int] = mapped_column(ForeignKey("paper_trades.id"), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    paper_price: Mapped[float] = mapped_column(Float)
    leader_mark: Mapped[float | None] = mapped_column(Float)
    leader_unrealized_pnl: Mapped[float | None] = mapped_column(Float)
    leader_adverse_pct: Mapped[float | None] = mapped_column(Float)
    gross_pnl: Mapped[float] = mapped_column(Float, default=0)
    net_pnl: Mapped[float] = mapped_column(Float, default=0)
    pnl_pct: Mapped[float] = mapped_column(Float, default=0)
    source: Mapped[str] = mapped_column(String(40), default="BINANCE_REST")
    data_gap_seconds: Mapped[float] = mapped_column(Float, default=0)

class PaperTradeEvent(Base):
    __tablename__ = "paper_trade_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    paper_trade_id: Mapped[int] = mapped_column(ForeignKey("paper_trades.id"), index=True)
    event_type: Mapped[str] = mapped_column(String(60), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    value: Mapped[float | None] = mapped_column(Float)
    detail: Mapped[str] = mapped_column(Text, default="")

class OpportunityDecision(Base):
    __tablename__ = "opportunity_decisions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trader_id: Mapped[str] = mapped_column(String(160), index=True)
    snapshot_id: Mapped[int | None] = mapped_column(Integer, index=True)
    position_key: Mapped[str] = mapped_column(String(420), index=True)
    strategy: Mapped[str] = mapped_column(String(24), index=True)
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(12))
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    qualified: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    status: Mapped[str] = mapped_column(String(80))
    execution_status: Mapped[str] = mapped_column(String(80), default="NOT_ATTEMPTED")
    paper_trade_id: Mapped[int | None] = mapped_column(Integer, index=True)
    reasons_json: Mapped[str] = mapped_column(Text, default="[]")
    checks_json: Mapped[str] = mapped_column(Text, default="{}")


class PaperTrade(Base):
    __tablename__ = "paper_trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_key: Mapped[str] = mapped_column(String(420), index=True)
    trader_id: Mapped[str] = mapped_column(String(160), index=True)
    trader_name: Mapped[str | None] = mapped_column(String(240))
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(12))
    strategy: Mapped[str] = mapped_column(String(24), default="RECOVERY", index=True)
    status: Mapped[str] = mapped_column(String(20), default="OPEN", index=True)
    entry_price: Mapped[float] = mapped_column(Float)
    mark_price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[float] = mapped_column(Float)
    notional: Mapped[float] = mapped_column(Float)
    margin_used: Mapped[float] = mapped_column(Float)
    leverage: Mapped[float] = mapped_column(Float)
    stop_price: Mapped[float] = mapped_column(Float)
    target_price: Mapped[float] = mapped_column(Float)
    gross_pnl: Mapped[float] = mapped_column(Float, default=0)
    estimated_costs: Mapped[float] = mapped_column(Float, default=0)
    net_pnl: Mapped[float] = mapped_column(Float, default=0)
    exit_price: Mapped[float | None] = mapped_column(Float)
    exit_reason: Mapped[str | None] = mapped_column(String(80))
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_json: Mapped[str] = mapped_column(Text, default="{}")

def init_database(max_attempts: int = 8) -> None:
    """Create tables, retrying transient connection failures instead of crashing on the first one."""
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        try:
            Base.metadata.create_all(engine)
            return
        except Exception as exc:  # OperationalError: DNS/connection refused/starting DB
            if attempt == max_attempts:
                host = engine.url.host or ""
                hint = ""
                if host.startswith("dpg-") and "." not in host:
                    hint = (" DATABASE_URL uses a Render *internal* hostname, which only resolves from a service in the "
                            "SAME region as the database. Use the database's External Database URL, or recreate the "
                            "database in the web service's region.")
                raise RuntimeError(f"Database unreachable at host '{host}' after {max_attempts} attempts: {str(exc)[:300]}.{hint}") from exc
            print(f"[startup] database not reachable (attempt {attempt}/{max_attempts}): {str(exc)[:160]}; retrying in {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(20.0, delay * 2)


init_database()

# Provider timestamps are Unix milliseconds (13 digits); migrate legacy 32-bit columns.
if engine.dialect.name == "postgresql":
    with engine.begin() as migration:
        migration.execute(text("ALTER TABLE history_positions ALTER COLUMN opened_at_ms TYPE BIGINT"))
        migration.execute(text("ALTER TABLE history_positions ALTER COLUMN closed_at_ms TYPE BIGINT"))
        migration.execute(text("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS strategy VARCHAR(24) NOT NULL DEFAULT 'RECOVERY'"))

last_scan: dict[str, Any] = {"state": "starting", "started_at": None, "finished_at": None, "last_error": None, "traders_found": 0, "positions_checked": 0, "observations_saved": 0}
last_discovery_stats: dict[str, Any] = {"candidates": 0, "qualified": 0, "gate_failures": {}}
worker_alive = False
scan_lock = asyncio.Lock()
opportunity_lock = asyncio.Lock()
opportunity_strategy_locks = {"RECOVERY": asyncio.Lock(), "MOMENTUM": asyncio.Lock()}
opportunity_result_cache: dict[str, dict] = {
    "RECOVERY": {"at": 0.0, "items": []},
    "MOMENTUM": {"at": 0.0, "items": []},
}
OPPORTUNITY_CACHE_TTL_SECONDS = 30
recovery_audit_lock = asyncio.Lock()
last_manual_sync = 0.0
news_cache: dict[str, Any] = {"fetched_at": 0.0, "items": [], "error": None}

# Binance public market data is WebSocket-first. A circuit breaker prevents repeated
# REST fallback calls from extending exchange-side HTTP 418/429 bans.
BINANCE_REST_BLOCKED_UNTIL = 0.0
BINANCE_REST_BLOCK_REASON = ""
BINANCE_REST_LAST_LIMIT_LOG_AT = 0.0

PAPER_PRICE_STREAM_STATE: dict[str, Any] = {
    "status": "starting", "connected": False, "connected_at": None,
    "last_message_at": None, "last_price_at": None, "last_flush_at": None,
    "messages_received": 0, "price_updates_received": 0, "reconnect_count": 0,
    "subscribed_symbols": [], "unsupported_symbols": [], "last_error": None
}
PAPER_SYMBOL_LAST_UPDATE: dict[str, str] = {}
PAPER_SYMBOL_LAST_PRICE: dict[str, float] = {}
PAPER_SYMBOL_LAST_PRICE_AT: dict[str, float] = {}
PAPER_SYMBOL_FUNDING_RATE: dict[str, float] = {}
PAPER_SYMBOL_INDEX_PRICE: dict[str, float] = {}
PAPER_SYMBOL_PRICE_HISTORY: dict[str, list[tuple[float, float]]] = {}
PAPER_CANDLE_CACHE: dict[str, dict[str, Any]] = {}
templates = Jinja2Templates(directory="templates")


class BinanceGlobalRateLimiter:
    """One rolling request-weight budget shared by every Binance HTTP call in this app."""

    def __init__(self, weight_per_minute: int):
        self.weight_per_minute = weight_per_minute
        self.events: deque[tuple[float, int]] = deque()
        self.lock = asyncio.Lock()
        self.last_server_used_weight: int | None = None
        self.last_server_weight_at = 0.0
        self.total_requests = 0
        self.total_weight = 0
        self.last_wait_seconds = 0.0

    async def acquire(self, weight: int = 1) -> float:
        weight = max(1, min(int(weight), self.weight_per_minute))
        waited = 0.0
        while True:
            async with self.lock:
                current = time.monotonic()
                while self.events and current - self.events[0][0] >= 60.0:
                    self.events.popleft()
                local_used = sum(event_weight for _, event_weight in self.events)
                external_used = 0
                if current - self.last_server_weight_at <= 60.0 and self.last_server_used_weight is not None:
                    after_server_sample = sum(
                        event_weight for event_at, event_weight in self.events
                        if event_at > self.last_server_weight_at
                    )
                    external_used = self.last_server_used_weight + after_server_sample
                effective_used = max(local_used, external_used)
                if effective_used + weight <= self.weight_per_minute:
                    self.events.append((current, weight))
                    self.total_requests += 1
                    self.total_weight += weight
                    self.last_wait_seconds = waited
                    return waited
                delay = 1.0
                if self.events:
                    delay = max(0.05, 60.0 - (current - self.events[0][0]))
                elif self.last_server_weight_at:
                    delay = max(0.05, 60.0 - (current - self.last_server_weight_at))
            await asyncio.sleep(delay)
            waited += delay

    def observe_server_weight(self, value: Any) -> None:
        parsed = safe_float(value)
        if parsed is not None:
            self.last_server_used_weight = max(0, int(parsed))
            self.last_server_weight_at = time.monotonic()

    def status(self) -> dict:
        current = time.monotonic()
        while self.events and current - self.events[0][0] >= 60.0:
            self.events.popleft()
        return {
            "weight_budget_per_minute": self.weight_per_minute,
            "local_weight_last_60s": sum(weight for _, weight in self.events),
            "server_used_weight_1m": self.last_server_used_weight,
            "server_weight_age_seconds": round(current - self.last_server_weight_at, 1) if self.last_server_weight_at else None,
            "total_requests": self.total_requests,
            "total_weight": self.total_weight,
            "last_wait_seconds": round(self.last_wait_seconds, 2),
        }


binance_rest_limiter = BinanceGlobalRateLimiter(BINANCE_REST_WEIGHT_BUDGET)

def now() -> datetime:
    return datetime.now(timezone.utc)

def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

def safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None

def parse_json(value: str | None, fallback: Any = None) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return {} if fallback is None else fallback

def unwrap_data(payload: Any) -> dict:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}

def timestamp_ms(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except Exception:
            return None

def ms_datetime(value: Any) -> datetime | None:
    millis = timestamp_ms(value)
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc) if millis is not None else None

def setting(key: str, default: Any) -> Any:
    with SessionLocal() as db:
        row = db.get(AppSetting, key)
        return parse_json(row.value, default) if row else default

def set_setting(key: str, value: Any) -> None:
    with SessionLocal() as db:
        row = db.get(AppSetting, key)
        if row is None:
            row = AppSetting(key=key, value=json.dumps(value))
            db.add(row)
        else:
            row.value = json.dumps(value)
        db.commit()

def record_opportunity_decision(obs, strategy, qualified, status, reasons, checks,
                               execution_status="NOT_ATTEMPTED", paper_trade_id=None):
    """Persist one candidate decision per minute with the actual gate evidence."""
    stamp = now()
    minute = stamp.replace(second=0, microsecond=0)
    with SessionLocal() as db:
        row = db.scalar(select(OpportunityDecision).where(
            OpportunityDecision.trader_id == obs.trader_id,
            OpportunityDecision.snapshot_id == obs.snapshot_id,
            OpportunityDecision.position_key == obs.position_key,
            OpportunityDecision.strategy == strategy,
            OpportunityDecision.observed_at >= minute
        ).order_by(desc(OpportunityDecision.id)).limit(1))
        if row is None:
            row = OpportunityDecision(trader_id=obs.trader_id, snapshot_id=obs.snapshot_id,
                position_key=obs.position_key, strategy=strategy, symbol=obs.symbol, side=obs.side,
                observed_at=stamp, qualified=bool(qualified), status=status,
                execution_status=execution_status, paper_trade_id=paper_trade_id)
            db.add(row)
        row.qualified = bool(qualified)
        row.status = status
        if not (row.execution_status == "PAPER_ENTRY_CREATED" and execution_status == "SCANNER_PREVIEW_ONLY"):
            row.execution_status = execution_status
            row.paper_trade_id = paper_trade_id or row.paper_trade_id
        row.reasons_json = json.dumps(reasons or [], separators=(",", ":"), default=str)
        row.checks_json = json.dumps(checks or {}, separators=(",", ":"), default=str)
        db.commit()


def paper_config() -> dict:
    defaults = {
        "capital_usdt": 200.0,
        "auto_enabled": True,
        "risk_pct": 0.50,
        "max_margin_pct": 10.0,
        "max_total_margin_pct": 40.0,
        "research_unconstrained": False,
        "max_notional_usdt": 50.0,
        "max_positions_per_symbol_direction": 1,
        "shadow_max_total_margin_pct": 40.0,
        "shadow_max_positions_per_symbol_direction": 1,
        "path_sample_seconds": 5.0,
        "meaningful_profit_steps": [0.10, 0.20, 0.30, 0.50, 0.70, 1.00],
        "target_pct": 0.70,
        "stop_pct": 0.45,
        "profit_harvest_min_margin_pct": 0.70,
        "profit_giveback_pct": 35.0,
        "recovery_max_hold_hours": 72.0,
        "momentum_max_hold_hours": 24.0,
        "structure_lookback_minutes": 60,
        "structure_break_buffer_pct": 0.20,
        "structure_break_confirmations": 3,
        "momentum_min_leader_profit_pct": 0.05,
        "momentum_min_5m_pct": 0.02,
        "entry_timing_audit_window_minutes": 30.0,
        "post_exit_recovery_audit_version": 9,
        "taker_fee_pct": 0.05,
        "slippage_pct": 0.02,
        "max_leverage": 3.0,
        "min_adverse_pct": 0.30,
        "reversal_min_bounce_pct": 0.03,
        "recovery_min_score": 30.0,
        "leader_continuation_enabled": True,
        "leader_reversal_exit_enabled": True,
        "post_exit_recovery_audit_hours": 12.0
    }
    saved = setting("paper_config", {})
    saved = saved if isinstance(saved, dict) else {}
    merged = {**defaults, **saved}
    config_version = int(setting("paper_config_version", 0) or 0)
    if config_version < 2:
        # One-time migration away from the old unconstrained research-wallet values.
        # The discovery scanner remains broad; only paper execution receives this wallet.
        for key in ("capital_usdt", "risk_pct", "max_margin_pct", "max_total_margin_pct",
                    "max_notional_usdt", "max_positions_per_symbol_direction", "max_leverage",
                    "research_unconstrained", "auto_enabled"):
            merged[key] = defaults[key]
        set_setting("paper_config", merged)
        set_setting("paper_config_version", 2)
    wallet_epoch = setting("shared_wallet_epoch_at", None)
    if not wallet_epoch:
        wallet_epoch = now().isoformat()
        set_setting("shared_wallet_epoch_at", wallet_epoch)
    merged["wallet_epoch_at"] = wallet_epoch
    # Discovery/qualification is intentionally independent of wallet capacity.
    # Only paper execution is constrained by the shared portfolio wallet.
    merged["capital_usdt"] = max(0.0, min(float(merged.get("capital_usdt", 200.0) or 0.0), 100000000.0))
    merged["max_margin_pct"] = min(100.0, max(0.1, float(merged.get("max_margin_pct", 10.0) or 10.0)))
    merged["max_total_margin_pct"] = min(100.0, max(1.0, float(merged.get("max_total_margin_pct", 40.0) or 40.0)))
    merged["max_notional_usdt"] = max(5.0, float(merged.get("max_notional_usdt", 50.0) or 50.0))
    merged["max_positions_per_symbol_direction"] = max(1, min(10, int(merged.get("max_positions_per_symbol_direction", 1) or 1)))
    merged["research_unconstrained"] = False
    return merged

def run_local_forensic_audit_all_closed() -> None:
    """Fast local forensic pass over every closed trade using persisted paper paths."""
    with SessionLocal() as db:
        trades = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED")).all()
        rows = db.execute(
            select(PaperTradePath.paper_trade_id, func.count(PaperTradePath.id),
                   func.max(PaperTradePath.net_pnl), func.min(PaperTradePath.net_pnl))
            .group_by(PaperTradePath.paper_trade_id)
        ).all()
        agg = {int(tid): (int(n or 0), safe_float(mfe), safe_float(mae))
               for tid, n, mfe, mae in rows}
        changed = 0
        for trade in trades:
            raw = parse_json(trade.raw_json)
            if int(raw.get("forensic_audit_version", 0) or 0) >= 1:
                continue
            n, mfe, mae = agg.get(int(trade.id), (0, None, None))
            raw["forensic_audit_version"] = 1
            raw["forensic_audit_status"] = "LOCAL_PATH_AUDIT_COMPLETE" if n else "NO_LOCAL_PATH_DATA"
            raw["forensic_path_count"] = n
            raw["forensic_path_mfe_net_pnl"] = mfe
            raw["forensic_path_mae_net_pnl"] = mae
            raw["forensic_exit_capture_pct"] = round(float(trade.net_pnl) / mfe * 100.0, 2) if mfe and mfe > 0 else None
            raw["forensic_mfe_vs_realized"] = (
                "NO_PATH" if mfe is None else
                "FAVORABLE_EXCURSION_LEFT_ON_TABLE" if mfe > float(trade.net_pnl or 0) + 0.05 else
                "SOME_FAVORABLE_EXCURSION_LEFT" if mfe > float(trade.net_pnl or 0) else
                "NO_MATERIAL_MFE_GAP"
            )
            raw["forensic_post_exit_status"] = (
                "POST_EXIT_CANDLE_PATH_PRESENT"
                if len(parse_json(raw.get("post_exit_mark_candles"), [])) > 0
                else "POST_EXIT_CANDLE_AUDIT_PENDING_EXTERNAL_CANDLE_PATH"
            )
            raw["forensic_entry_timing_status"] = "PRE_ENTRY_BINANCE_CANDLE_AUDIT_REQUIRED"
            raw["forensic_note"] = "Local path audit only; no better fill or post-exit recovery is claimed without verified Binance candles."
            trade.raw_json = json.dumps(raw)
            changed += 1
        if changed:
            db.commit()

def migrate_paper_audit_metadata() -> None:
    """
    Idempotent, non-destructive metadata migration.
    Financial PnL/status/exit prices are never rewritten here.
    """
    cutoff_utc = datetime(2026, 11, 5, 11, 29, 0, tzinfo=timezone.utc)  # 16:59 IST
    with SessionLocal() as db:
        trades = db.scalars(select(PaperTrade)).all()
        gap_rows = db.execute(
            select(PaperTradePath.paper_trade_id, func.max(PaperTradePath.data_gap_seconds))
            .group_by(PaperTradePath.paper_trade_id)
        ).all()
        max_gaps = {int(trade_id): float(gap or 0.0) for trade_id, gap in gap_rows}
        for trade in trades:
            raw = parse_json(trade.raw_json)
            opened = trade.opened_at
            if opened and opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            max_gap = max(
                max_gaps.get(trade.id, 0.0),
                float(raw.get("max_data_gap_seconds", 0.0) or 0.0)
            )
            raw["max_data_gap_seconds"] = max_gap
            raw.setdefault("audit_schema_version", 2)

            event_specs = []
            if opened and opened < cutoff_utc:
                # The user's 4:59 PM research cutoff takes precedence over generic
                # data-gap classification; both facts can still be shown separately.
                raw["data_quality_status"] = "INFORMATION_CUTOFF"
                raw["information_cutoff_at"] = "2026-11-05T16:59:00+05:30"
                raw["information_cutoff_reason"] = (
                    "Historical record opened before the 4:59 PM IST research information cutoff. "
                    "Its PnL is preserved, but the full observation path is not considered complete."
                )
                event_specs.append((
                    "INFORMATION_CUTOFF",
                    cutoff_utc,
                    "Historical record classified at the requested 2026-11-05 16:59 IST information cutoff; financial outcome unchanged."
                ))
                if max_gap > 15:
                    event_specs.append((
                        "DATA_INTERRUPTED_REVIEW",
                        trade.closed_at or trade.opened_at or cutoff_utc,
                        f"Historical maximum saved price-path gap: {max_gap:.1f} seconds; INFORMATION_CUTOFF label retained and financial outcome unchanged."
                    ))
            elif max_gap > 15 or raw.get("data_quality_status") == "CONTINUOUS_AGAIN":
                # CONTINUOUS_AGAIN was an old status written after a gap had occurred.
                # It is not proof that the historical price path was continuous.
                raw["data_quality_status"] = "DATA_INTERRUPTED_REVIEW"
                raw["data_quality_reason"] = (
                    "At least one saved price-path interval exceeded 15 seconds. "
                    "Monitoring may have resumed later, but the missing historical interval "
                    "cannot be reconstructed. Recorded PnL and exit are preserved."
                )
                event_specs.append((
                    "DATA_INTERRUPTED_REVIEW",
                    trade.closed_at or trade.opened_at or now(),
                    f"Historical maximum saved price-path gap: {max_gap:.1f} seconds; existing financial outcome preserved."
                ))
            elif not raw.get("data_quality_status"):
                raw["data_quality_status"] = "PATH_NOT_RECORDED" if trade.id not in max_gaps else "LEGACY_UNCLASSIFIED"

            if max_gap > 15 and raw.get("monitoring_status") != "RESUMED_AFTER_GAP":
                raw["monitoring_status"] = "INTERRUPTED"

            if "funding_history_verified" not in raw:
                raw["funding_audit_status"] = "LEGACY_FUNDING_UNVERIFIED"
                event_specs.append((
                    "LEGACY_FUNDING_UNVERIFIED",
                    trade.closed_at or trade.opened_at or now(),
                    "This record predates persisted funding verification. Funding cannot be reconstructed from its saved audit fields."
                ))

            # Reconcile legacy closed rows into an explicit realized-exit audit record.
            # This copies only values already stored on PaperTrade; it never infers a
            # missing path, price, fee, funding payment, or exit reason.
            if trade.status == "CLOSED":
                if raw.get("realized_net_pnl") is None:
                    raw["realized_net_pnl"] = float(trade.net_pnl or 0.0)
                    raw["realized_net_pnl_source"] = "LEGACY_PAPER_TRADES_ROW"
                if raw.get("realized_exit_price") is None and trade.exit_price is not None:
                    raw["realized_exit_price"] = float(trade.exit_price)
                if raw.get("realized_exit_at") is None and trade.closed_at is not None:
                    raw["realized_exit_at"] = trade.closed_at.isoformat()
                if raw.get("realized_exit_reason") is None:
                    raw["realized_exit_reason"] = trade.exit_reason

                realized_event_exists = db.scalar(select(PaperTradeEvent.id).where(
                    PaperTradeEvent.paper_trade_id == trade.id,
                    PaperTradeEvent.event_type.in_([
                        "REALIZED_EXIT",
                        "LEGACY_REALIZED_EXIT_RECONCILED"
                    ])
                ).limit(1))
                if not realized_event_exists:
                    db.add(PaperTradeEvent(
                        paper_trade_id=trade.id,
                        event_type="LEGACY_REALIZED_EXIT_RECONCILED",
                        observed_at=trade.closed_at or trade.opened_at or now(),
                        value=float(trade.net_pnl or 0.0),
                        detail=(
                            "Historical realized close copied from the preserved paper_trades row. "
                            "Original exit reason and net PnL are retained; historical price path and "
                            "funding are not reconstructed."
                        )
                    ))

            trade.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
            for event_type, event_at, detail in event_specs:
                exists = db.scalar(select(PaperTradeEvent.id).where(
                    PaperTradeEvent.paper_trade_id == trade.id,
                    PaperTradeEvent.event_type == event_type
                ).limit(1))
                if not exists:
                    db.add(PaperTradeEvent(
                        paper_trade_id=trade.id,
                        event_type=event_type,
                        observed_at=event_at,
                        value=trade.net_pnl,
                        detail=detail
                    ))
        db.commit()


migrate_paper_audit_metadata()


def log_check(source: str, ok: bool, status: int | None = None, latency: int | None = None, detail: str = "") -> None:
    try:
        with SessionLocal() as db:
            db.add(SourceCheck(source=source, ok=ok, http_status=status, latency_ms=latency, detail=detail[:900]))
            db.commit()
    except Exception:
        pass

async def request_json(
    url: str, params: dict | None = None, headers: dict | None = None,
    timeout: float = 20, method: str = "GET", json_body: dict | None = None,
    response_headers: dict | None = None
) -> tuple[Any, int]:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(
            method, url, params=params or {}, headers=headers or {}, json=json_body
        )
    if response_headers is not None:
        response_headers.update({key.lower(): value for key, value in response.headers.items()})
    if response.status_code >= 400:
        message = response.text[:350]
        retry_after = response.headers.get("Retry-After")
        retry_note = f" Retry-After={retry_after}." if retry_after else ""
        raise RuntimeError(f"HTTP {response.status_code}.{retry_note} {message}")
    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError("Provider returned a non-JSON response") from exc
    # Preserve JSON arrays: Binance /klines returns a list, not an object.
    return payload, response.status_code

def _binance_rate_limit_cooldown(error: Exception) -> None:
    global BINANCE_REST_BLOCKED_UNTIL, BINANCE_REST_BLOCK_REASON, BINANCE_REST_LAST_LIMIT_LOG_AT
    message = str(error)
    match = re.search(r"HTTP\s+(403|418|429|451)", message)
    if not match:
        return
    status = int(match.group(1))
    current = time.time()
    until = None
    ban_match = re.search(r"banned until\s+(\d{13})", message, re.IGNORECASE)
    if ban_match:
        until = int(ban_match.group(1)) / 1000.0
    if until is None:
        retry_note = message.split("Retry-After=", 1)[1].split(". ", 1)[0].strip().rstrip(".") if "Retry-After=" in message else ""
        retry_seconds = safe_float(retry_note)
        if retry_seconds is not None:
            until = current + retry_seconds
        elif retry_note:
            try:
                retry_at = parsedate_to_datetime(retry_note)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                until = retry_at.timestamp()
            except (TypeError, ValueError, OverflowError):
                until = None
        if until is None:
            until = current + (3600 if status in (418, 451) else 600 if status == 403 else 120)
    if until <= current:
        until = current + (3600 if status in (418, 451) else 600 if status == 403 else 120)
    if until > BINANCE_REST_BLOCKED_UNTIL:
        BINANCE_REST_BLOCKED_UNTIL = until
        BINANCE_REST_BLOCK_REASON = (
            f"Binance HTTP {status}" + (" (geo-restricted IP: deploy in a non-US region such as Frankfurt)" if status == 451 else "")
            + "; all Binance REST and public copy-page HTTP calls are paused."
        )
    if current - BINANCE_REST_LAST_LIMIT_LOG_AT >= 30:
        BINANCE_REST_LAST_LIMIT_LOG_AT = current
        log_check(
            "binance_rest_circuit_breaker", False, status, None,
            f"REST paused until {datetime.fromtimestamp(BINANCE_REST_BLOCKED_UNTIL, timezone.utc).isoformat()}. {message[:350]}"
        )


def binance_rest_status() -> dict:
    remaining = max(0.0, BINANCE_REST_BLOCKED_UNTIL - time.time())
    return {
        "blocked": remaining > 0,
        "blocked_until": datetime.fromtimestamp(BINANCE_REST_BLOCKED_UNTIL, timezone.utc).isoformat() if remaining else None,
        "remaining_seconds": round(remaining, 1),
        "reason": BINANCE_REST_BLOCK_REASON if remaining else None,
        "limiter": binance_rest_limiter.status()
    }


def restore_binance_rest_cooldown() -> None:
    """
    Restore an active Binance 418/429 ban after a Render restart/redeploy.
    Otherwise each restart could make another prohibited REST request.
    """
    global BINANCE_REST_BLOCKED_UNTIL, BINANCE_REST_BLOCK_REASON
    try:
        with SessionLocal() as db:
            row = db.scalar(
                select(SourceCheck)
                .where(SourceCheck.source == "binance_rest_circuit_breaker")
                .order_by(desc(SourceCheck.id))
                .limit(1)
            )
        if not row or not row.detail:
            return
        match = re.search(r"REST paused until (\S+)", row.detail)
        if not match:
            return
        expires_at = datetime.fromisoformat(match.group(1).rstrip("."))
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        expires_epoch = expires_at.timestamp()
        if expires_epoch > time.time():
            BINANCE_REST_BLOCKED_UNTIL = expires_epoch
            BINANCE_REST_BLOCK_REASON = row.detail[:700]
    except Exception:
        # The live WebSocket path must still start even if the persisted audit row
        # cannot be read; it will re-establish the circuit breaker on next 418/429.
        return


restore_binance_rest_cooldown()


KOPYON_LIMIT_STATUS: dict[str, Any] = {"limit_month": None, "remaining_month": None, "reset_month": None, "observed_at": None}

async def kopyon_get(route: str, params: dict | None = None) -> tuple[dict, int]:
    """Call Kopyon's documented v5 API independently of Binance's REST ban gate."""
    if not KOPYON_API_KEY:
        raise RuntimeError("KOPYON_API_KEY is not configured on the web service")
    url = f"{KOPYON_BASE_URL}/{route.lstrip('/')}"
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=18) as client:
            response = await client.get(url, params=params or {}, headers={"Accept": "application/json", "X-API-KEY": KOPYON_API_KEY})
        headers = {key.lower(): value for key, value in response.headers.items()}
        for header, field in (("x-ratelimit-limit-month", "limit_month"), ("x-ratelimit-remaining-month", "remaining_month"), ("x-ratelimit-reset-month", "reset_month")):
            if headers.get(header) is not None:
                KOPYON_LIMIT_STATUS[field] = safe_float(headers[header])
        KOPYON_LIMIT_STATUS["observed_at"] = now().isoformat()
        if response.status_code >= 400:
            detail = response.text[:350]
            log_check("kopyon_api", False, response.status_code, int((time.perf_counter() - started) * 1000), f"{route}: {detail}")
            raise RuntimeError(f"Kopyon HTTP {response.status_code}: {detail}")
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError("Kopyon returned a non-JSON response") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            error = payload.get("error") if isinstance(payload, dict) else None
            raise RuntimeError(f"Kopyon response has no data object: {str(error or payload)[:300]}")
        log_check("kopyon_api", True, response.status_code, int((time.perf_counter() - started) * 1000), route)
        return payload, response.status_code
    except Exception as exc:
        if not str(exc).startswith("Kopyon HTTP "):
            log_check("kopyon_api", False, None, int((time.perf_counter() - started) * 1000), f"{route}: {str(exc)[:650]}")
        raise

async def binance_get(path: str, params: dict | None = None) -> tuple[Any, int]:
    weight = _binance_rest_weight(path, params)
    return await _binance_http_request(
        f"{BINANCE_ROOT}{path}", path, "GET", params=params, weight=weight
    )


def _binance_rest_weight(path: str, params: dict | None = None) -> int:
    params = params or {}
    if path.endswith("/ticker/24hr") and not params.get("symbol"):
        return 40
    if path.endswith("/premiumIndex") and not params.get("symbol"):
        return 10
    if path.endswith("/depth"):
        limit = int(params.get("limit", 100) or 100)
        return 5 if limit <= 100 else 10 if limit <= 500 else 20
    return 1


async def _binance_http_request(
    url: str, route: str, method: str, params: dict | None = None,
    body: dict | None = None, weight: int = 1, headers: dict | None = None
) -> tuple[Any, int]:
    if time.time() < BINANCE_REST_BLOCKED_UNTIL:
        remaining = max(0.0, BINANCE_REST_BLOCKED_UNTIL - time.time())
        raise RuntimeError(f"Binance global REST circuit breaker active for {remaining:.0f}s.")
    await binance_rest_limiter.acquire(weight)
    if time.time() < BINANCE_REST_BLOCKED_UNTIL:
        remaining = max(0.0, BINANCE_REST_BLOCKED_UNTIL - time.time())
        raise RuntimeError(f"Binance global REST circuit breaker active for {remaining:.0f}s.")
    started = time.perf_counter()
    response_headers: dict[str, str] = {}
    try:
        payload, status = await request_json(
            url, params=params, headers={"Accept": "application/json", **(headers or {})},
            timeout=18, method=method, json_body=body,
            response_headers=response_headers
        )
    except Exception as exc:
        _binance_rate_limit_cooldown(exc)
        raise
    for key, value in response_headers.items():
        if key.startswith("x-mbx-used-weight-") and key.endswith("1m"):
            binance_rest_limiter.observe_server_weight(value)
    source = "binance_copy_trading_page" if "www.binance.com/bapi/" in url else "binance_market_data"
    log_check(source, True, status, int((time.perf_counter() - started) * 1000), route)
    return payload, status


async def binance_copy_get(route: str, params: dict | None = None) -> tuple[dict, int]:
    payload, status = await _binance_http_request(
        f"{BINANCE_COPY_ROOT}/{route}", route, "GET", params=params, weight=1
    )
    return _validate_binance_copy_payload(payload), status


async def binance_copy_post(route: str, body: dict) -> tuple[dict, int]:
    headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": "LAES public Binance data monitor"}
    # Keep the page endpoint and market REST requests under the same IP-budget gate.
    payload, status = await _binance_http_request(
        f"{BINANCE_COPY_ROOT}/{route}", route, "POST", body=body, weight=1, headers=headers
    )
    return _validate_binance_copy_payload(payload), status


def _validate_binance_copy_payload(payload: Any) -> dict:
    if not isinstance(payload, dict):
        raise RuntimeError("Binance Copy Trading source returned a non-object response")
    code = str(payload.get("code", ""))
    if code and code not in {"000000", "0"}:
        message = str(payload.get("message") or payload.get("messageDetail") or code)
        raise RuntimeError(f"Binance Copy Trading source error {code}: {message[:300]}")
    if "data" not in payload:
        raise RuntimeError("Binance Copy Trading source response has no data field")
    return payload

BINANCE_COPY_SOURCE = "BINANCE_PUBLIC_COPY_TRADING_PAGE"


def normalize_binance_leader_row(row: dict) -> dict:
    """Convert Binance page metrics (percent units) to the app's fractional ratios."""
    roi_pct = safe_float(row.get("roi"))
    drawdown_pct = safe_float(row.get("mdd"))
    win_rate_pct = safe_float(row.get("winRate"))
    sharpe = safe_float(row.get("sharpRatio"))
    copier_pnl = safe_float(row.get("copierPnl"))
    started_ms = timestamp_ms(row.get("startTime"))
    tenure_days = max(0.0, (now().timestamp() - started_ms / 1000.0) / 86400.0) if started_ms else 0.0
    gates = {
        "public_portfolio": str(row.get("portfolioType") or "").upper() == "PUBLIC",
        "tenure_days": tenure_days >= LEADER_MIN_TRADING_DAYS,
        "positive_30d_roi": roi_pct is not None and roi_pct > LEADER_MIN_30D_ROI_PCT,
        "win_rate": win_rate_pct is not None and win_rate_pct >= LEADER_MIN_WIN_RATE_PCT,
        "max_drawdown": drawdown_pct is not None and drawdown_pct <= LEADER_MAX_30D_DRAWDOWN_PCT,
        "sharpe": sharpe is not None and sharpe >= LEADER_MIN_SHARPE,
        "copier_profit": copier_pnl is not None and copier_pnl > 0,
    }
    return {
        "traderId": str(row.get("leadPortfolioId") or "").strip(),
        "name": row.get("nickname"),
        "avatarUrl": row.get("avatarUrl"),
        "type": row.get("portfolioType") or "PUBLIC",
        "roi": roi_pct / 100.0 if roi_pct is not None else None,
        "pnl": safe_float(row.get("pnl")),
        "aum": safe_float(row.get("aum")),
        "maxDrawdown": drawdown_pct / 100.0 if drawdown_pct is not None else None,
        "winRate": win_rate_pct / 100.0 if win_rate_pct is not None else None,
        "copierPnl": copier_pnl,
        "sharpeRatio": sharpe,
        "copiers": safe_float(row.get("currentCopyCount")),
        "maxCopiers": safe_float(row.get("maxCopyCount")),
        "startTime": started_ms,
        "tradingDays": round(tenure_days, 1),
        "roi30dPct": roi_pct,
        "mdd30dPct": drawdown_pct,
        "winRatePct": win_rate_pct,
        "source": BINANCE_COPY_SOURCE,
        "qualityGates": gates,
        "qualityQualified": all(gates.values()),
        "sourcePayload": row,
    }


def normalize_kopyon_leader_row(row: dict) -> dict:
    """Map Kopyon v5 fractional ratios into LAES percentage-based quality gates."""
    roi_ratio = safe_float(row.get("roi"))
    drawdown_ratio = safe_float(row.get("maxDrawdown"))
    win_rate_ratio = safe_float(row.get("winRate"))
    roi_pct = roi_ratio * 100.0 if roi_ratio is not None else None
    drawdown_pct = drawdown_ratio * 100.0 if drawdown_ratio is not None else None
    win_rate_pct = win_rate_ratio * 100.0 if win_rate_ratio is not None else None
    sharpe = safe_float(row.get("sharpeRatio"))
    copier_pnl = safe_float(row.get("copierPnl"))
    details = row.get("sourceDetails") if isinstance(row.get("sourceDetails"), dict) else {}
    started_ms = timestamp_ms(details.get("startedAt"))
    tenure_days = max(0.0, (now().timestamp() - started_ms / 1000.0) / 86400.0) if started_ms else 0.0
    gates = {
        "public_portfolio": str(details.get("type") or row.get("type") or "PUBLIC").upper() == "PUBLIC",
        # The list endpoint may omit start date / Sharpe: unknown passes, known values must meet the gate.
        "tenure_days": (not started_ms) or tenure_days >= LEADER_MIN_TRADING_DAYS,
        "positive_30d_roi": roi_pct is not None and roi_pct > LEADER_MIN_30D_ROI_PCT,
        "win_rate": win_rate_pct is not None and win_rate_pct >= LEADER_MIN_WIN_RATE_PCT,
        "max_drawdown": drawdown_pct is not None and drawdown_pct <= LEADER_MAX_30D_DRAWDOWN_PCT,
        "sharpe": sharpe is None or sharpe >= LEADER_MIN_SHARPE,
        "copier_profit": copier_pnl is not None and copier_pnl > 0,
    }
    normalized = {
        "traderId": str(row.get("traderId") or "").strip(),
        "name": row.get("name"), "avatarUrl": row.get("avatarUrl"),
        "type": str(details.get("type") or row.get("type") or "PUBLIC").upper(),
        "roi": roi_ratio, "pnl": safe_float(row.get("pnl")),
        "aum": safe_float(row.get("aum")),
        "maxDrawdown": drawdown_ratio, "winRate": win_rate_ratio,
        "copierPnl": copier_pnl, "sharpeRatio": sharpe,
        "copiers": safe_float(row.get("copiers")),
        "maxCopiers": safe_float(row.get("maxCopiers")),
        "startTime": started_ms, "tradingDays": round(tenure_days, 1),
        "roi30dPct": roi_pct, "mdd30dPct": drawdown_pct,
        "winRatePct": win_rate_pct, "source": BINANCE_COPY_SOURCE,
        "provider": KOPYON_SOURCE, "qualityGates": gates,
        "qualityQualified": all(gates.values()), "sourcePayload": row,
    }
    return normalized

def is_qualified_binance_leader(raw: dict) -> bool:
    return raw.get("source") == BINANCE_COPY_SOURCE and bool(raw.get("qualityQualified"))


def is_binance_snapshot(snapshot: PositionSnapshot) -> bool:
    payload = parse_json(snapshot.raw_json)
    data = payload.get("data") if isinstance(payload, dict) else None
    return isinstance(data, dict) and data.get("source") == BINANCE_COPY_SOURCE


def normalize_binance_positions(payload: dict) -> dict:
    data = payload.get("data")
    if isinstance(data, list):
        source_rows = data
    elif isinstance(data, dict):
        source_rows = data.get("positions")
        if source_rows is None:
            source_rows = data.get("list")
    else:
        source_rows = None
    if not isinstance(source_rows, list):
            raise RuntimeError("Binance Copy Trading source response has no positions list")
    positions = []
    for row in source_rows:
        if not isinstance(row, dict):
            continue
        amount = safe_float(row.get("positionAmount") or row.get("amount") or row.get("size"))
        if amount is None or abs(amount) <= 1e-12:
            continue
        position_side = str(row.get("positionSide") or row.get("side") or "").upper()
        side = position_side if position_side in {"LONG", "SHORT"} else ("LONG" if amount > 0 else "SHORT")
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        positions.append({
            "positionId": str(row.get("positionId") or row.get("id") or f"{symbol}_{side}"),
            "symbol": symbol,
            "side": side,
            "size": abs(amount),
            "entryPrice": safe_float(row.get("entryPrice")),
            "markPrice": safe_float(row.get("markPrice")),
            "unrealizedPnl": safe_float(row.get("unrealizedProfit") or row.get("unrealizedPnl")),
            "leverage": safe_float(row.get("leverage")),
            "marginMode": str(row.get("marginMode") or ("ISOLATED" if row.get("isolated") else "CROSS")).upper(),
            "liquidationPrice": safe_float(row.get("liquidationPrice")),
            "roi": safe_float(row.get("roi")),
            "source": BINANCE_COPY_SOURCE,
            "provider": KOPYON_SOURCE if KOPYON_ACTIVE else BINANCE_COPY_SOURCE,
            "sourcePayload": row,
        })
    hidden = bool(data.get("hidden", False)) if isinstance(data, dict) else False
    fetched_at = (data.get("fetchedAt") or now().isoformat()) if isinstance(data, dict) else now().isoformat()
    return {
        "code": "000000",
        "data": {
            "positions": positions,
            "hidden": hidden,
            "fetchedAt": fetched_at,
            "source": BINANCE_COPY_SOURCE,
            "positionsDisclosureVerified": True,
        },
    }


def persist_traders(rows: list[dict]) -> int:
    saved = 0
    with SessionLocal() as db:
        for item in rows:
            trader_id = str(item.get("traderId") or item.get("portfolioId") or item.get("id") or "").strip()
            if not trader_id:
                continue
            trader = db.get(Trader, trader_id)
            if trader is None:
                trader = Trader(trader_id=trader_id)
                db.add(trader)
            previous = parse_json(trader.raw_json)
            if previous.get("source") != item.get("source"):
                trader.visibility = "not_checked"
            trader.name = item.get("name") or item.get("nickname") or trader.name
            trader.avatar_url = item.get("avatarUrl") or trader.avatar_url
            trader.portfolio_type = item.get("type") or item.get("portfolioType") or trader.portfolio_type or "PUBLIC"
            trader.roi = safe_float(item.get("roi"))
            trader.pnl = safe_float(item.get("pnl"))
            trader.aum = safe_float(item.get("aum"))
            trader.max_drawdown = safe_float(item.get("maxDrawdown"))
            trader.win_rate = safe_float(item.get("winRate"))
            trader.copier_pnl = safe_float(item.get("copierPnl"))
            trader.sharpe_ratio = safe_float(item.get("sharpeRatio"))
            trader.raw_json = json.dumps(item, separators=(",", ":"), default=str)
            trader.last_seen_at = now()
            saved += 1
        db.commit()
    return saved

def record_position_snapshot(trader_id: str, payload: dict) -> tuple[int, bool]:
    data = unwrap_data(payload)
    positions = data.get("positions") or []
    positions_complete = isinstance(data.get("positions"), list)
    hidden = bool(data.get("hidden", False))
    fetched_at = data.get("fetchedAt") or data.get("fetched_at")
    saved = 0
    with SessionLocal() as db:
        trader = db.get(Trader, trader_id)
        if trader is None:
            trader = Trader(trader_id=trader_id, name=trader_id)
            db.add(trader)
        trader.visibility = "hidden_positions" if hidden else ("public_positions" if positions else "visible_empty_or_unreported")
        snapshot = PositionSnapshot(
            trader_id=trader_id,
            source_fetched_at=str(fetched_at) if fetched_at else None,
            hidden=hidden,
            position_count=len(positions),
            raw_json=json.dumps(payload, separators=(",", ":"), default=str)
        )
        db.add(snapshot)
        db.flush()
        if not hidden:
            for position in positions:
                symbol = str(position.get("symbol") or "UNKNOWN").upper()
                side = str(position.get("side") or "UNKNOWN").upper()
                position_id = str(position.get("positionId") or "")
                key = f"{position_id}|{symbol}|{side}" if position_id else f"{symbol}|{side}"
                db.add(PositionObservation(
                    snapshot_id=snapshot.id,
                    trader_id=trader_id,
                    position_key=key,
                    symbol=symbol,
                    side=side,
                    size=safe_float(position.get("size")),
                    entry_price=safe_float(position.get("entryPrice")),
                    mark_price=safe_float(position.get("markPrice")),
                    unrealized_pnl=safe_float(position.get("unrealizedPnl")),
                    leverage=safe_float(position.get("leverage")),
                    margin_mode=position.get("marginMode"),
                    liquidation_price=safe_float(position.get("liquidationPrice")),
                    roi=safe_float(position.get("roi")),
                    raw_json=json.dumps(position, separators=(",", ":"), default=str)
                ))
                saved += 1
            # A visible snapshot that omits an exact leader position is evidence of
            # possible closure. Require two consecutive visible omissions; hidden data
            # is never interpreted as a close.
            observed_keys = {
                (f"{str(p.get('positionId'))}|{str(p.get('symbol') or 'UNKNOWN').upper()}|{str(p.get('side') or 'UNKNOWN').upper()}"
                 if p.get("positionId") else f"{str(p.get('symbol') or 'UNKNOWN').upper()}|{str(p.get('side') or 'UNKNOWN').upper()}")
                for p in positions
            }
            for paper in (db.scalars(select(PaperTrade).where(
                PaperTrade.trader_id == trader_id, PaperTrade.status == "OPEN"
            )).all() if positions_complete else []):
                exact_key = paper.source_key.split("|", 1)[1] if "|" in paper.source_key else ""
                raw_trade = parse_json(paper.raw_json)
                # Do not interpret the first Binance-page snapshot as proof that a
                # legacy Kopyon-sourced paper position closed: their keys differ.
                if raw_trade.get("leader_source") not in {BINANCE_COPY_SOURCE, "BINANCE_PUBLIC_COPY_TRADING"}:
                    continue
                if exact_key in observed_keys:
                    raw_trade["leader_missing_visible_snapshots"] = 0
                    raw_trade["leader_status"] = "OPEN_OBSERVED"
                else:
                    missing = int(raw_trade.get("leader_missing_visible_snapshots", 0)) + 1
                    raw_trade["leader_missing_visible_snapshots"] = missing
                    raw_trade["leader_status"] = "MISSING_FROM_VISIBLE_SNAPSHOT"
                    if missing >= 2:
                        paper.status = "CLOSED"
                        paper.exit_price = paper.mark_price
                        paper.exit_reason = "LEADER_POSITION_CLOSED"
                        paper.closed_at = now()
                        db.add(PaperTradeEvent(
                            paper_trade_id=paper.id, event_type="LEADER_EXIT",
                            observed_at=paper.closed_at, value=paper.net_pnl,
                            detail="Exact leader position absent from two consecutive visible public Copy Trading snapshots"
                        ))
                paper.raw_json = json.dumps(raw_trade, separators=(",", ":"), default=str)
        db.commit()
    # A hidden or empty response is not evidence of a close; old observations remain intact.
    return saved, hidden

async def discover_traders() -> tuple[int, list[str]]:
    """Discover Binance copy portfolios from Kopyon v5 when configured, else public pages."""
    all_rows: list[dict[str, Any]] = []
    qualified_ids: list[str] = []
    seen_ids: set[str] = set()

    for page in range(1, TRADER_DISCOVERY_PAGES + 1):
        if KOPYON_ACTIVE:
            payload, _ = await kopyon_get("binance-copy-trading/traders", {
                "period": "30D", "sortBy": "pnl", "order": "DESC", "type": "PUBLIC",
                "hideFull": "false", "page": page, "rows": 20,
            })
            data = payload.get("data") or {}
            rows = data.get("traders") or []
        else:
            body = {
            "pageNumber": page, "pageSize": 20, "PAGE_SIZE": 20,
            "timeRange": "30D", "dataType": "PNL", "favoriteOnly": False,
            "hideFull": False, "nickname": "", "order": "DESC", "userAsset": 0,
            "portfolioType": "ALL", "useAiRecommended": False,
            }
            payload, _ = await binance_copy_post("home-page/query-list", body)
            data = payload.get("data") or {}
            rows = data.get("list") or []
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized = normalize_kopyon_leader_row(row) if KOPYON_ACTIVE else normalize_binance_leader_row(row)
            trader_id = normalized["traderId"]
            if not trader_id or trader_id in seen_ids:
                continue
            seen_ids.add(trader_id)
            all_rows.append(normalized)
            if is_qualified_binance_leader(normalized):
                qualified_ids.append(trader_id)

        total = int(data.get("total") or 0)
        has_more = bool(data.get("hasMore")) if KOPYON_ACTIVE else (total > 0 and page * 20 < total)
        if len(rows) < 20 or not has_more:
            break

    gate_failures: dict[str, int] = {}
    for row in all_rows:
        for gate, passed in (row.get("qualityGates") or {}).items():
            if not passed:
                gate_failures[gate] = gate_failures.get(gate, 0) + 1
    last_discovery_stats.update({"candidates": len(all_rows), "qualified": len(qualified_ids), "gate_failures": gate_failures})
    persist_traders(all_rows)
    return len(qualified_ids), qualified_ids

async def sync_one_trader_positions(trader_id: str) -> tuple[int, bool]:
    if KOPYON_ACTIVE:
        payload, _ = await kopyon_get("binance-copy-trading/openPositions", {"traderId": trader_id})
    else:
        payload, _ = await binance_copy_get("lead-data/positions", {"portfolioId": trader_id})
    normalized = normalize_binance_positions(payload)
    return record_position_snapshot(trader_id, normalized)

async def sync_trader_profile_if_stale(trader_id: str, max_age_hours: int = 24) -> bool:
    with SessionLocal() as db:
        cache = db.get(TraderProfileCache, trader_id)
        fetched_at = cache.fetched_at if cache else None
        if fetched_at is not None and fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        stale = fetched_at is None or (now() - fetched_at).total_seconds() >= max_age_hours * 3600
    if not stale:
        return False
    await load_trader_intelligence(trader_id)
    return True


def latest_snapshot_meta() -> dict[str, dict]:
    """Latest Binance-source snapshot per trader: hidden flag, position count, age in seconds."""
    with SessionLocal() as db:
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).where(
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).group_by(PositionSnapshot.trader_id).subquery()
        rows = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
    return {
        row.trader_id: {
            "hidden": bool(row.hidden), "count": int(row.position_count or 0),
            "age": max(0.0, (now() - (as_utc(row.observed_at) or now())).total_seconds()),
        } for row in rows
    }


async def refresh_leader_position_batch(candidate_ids: list[str] | None = None) -> dict:
    """Refresh a bounded rotating batch; prioritize open paper leaders."""
    with SessionLocal() as db:
        rows = db.scalars(select(Trader).order_by(desc(Trader.copier_pnl), desc(Trader.last_seen_at))).all()
        cutoff = now() - timedelta(seconds=max(7200, DISCOVERY_INTERVAL * 2))
        eligible = []
        for row in rows:
            raw = parse_json(row.raw_json)
            last_seen = row.last_seen_at
            if last_seen is not None and last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)
            if not is_qualified_binance_leader(raw):
                continue
            if KOPYON_ACTIVE and (raw.get("provider") != KOPYON_SOURCE or last_seen is None or last_seen < cutoff):
                continue
            eligible.append(row.trader_id)
        open_ids = sorted({
            trade.trader_id for trade in db.scalars(
                select(PaperTrade).where(PaperTrade.status == "OPEN")
            ).all()
            if parse_json(trade.raw_json).get("leader_source") in {BINANCE_COPY_SOURCE, "BINANCE_PUBLIC_COPY_TRADING"}
        })
    if KOPYON_ACTIVE:
        # Spend the quota where it matters: open paper trades first, then leaders that actually hold
        # live positions, then never-checked leaders. Leaders with no open positions are re-checked
        # hourly and leaders hiding positions every 6 hours.
        meta = latest_snapshot_meta()
        live_ids, new_ids, idle_ids = [], [], []
        for tid in dict.fromkeys([str(item) for item in (candidate_ids or [])] + eligible):
            info = meta.get(tid)
            if info is None:
                new_ids.append(tid)
            elif info["hidden"]:
                if info["age"] >= 6 * 3600:
                    idle_ids.append(tid)
            elif info["count"] > 0:
                live_ids.append(tid)
            elif info["age"] >= 3600:
                idle_ids.append(tid)
        pool = list(dict.fromkeys(open_ids + live_ids + new_ids + idle_ids))
        cursor_key = "kopyon_position_rotation_cursor"
    else:
        pool = list(dict.fromkeys([str(item) for item in (candidate_ids or [])] + eligible))
        cursor_key = "binance_position_rotation_cursor"
    cursor = int(setting(cursor_key, 0) or 0)
    rotated = [pool[(cursor + offset) % len(pool)] for offset in range(min(POSITION_BATCH, len(pool)))] if pool else []
    selected = rotated if KOPYON_ACTIVE else list(dict.fromkeys(open_ids + rotated))[:POSITION_BATCH]
    if pool:
        set_setting(cursor_key, (cursor + max(1, len(rotated))) % len(pool))
    checked = saved = 0
    for index, trader_id in enumerate(selected):
        if index and KOPYON_ACTIVE:
            await asyncio.sleep(0.4)  # spread the burst; stay well under per-minute limits
        try:
            count, hidden = await sync_one_trader_positions(trader_id)
            checked += 1
            saved += count
            log_check("binance_leader_positions", True, 200, None, f"{trader_id}: hidden={hidden}, active_positions={count}")
        except Exception as exc:
            log_check("binance_leader_positions", False, None, None, f"{trader_id}: {str(exc)[:600]}")
    return {"positions_checked": checked, "observations_saved": saved, "selected_trader_ids": selected}


async def run_monitor_cycle(force: bool = False) -> dict:
    global last_manual_sync
    async with scan_lock:
        last_scan.update({"state": "running", "started_at": now().isoformat(), "last_error": None})
        try:
            trader_count, discovered_ids = await discover_traders()
            refreshed = await refresh_leader_position_batch(discovered_ids)
            selected = refreshed["selected_trader_ids"]
            checked = refreshed["positions_checked"]
            saved = refreshed["observations_saved"]
            profiles_loaded = 0
            try:
                opportunities = []
                for strategy in ("RECOVERY", "MOMENTUM"):
                    opportunities.extend(await scan_paper_opportunities(
                        limit=2000, allow_entry_creation=True, strategy=strategy
                    ))
                qualified_count = sum(1 for item in opportunities if item.get("qualified"))
                market_blocked = sum(1 for item in opportunities if any("live Binance" in reason for reason in item.get("dead_market_reasons", [])))
                log_check("paper_analysis", True, 200, None, f"observed={len(opportunities)} qualified={qualified_count} live_market_warmup_or_blocked={market_blocked}")
            except Exception as analysis_exc:
                log_check("paper_analysis", False, None, None, str(analysis_exc))
                opportunities = []
                qualified_count = 0
            await evaluate_open_paper_trades()
            last_scan.update({
                "state": "idle",
                "finished_at": now().isoformat(),
                "last_error": None,
                "traders_found": trader_count,
                "candidates_found": last_discovery_stats["candidates"],
                "gate_failures": dict(last_discovery_stats["gate_failures"]),
                "positions_checked": checked,
                "observations_saved": saved,
                "profiles_loaded": profiles_loaded,
                "opportunities_analyzed": len(opportunities),
                "qualified_opportunities": qualified_count,
                "selected_trader_ids": selected
            })
            last_manual_sync = time.time()
            return dict(last_scan)
        except Exception as exc:
            last_scan.update({"state": "error", "finished_at": now().isoformat(), "last_error": str(exc)[:900]})
            log_check("monitor", False, None, None, str(exc))
            return dict(last_scan)

def prune_opportunity_decisions(retention_days: int = 90) -> int:
    cutoff = now() - timedelta(days=max(7, min(365, int(retention_days))))
    with SessionLocal() as db:
        result = db.execute(delete(OpportunityDecision).where(OpportunityDecision.observed_at < cutoff))
        db.commit()
        return int(result.rowcount or 0)


async def monitor_worker() -> None:
    global worker_alive
    worker_alive = True
    try:
        # Discovery is periodic; position refresh is independently rotated every five minutes.
        # Historical audits are deliberately not part of live monitoring.
        await run_monitor_cycle()
        last_discovery = time.monotonic()
        last_position_refresh = time.monotonic()
        last_decision_prune = time.monotonic() - 86400.0
        while True:
            await asyncio.sleep(60)
            if time.monotonic() - last_decision_prune >= 86400.0:
                try:
                    pruned = prune_opportunity_decisions(90)
                    log_check("opportunity_decision_retention", True, 200, None, f"pruned={pruned} retention_days=90")
                except Exception as prune_exc:
                    log_check("opportunity_decision_retention", False, None, None, str(prune_exc)[:500])
                last_decision_prune = time.monotonic()
            if time.monotonic() - last_position_refresh >= POSITION_REFRESH_INTERVAL:
                try:
                    refreshed = await refresh_leader_position_batch()
                    log_check("kopyon_leader_rotation" if KOPYON_ACTIVE else "binance_leader_rotation", True, 200, None, f"checked={refreshed['positions_checked']} saved={refreshed['observations_saved']}")
                except Exception as refresh_exc:
                    log_check("kopyon_leader_rotation" if KOPYON_ACTIVE else "binance_leader_rotation", False, None, None, str(refresh_exc)[:700])
                last_position_refresh = time.monotonic()
            try:
                opportunities = []
                for strategy in ("RECOVERY", "MOMENTUM"):
                    opportunities.extend(await scan_paper_opportunities(
                        limit=2000, allow_entry_creation=True, strategy=strategy
                    ))
                qualified_count = sum(1 for item in opportunities if item.get("qualified"))
                log_check("paper_analysis", True, 200, None, f"minute_recheck observed={len(opportunities)} qualified={qualified_count}")
            except Exception as exc:
                log_check("paper_analysis", False, None, None, f"minute_recheck: {str(exc)[:700]}")
            if time.monotonic() - last_discovery >= DISCOVERY_INTERVAL:
                await run_monitor_cycle()
                last_discovery = time.monotonic()
    except asyncio.CancelledError:
        raise
    finally:
        worker_alive = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Live monitoring starts separately; historical recovery audits are manual only.
    task = asyncio.create_task(monitor_worker())
    price_task = asyncio.create_task(paper_price_worker())
    app.state.monitor_task = task
    app.state.paper_price_task = price_task
    yield
    for background_task in (task, price_task):
        background_task.cancel()
    for background_task in (task, price_task):
        with suppress(asyncio.CancelledError):
            await background_task

app = FastAPI(title=APP_NAME, version="1.0.0", lifespan=lifespan, docs_url="/api/docs")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "app_name": APP_NAME})

def db_metrics() -> dict:
    used = None
    tables = []
    connection = "connected"
    try:
        with engine.connect() as conn:
            if engine.dialect.name == "postgresql":
                used = int(conn.execute(text("SELECT pg_database_size(current_database())")).scalar_one())
                rows = conn.execute(text(
                    "SELECT relname, pg_total_relation_size(relid) AS bytes "
                    "FROM pg_catalog.pg_statio_user_tables ORDER BY bytes DESC"
                )).all()
                tables = [{"name": row[0], "bytes": int(row[1])} for row in rows]
            else:
                path = engine.url.database
                if path and os.path.exists(path):
                    used = os.path.getsize(path)
                rows = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")).all()
                tables = [{"name": row[0], "bytes": None} for row in rows]
    except Exception as exc:
        connection = "error: " + str(exc)[:250]
    return {
        "engine": engine.dialect.name,
        "connection": connection,
        "persistent": engine.dialect.name == "postgresql",
        "used_bytes": used,
        "used_mb": round(used / 1024 / 1024, 3) if used is not None else None,
        "configured_capacity_gb": safe_float(os.getenv("DATABASE_CAPACITY_GB")),
        "capacity_note": "Render plan quota is not exposed through the SQL connection; showing actual database bytes only." if os.getenv("DATABASE_CAPACITY_GB") is None else "Capacity is an operator-supplied environment value.",
        "tables": tables
    }

def kopyon_call_stats() -> dict:
    """Kopyon API calls made, counted from the persisted per-call audit rows (survives restarts)."""
    if not KOPYON_ACTIVE:
        return {"active": False}
    with SessionLocal() as db:
        base = select(func.count()).select_from(SourceCheck).where(SourceCheck.source == "kopyon_api")
        total = db.scalar(base) or 0
        day = db.scalar(base.where(SourceCheck.checked_at >= now() - timedelta(hours=24))) or 0
        month = db.scalar(base.where(SourceCheck.checked_at >= now() - timedelta(days=30))) or 0
        failed = db.scalar(base.where(SourceCheck.ok == False)) or 0  # noqa: E712
    lim = KOPYON_LIMIT_STATUS
    return {"active": True, "total": total, "last_24h": day, "last_30d": month, "failed": failed,
            "limit_month": lim.get("limit_month"), "remaining_month": lim.get("remaining_month")}


def overview_counts() -> dict:
    with SessionLocal() as db:
        traders = db.scalar(select(func.count()).select_from(Trader).where(
            Trader.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%'),
            Trader.raw_json.like('%"qualityQualified":true%'),
            Trader.visibility.in_(["public_positions", "visible_empty_or_unreported"])
        )) or 0
        snapshots = db.scalar(select(func.count()).select_from(PositionSnapshot)) or 0
        observations = db.scalar(select(func.count()).select_from(PositionObservation)) or 0
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).where(
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).group_by(PositionSnapshot.trader_id).subquery()
        latest_rows = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
        latest_rows = [row for row in latest_rows if is_binance_snapshot(row)]
        fresh_cutoff = now() - timedelta(seconds=900)
        latest_hidden = sum(1 for row in latest_rows if row.hidden)
        latest_visible = [row for row in latest_rows if not row.hidden]
        latest_stale = sum(1 for row in latest_visible if (as_utc(row.observed_at) or now()) < fresh_cutoff)
        latest_fresh_ids = [row.id for row in latest_visible if (as_utc(row.observed_at) or now()) >= fresh_cutoff]
        live_rows = db.scalars(select(PositionObservation).where(PositionObservation.snapshot_id.in_(latest_fresh_ids))).all() if latest_fresh_ids else []
        live_positions = len(live_rows)
        underwater_live = 0
        for row in live_rows:
            adverse = None
            if row.entry_price and row.mark_price and row.entry_price > 0:
                adverse = ((row.entry_price - row.mark_price) / row.entry_price * 100) if row.side == "LONG" else ((row.mark_price - row.entry_price) / row.entry_price * 100)
            if (row.unrealized_pnl is not None and row.unrealized_pnl < 0) or (adverse is not None and adverse > 0):
                underwater_live += 1
        open_paper = db.scalar(select(func.count()).select_from(PaperTrade).where(PaperTrade.status == "OPEN")) or 0
        closed_paper = db.scalar(select(func.count()).select_from(PaperTrade).where(PaperTrade.status == "CLOSED")) or 0
        latest = max((as_utc(row.observed_at) for row in latest_rows if row.observed_at), default=None)
        qualified_ids = {
            t.trader_id for t in db.scalars(select(Trader).where(
                Trader.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%'),
                Trader.raw_json.like('%"qualityQualified":true%')
            )).all()
        }
        q_rows = [row for row in latest_rows if row.trader_id in qualified_ids]
        q_hidden = sum(1 for row in q_rows if row.hidden)
        q_live_rows = [row for row in q_rows if not row.hidden and row.position_count and (as_utc(row.observed_at) or now()) >= fresh_cutoff]
        scanner_summary = {
            "qualified_leaders": len(qualified_ids),
            "leaders_position_checked": len(q_rows),
            "leaders_with_live_positions": len(q_live_rows),
            "live_positions": sum(int(row.position_count or 0) for row in q_live_rows),
            "leaders_hidden_excluded": q_hidden,
            "leaders_no_open_positions": sum(1 for row in q_rows if not row.hidden and not row.position_count),
            "leaders_not_checked_yet": len(qualified_ids) - len(q_rows),
        }
    return {
        "scanner_summary": scanner_summary,
        "kopyon_calls": kopyon_call_stats(),
        "traders": traders, "snapshots": snapshots, "observations": observations,
        "live_positions": live_positions, "underwater_live_positions": underwater_live,
        "latest_trader_snapshots_hidden": latest_hidden, "hidden_snapshots": latest_hidden,
        "stale_visible_trader_snapshots": latest_stale,
        "open_paper_trades": open_paper, "closed_paper_trades": closed_paper,
        "underwater_observations": underwater_live,
        "latest_snapshot": latest.isoformat() if latest else None
    }


@app.get("/healthz")
async def healthz():
    try:
        with SessionLocal() as db:
            db.execute(select(func.count()).select_from(Trader)).scalar_one()
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else "unavailable",
        "binance_public_copy_trading_source": True,
        "binance_rest_limiter": binance_rest_limiter.status(),
        "automatic_monitor": worker_alive,
        "monitor_state": last_scan.get("state"),
        "paper_only": True,
        "order_execution": False
    }

@app.get("/api/overview")
async def api_overview():
    db = db_metrics()
    counts = overview_counts()
    with SessionLocal() as session:
        checks = session.scalars(select(SourceCheck).order_by(desc(SourceCheck.checked_at)).limit(12)).all()
    copy_source_check = "kopyon_api" if KOPYON_ACTIVE else "binance_copy_trading_page"
    copy_check = next(({"ok": check.ok, "http_status": check.http_status,
                        "detail": check.detail, "checked_at": check.checked_at.isoformat()}
                       for check in checks if check.source == copy_source_check), None)
    return {
        **counts,
        "database": db,
        "binance_copy_trading": {"base": KOPYON_BASE_URL if KOPYON_ACTIVE else BINANCE_COPY_ROOT,
                                 "provider": "Kopyon v5" if KOPYON_ACTIVE else "Binance public Copy Trading website",
                                 "source": "Kopyon normalized Binance public Copy Trading data" if KOPYON_ACTIVE else "Public Binance Futures Copy Trading website data",
                                 "kopyon_key_configured": KOPYON_ACTIVE,
                                 "kopyon_limits": dict(KOPYON_LIMIT_STATUS) if KOPYON_ACTIVE else None,
                                 "last_status": copy_check},
        "binance": {"base": BINANCE_ROOT, "role": "Public USDⓈ-M Futures market data; the WebSocket is primary for live mark prices."},
        "monitor": {**last_scan, "worker_alive": worker_alive, "discovery_interval_seconds": DISCOVERY_INTERVAL, "position_refresh_interval_seconds": POSITION_REFRESH_INTERVAL, "position_batch": POSITION_BATCH,
                    "quota_design": (f"Kopyon v5 REST; 1 leaderboard call per {DISCOVERY_INTERVAL}s (up to 20 leaders) plus {POSITION_BATCH} per-leader openPositions calls every {POSITION_REFRESH_INTERVAL}s (~{int(2592000 / DISCOVERY_INTERVAL * TRADER_DISCOVERY_PAGES + 2592000 / POSITION_REFRESH_INTERVAL * POSITION_BATCH):,} requests per 30 days before on-demand profiles)" if KOPYON_ACTIVE else f"All Binance HTTP calls share a {BINANCE_REST_WEIGHT_BUDGET}-weight rolling 60-second budget; leader profiles/history are loaded only when requested and cached for 24 hours.")},
        "paper": paper_config(),
        "paper_fast_price_monitor": True,
        "price_feed": {
            "status": PAPER_PRICE_STREAM_STATE.get("status"),
            "connected": bool(PAPER_PRICE_STREAM_STATE.get("connected")),
            "active_symbols": len(PAPER_PRICE_STREAM_STATE.get("subscribed_symbols") or []),
            "last_price_at": PAPER_PRICE_STREAM_STATE.get("last_price_at"),
            "last_error": PAPER_PRICE_STREAM_STATE.get("last_error"),
            "binance_rest": binance_rest_status()
        },
        "execution_enabled": False,
        "runtime_note": "On Render Free, the web service can spin down after 15 minutes without inbound traffic. The in-process monitor runs only while the instance is awake."
    }

@app.get("/api/traders")
async def api_traders(limit: int = 100, search: str = ""):
    limit = max(1, min(limit, 300))
    with SessionLocal() as db:
        query = select(Trader).where(
            Trader.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%'),
            Trader.raw_json.like('%"qualityQualified":true%')
        ).where(
            Trader.visibility.in_(["public_positions", "visible_empty_or_unreported"])
        ).order_by(desc(Trader.copier_pnl), desc(Trader.last_seen_at))
        q = search.lower().strip()
        if q:
            pattern = f"%{q}%"
            query = query.where((Trader.name.ilike(pattern)) | (Trader.trader_id.ilike(pattern)))
        rows = db.scalars(query.limit(limit)).all()
        cache = {p.trader_id: p for p in db.scalars(select(TraderProfileCache)).all()}
    meta = latest_snapshot_meta()
    rows = sorted(rows, key=lambda r: 0 if (meta.get(r.trader_id) or {}).get("count", 0) > 0 else 1)
    result = []
    for t in rows:
        profile_cache = cache.get(t.trader_id)
        profile = unwrap_data(parse_json(profile_cache.raw_profile)) if profile_cache else {}
        src = parse_json(t.raw_json)
        result.append({
            "trader_id": t.trader_id, "name": t.name, "avatar_url": t.avatar_url,
            "portfolio_type": t.portfolio_type, "visibility": t.visibility,
            "live_positions": (meta.get(t.trader_id) or {}).get("count", 0),
            "binance_url": f"https://www.binance.com/en/copy-trading/lead-details/{t.trader_id}",
            "roi": t.roi, "pnl": t.pnl, "aum": t.aum, "max_drawdown": t.max_drawdown,
            "win_rate": t.win_rate, "copier_pnl": t.copier_pnl, "sharpe_ratio": t.sharpe_ratio,
            "copiers": src.get("copiers"), "max_copiers": src.get("maxCopiers"),
            "bio": profile.get("bio"), "profit_share": profile.get("profitShare"),
            "last_seen_at": t.last_seen_at.isoformat(), "profile_loaded": bool(profile_cache)
        })
    return result

@app.get("/api/traders/{trader_id}")
async def api_trader_detail(trader_id: str):
    with SessionLocal() as db:
        trader = db.get(Trader, trader_id)
    if trader is None or not is_qualified_binance_leader(parse_json(trader.raw_json)) or trader.visibility not in {"public_positions", "visible_empty_or_unreported"}:
        raise HTTPException(404, "Trader not found in the live discovered universe")
    try:
        await sync_trader_profile_if_stale(trader_id, max_age_hours=24)
    except Exception as exc:
        log_check("kopyon_trader_profile" if KOPYON_ACTIVE else "binance_trader_profile", False, None, None, f"{trader_id}: {str(exc)[:500]}")
    with SessionLocal() as db:
        trader = db.get(Trader, trader_id)
        cache = db.get(TraderProfileCache, trader_id)
        history = db.scalars(select(HistoryPosition).where(
            HistoryPosition.trader_id == trader_id,
            HistoryPosition.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).order_by(desc(HistoryPosition.opened_at_ms)).limit(100)).all()
    durations = [((h.closed_at_ms - h.opened_at_ms) / 3600000) for h in history if h.opened_at_ms and h.closed_at_ms and h.closed_at_ms > h.opened_at_ms]
    pnl_values = [h.gross_pnl for h in history if h.gross_pnl is not None]
    wins = sum(1 for x in pnl_values if x > 0)
    profile = unwrap_data(parse_json(cache.raw_profile)) if cache else {}
    performance = unwrap_data(parse_json(cache.raw_performance)) if cache else {}
    return {
        "trader": {
            "trader_id": trader.trader_id, "name": trader.name, "avatar_url": trader.avatar_url,
            "portfolio_type": trader.portfolio_type, "roi": trader.roi, "pnl": trader.pnl,
            "aum": trader.aum, "max_drawdown": trader.max_drawdown, "win_rate": trader.win_rate,
            "copier_pnl": trader.copier_pnl, "sharpe_ratio": trader.sharpe_ratio,
            "visibility": trader.visibility, "raw": parse_json(trader.raw_json)
        },
        "profile": profile, "performance": performance,
        "history_summary": {
            "records": len(history), "win_rate_closed_history": wins / len(pnl_values) if pnl_values else None,
            "avg_hold_hours": statistics.mean(durations) if durations else None,
            "median_hold_hours": statistics.median(durations) if durations else None,
            "avg_reported_pnl": statistics.mean(pnl_values) if pnl_values else None,
            "avg_net_pnl": statistics.mean(pnl_values) if pnl_values else None
        },
        "history": [history_item(h) for h in history[:40]],
        "profile_fetched_at": cache.fetched_at.isoformat() if cache else None
    }

def history_item(h: HistoryPosition) -> dict:
    return {
        "position_id": h.position_id, "symbol": h.symbol, "side": h.side, "status": h.status,
        "opened_at": ms_datetime(h.opened_at_ms).isoformat() if h.opened_at_ms else None,
        "closed_at": ms_datetime(h.closed_at_ms).isoformat() if h.closed_at_ms else None,
        "entry_price": h.entry_price, "close_price": h.close_price, "roi": h.roi,
        "gross_pnl": h.gross_pnl, "net_pnl": h.net_pnl
    }

@app.post("/api/traders/{trader_id}/intel")
async def load_trader_intelligence(trader_id: str):
    with SessionLocal() as db:
        trader = db.get(Trader, trader_id)
    if trader is None or not is_qualified_binance_leader(parse_json(trader.raw_json)) or trader.visibility not in {"public_positions", "visible_empty_or_unreported"}:
        raise HTTPException(404, "Trader not found")
    results = {}
    try:
        if KOPYON_ACTIVE:
            profile_payload, _ = await kopyon_get("binance-copy-trading/traderProfile", {"traderId": trader_id})
            performance_payload, _ = await kopyon_get("binance-copy-trading/performance", {"traderId": trader_id, "period": "30D"})
            history_payload, _ = await kopyon_get("binance-copy-trading/positionHistory", {"traderId": trader_id, "rows": 20, "sortBy": "opened"})
        else:
            profile_payload, _ = await binance_copy_get("lead-portfolio/detail", {"portfolioId": trader_id})
            performance_payload = profile_payload
            history_payload, _ = await binance_copy_post("lead-portfolio/position-history", {
                "pageNumber": 1, "pageSize": 100, "portfolioId": trader_id, "sort": "OPENING"
            })
        profile = unwrap_data(profile_payload)
        history_data = history_payload.get("data") or {}
        history_data = history_data if isinstance(history_data, dict) else {}
        history_rows = (history_data.get("positions") or []) if KOPYON_ACTIVE else (history_data.get("list") or [])
        with SessionLocal() as db:
            cache = db.get(TraderProfileCache, trader_id)
            if cache is None:
                cache = TraderProfileCache(trader_id=trader_id)
                db.add(cache)
            cache.fetched_at = now()
            cache.raw_profile = json.dumps(profile_payload, separators=(",", ":"), default=str)
            cache.raw_performance = json.dumps(performance_payload, separators=(",", ":"), default=str)
            for item in history_rows:
                pid = str(item.get("positionId") or item.get("id") or "")
                exists = db.scalar(select(HistoryPosition.id).where(
                    HistoryPosition.trader_id == trader_id,
                    HistoryPosition.position_id == (pid or None)
                ).limit(1)) if pid else None
                if exists:
                    continue
                db.add(HistoryPosition(
                    trader_id=trader_id, position_id=pid or None,
                    symbol=str(item.get("symbol") or "UNKNOWN").upper(),
                    side=str(item.get("side") or "UNKNOWN").upper(),
                    status=item.get("status"),
                    opened_at_ms=timestamp_ms(item.get("opened") or item.get("openedAt") or item.get("openTime")),
                    closed_at_ms=timestamp_ms(item.get("closed") or item.get("closedAt") or item.get("closeTime")),
                    entry_price=safe_float(item.get("avgCost") or item.get("entryPrice")),
                    close_price=safe_float(item.get("avgClosePrice") or item.get("closePrice")),
                    roi=safe_float(item.get("roi")),
                    gross_pnl=safe_float(item.get("closingPnl") if "closingPnl" in item else item.get("grossPnl")),
                    net_pnl=safe_float(item.get("netPnl")),
                    raw_json=json.dumps({"source": BINANCE_COPY_SOURCE, "provider": KOPYON_SOURCE if KOPYON_ACTIVE else "BINANCE_PUBLIC_COPY_TRADING", "data": item}, separators=(",", ":"), default=str)
                ))
            db.commit()
        results = {"profile_loaded": True, "history_rows_received": len(history_rows), "history_total": history_data.get("total"), "performance_loaded": True, "source": KOPYON_SOURCE if KOPYON_ACTIVE else BINANCE_COPY_SOURCE}
    except Exception as exc:
        raise HTTPException(502, f"Could not load trader intelligence: {str(exc)[:700]}")
    return results

@app.get("/api/opportunity-decisions")
async def api_opportunity_decisions(limit: int = 200, strategy: str = ""):
    limit = max(1, min(1000, int(limit)))
    query = select(OpportunityDecision).order_by(desc(OpportunityDecision.observed_at), desc(OpportunityDecision.id))
    if strategy.upper() in {"RECOVERY", "MOMENTUM"}:
        query = query.where(OpportunityDecision.strategy == strategy.upper())
    with SessionLocal() as db:
        rows = db.scalars(query.limit(limit)).all()
    return [{"id": row.id, "trader_id": row.trader_id, "snapshot_id": row.snapshot_id,
        "position_key": row.position_key, "strategy": row.strategy, "symbol": row.symbol,
        "side": row.side, "observed_at": row.observed_at.isoformat(), "qualified": row.qualified,
        "status": row.status, "execution_status": row.execution_status,
        "paper_trade_id": row.paper_trade_id, "reasons": parse_json(row.reasons_json, []),
        "checks": parse_json(row.checks_json, {})} for row in rows]


@app.get("/api/positions")
async def api_positions(limit: int = 500):
    limit = max(1, min(limit, 1000))
    with SessionLocal() as db:
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).where(
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).group_by(PositionSnapshot.trader_id).subquery()
        records = db.execute(
            select(PositionObservation, PositionSnapshot)
            .join(PositionSnapshot, PositionSnapshot.id == PositionObservation.snapshot_id)
            .join(latest_ids, latest_ids.c.id == PositionSnapshot.id)
            .where(
                PositionSnapshot.hidden.is_(False),
                PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
            )
            .order_by(desc(PositionObservation.id))
            .limit(limit * 2)
        ).all()
    latest = {}
    current_time = now()
    for obs, snap in records:
        key = f"{obs.trader_id}|{obs.position_key}"
        if key not in latest:
            observed_at = as_utc(snap.observed_at) or current_time
            age = max(0.0, (current_time - observed_at).total_seconds())
            latest[key] = {
                "trader_id": obs.trader_id, "symbol": obs.symbol, "side": obs.side, "position_key": obs.position_key,
                "size": obs.size, "entry_price": obs.entry_price, "mark_price": obs.mark_price,
                "unrealized_pnl": obs.unrealized_pnl, "leverage": obs.leverage, "margin_mode": obs.margin_mode,
                "liquidation_price": obs.liquidation_price, "roi": obs.roi,
                "observed_at": observed_at.isoformat(),
                "source_fetched_at": snap.source_fetched_at,
                "snapshot_age_seconds": round(age, 1),
                "freshness": "FRESH" if age <= 900 else "STALE",
                "hidden": False, "raw": parse_json(obs.raw_json)
            }
        if len(latest) >= limit:
            break
    return list(latest.values())


async def fetch_news_items(force: bool = False) -> list[dict]:
    if not force and time.time() - news_cache["fetched_at"] < 1800:
        return news_cache["items"]
    feed_errors = []
    feeds = [
        ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
        ("Cointelegraph", "https://cointelegraph.com/rss"),
        ("The Block", "https://www.theblock.co/rss.xml")
    ]
    items = []
    for source_name, url in feeds:
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                response = await client.get(url)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            for item in root.findall(".//item")[:60]:
                title = item.findtext("title") or ""
                link = item.findtext("link") or ""
                pub = item.findtext("pubDate") or ""
                if title and link:
                    items.append({"title": title, "url": link, "published_at": pub, "source": source_name})
        except Exception as exc:
            feed_errors.append(f"{source_name}: {str(exc)[:150]}")
    news_cache.update({"fetched_at": time.time(), "items": items, "error": "; ".join(feed_errors) or None})
    return items

def rsi_value(closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(x, 0) for x in diffs[-period:]]
    losses = [max(-x, 0) for x in diffs[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def live_reversal_features(symbol: str, live_price: float | None, live_fresh: bool) -> dict:
    """Short-horizon reversal evidence from the live WebSocket history (needs ~90s, not 5 minutes)."""
    history = PAPER_SYMBOL_PRICE_HISTORY.get(symbol, [])
    t = time.time()
    out: dict[str, Any] = {"history_span_seconds": 0.0, "momentum_1m_pct": None, "low_5m": None, "high_5m": None, "reversal_ready": False}
    if not history:
        return out
    out["history_span_seconds"] = round(t - history[0][0], 1)
    window = [price for stamp, price in history if stamp >= t - 300]
    if window:
        out["low_5m"], out["high_5m"] = min(window), max(window)
    base_1m = next((price for stamp, price in reversed(history) if stamp <= t - 60), None)
    if live_fresh and live_price and base_1m and base_1m > 0 and out["history_span_seconds"] >= 90 and window:
        out["momentum_1m_pct"] = (live_price / base_1m - 1) * 100
        out["reversal_ready"] = True
    return out


# REST guards for the six-endpoint static market refresh (~11 weight each).
MARKET_REST_FAILURE_BACKOFF_SECONDS = 900.0   # do not retry a failed symbol for 15 min
MARKET_REST_MIN_GAP_SECONDS = 12.0            # at most one full refresh per 12s (~55 weight/min, also capped by the global limiter)
MARKET_REST_MANUAL_REFRESH_MIN_AGE = 600.0    # ?refresh=true is ignored if cache is younger than 10 min
_market_rest_failed_until: dict[str, float] = {}
_market_rest_inflight: dict[str, "asyncio.Task"] = {}
_market_rest_gap_lock = asyncio.Lock()
_market_rest_last_started = 0.0


async def market_features(symbol: str, refresh: bool = False, allow_rest: bool = True, wait: bool = True) -> dict:
    with SessionLocal() as db:
        cache = db.get(MarketInsight, symbol)
        cache_age = (now() - (as_utc(cache.fetched_at) or now())).total_seconds() if cache else None
        # Static liquidity/volatility data is reused for up to 12h to avoid
        # repeatedly exhausting Binance REST. Mark price, funding and 5m
        # confirmation are refreshed from the live mark-price WebSocket.
        if refresh and cache_age is not None and cache_age < MARKET_REST_MANUAL_REFRESH_MIN_AGE:
            refresh = False
        if cache and not refresh and cache_age is not None and cache_age < 43200:
            cached = parse_json(cache.raw_json)
            cached_closes = cached.get("closes_1h") if isinstance(cached, dict) else None
            if isinstance(cached_closes, list) and len(cached_closes) >= 25 and cached.get("quote_volume_24h_usdt") is not None and cached.get("spread_pct") is not None and cached.get("min_depth_within_1pct_usdt") is not None and cached.get("volume_ratio_recent_6h_vs_prior_24h") is not None:
                live_at = PAPER_SYMBOL_LAST_PRICE_AT.get(symbol)
                live_fresh = bool(live_at and time.time() - live_at <= 10)
                live_price = PAPER_SYMBOL_LAST_PRICE.get(symbol) if live_fresh else None
                history = PAPER_SYMBOL_PRICE_HISTORY.get(symbol, [])
                cutoff = time.time() - 300
                baseline = next((price for stamp, price in history if stamp <= cutoff), None)
                momentum_fresh = bool(live_fresh and baseline and baseline > 0 and live_price and live_price > 0)
                cached["momentum_5m_pct"] = ((live_price / baseline) - 1) * 100 if momentum_fresh else None
                if live_fresh and live_price:
                    cached["mark_price"] = live_price
                    cached["funding_rate"] = PAPER_SYMBOL_FUNDING_RATE.get(symbol, cached.get("funding_rate"))
                    cached["index_price"] = PAPER_SYMBOL_INDEX_PRICE.get(symbol, cached.get("index_price"))
                cached["live_price_fresh"] = live_fresh
                cached["momentum_5m_fresh"] = momentum_fresh
                cached.update(live_reversal_features(symbol, live_price, live_fresh))
                cached["market_data_age_seconds"] = round(cache_age, 1)
                cached["fetched_at"] = cache.fetched_at.isoformat()
                cached["source"] = "Cached Binance liquidity/volatility + live WebSocket mark price"
                return cached
    queue_only = bool(allow_rest and not wait)
    if queue_only:
        # Never block the scanner on Binance REST: queue one paced background refresh and use cached data now.
        if (time.time() >= _market_rest_failed_until.get(symbol, 0.0) and time.time() >= BINANCE_REST_BLOCKED_UNTIL
                and symbol not in _market_rest_inflight):
            _start_market_rest_refresh(symbol)
    if not allow_rest or queue_only:
        cached = parse_json(cache.raw_json) if cache else {}
        cached = cached if isinstance(cached, dict) else {}
        live_at = PAPER_SYMBOL_LAST_PRICE_AT.get(symbol)
        live_fresh = bool(live_at and time.time() - live_at <= 10)
        live_price = PAPER_SYMBOL_LAST_PRICE.get(symbol) if live_fresh else safe_float(cached.get("mark_price"))
        history = PAPER_SYMBOL_PRICE_HISTORY.get(symbol, [])
        cutoff = time.time() - 300
        baseline = next((price for stamp, price in history if stamp <= cutoff), None)
        momentum_fresh = bool(live_fresh and baseline and baseline > 0 and live_price and live_price > 0)
        cached.update({
            "symbol": symbol, "mark_price": live_price,
            "momentum_5m_pct": ((live_price / baseline) - 1) * 100 if momentum_fresh else None,
            "momentum_5m_fresh": momentum_fresh, "live_price_fresh": live_fresh,
            "funding_rate": PAPER_SYMBOL_FUNDING_RATE.get(symbol, cached.get("funding_rate")),
            "static_market_data_status": ("REFRESH_QUEUED" if queue_only else "CACHED_STALE") if cache or queue_only else "PENDING",
            "market_data_age_seconds": round(cache_age, 1) if cache_age is not None else None,
            "source": "Read-only scanner: persisted Binance market insight + live WebSocket",
        })
        cached.update(live_reversal_features(symbol, live_price, live_fresh))
        return cached
    failed_until = _market_rest_failed_until.get(symbol, 0.0)
    if time.time() < failed_until:
        raise RuntimeError(f"Binance market refresh for {symbol} backed off for {failed_until - time.time():.0f}s after a failure")
    if time.time() < BINANCE_REST_BLOCKED_UNTIL:
        raise RuntimeError("Binance REST paused by circuit breaker")
    inflight = _market_rest_inflight.get(symbol) or _start_market_rest_refresh(symbol)
    return await asyncio.shield(inflight)


def _start_market_rest_refresh(symbol: str) -> "asyncio.Future":
    task = asyncio.ensure_future(_market_features_rest_guarded(symbol))
    _market_rest_inflight[symbol] = task

    def _done(t, _s=symbol):
        _market_rest_inflight.pop(_s, None)
        if not t.cancelled() and t.exception() is not None:  # retrieve so asyncio never logs it as unhandled
            log_check("binance_market_refresh", False, None, None, f"{_s}: {str(t.exception())[:300]}")
    task.add_done_callback(_done)
    return task


async def _market_features_rest_guarded(symbol: str) -> dict:
    try:
        return await _market_features_rest(symbol)
    except Exception:
        _market_rest_failed_until[symbol] = time.time() + MARKET_REST_FAILURE_BACKOFF_SECONDS
        raise


async def _market_features_rest(symbol: str) -> dict:
    global _market_rest_last_started
    async with _market_rest_gap_lock:
        wait = MARKET_REST_MIN_GAP_SECONDS - (time.monotonic() - _market_rest_last_started)
        if wait > 0:
            await asyncio.sleep(wait)
        _market_rest_last_started = time.monotonic()
    premium, _ = await binance_get("/fapi/v1/premiumIndex", {"symbol": symbol})
    ticker24, _ = await binance_get("/fapi/v1/ticker/24hr", {"symbol": symbol})
    book, _ = await binance_get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
    depth, _ = await binance_get("/fapi/v1/depth", {"symbol": symbol, "limit": 100})
    klines, _ = await binance_get("/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "limit": 168})
    micro, _ = await binance_get("/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 3})
    if not isinstance(klines, list):
        raise RuntimeError("Binance hourly klines response was not a list")
    if not isinstance(micro, list):
        raise RuntimeError("Binance 5-minute klines response was not a list")
    closes = [float(k[4]) for k in klines if isinstance(k, (list, tuple)) and len(k) > 4]
    highs = [float(k[2]) for k in klines if isinstance(k, (list, tuple)) and len(k) > 4]
    lows = [float(k[3]) for k in klines if isinstance(k, (list, tuple)) and len(k) > 4]
    hourly_volumes = [float(k[5]) for k in klines if isinstance(k, (list, tuple)) and len(k) > 5]
    recent_6h_volume = sum(hourly_volumes[-6:]) if len(hourly_volumes) >= 6 else None
    prior_24h_avg_hour = statistics.mean(hourly_volumes[-30:-6]) if len(hourly_volumes) >= 30 else None
    volume_ratio_6h = (recent_6h_volume / (prior_24h_avg_hour * 6)) if recent_6h_volume is not None and prior_24h_avg_hour and prior_24h_avg_hour > 0 else None
    last = closes[-1] if closes else safe_float(premium.get("markPrice"))
    seven_return = (closes[-1] / closes[0] - 1) * 100 if len(closes) >= 2 and closes[0] else None
    day_return = (closes[-1] / closes[-25] - 1) * 100 if len(closes) >= 25 and closes[-25] else None
    tr = []
    for i in range(1, len(closes)):
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr_pct = (statistics.mean(tr[-24:]) / last * 100) if tr and last else None
    micro_closes = [float(k[4]) for k in micro if isinstance(k, (list, tuple)) and len(k) > 4]
    mom5 = ((micro_closes[-1] / micro_closes[-2]) - 1) * 100 if len(micro_closes) >= 2 and micro_closes[-2] else None
    depth_mark = safe_float(premium.get("markPrice")) or last or 0.0
    bid_depth_1pct = sum((safe_float(level[0]) or 0) * (safe_float(level[1]) or 0) for level in (depth.get("bids") or []) if len(level) >= 2 and safe_float(level[0]) is not None and safe_float(level[0]) >= depth_mark * 0.99) if isinstance(depth, dict) else None
    ask_depth_1pct = sum((safe_float(level[0]) or 0) * (safe_float(level[1]) or 0) for level in (depth.get("asks") or []) if len(level) >= 2 and safe_float(level[0]) is not None and safe_float(level[0]) <= depth_mark * 1.01) if isinstance(depth, dict) else None
    min_depth_1pct = min(bid_depth_1pct or 0, ask_depth_1pct or 0) if bid_depth_1pct is not None and ask_depth_1pct is not None else None

    # Estimate execution for the maximum paper notional. This is deliberately
    # conservative: a paper signal is considered liquid only if the sampled
    # order book can absorb the whole simulated position inside 1% with a bounded
    # VWAP slippage. The calculation is not an order and does not reserve liquidity.
    paper_cfg = paper_config()
    paper_capital = max(0.0, float(paper_cfg.get("capital_usdt", 1000.0)))
    paper_max_notional = min(
        paper_capital * float(paper_cfg.get("max_margin_pct", 10.0)) / 100.0
        * float(paper_cfg.get("max_leverage", 3.0)),
        paper_capital * 0.25
    )
    def _book_execution(levels: Any, required_notional: float) -> dict:
        if not isinstance(levels, list) or required_notional <= 0:
            return {"covered": False, "filled_notional": 0.0, "vwap": None, "slippage_pct": None}
        remaining = required_notional
        filled = 0.0
        quote = 0.0
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            px = safe_float(level[0]); qty = safe_float(level[1])
            if not px or not qty or px <= 0 or qty <= 0:
                continue
            level_notional = px * qty
            take = min(remaining, level_notional)
            quote += take
            filled += take
            remaining -= take
            if remaining <= 1e-9:
                break
        vwap = quote / filled if filled > 0 else None
        return {
            "covered": remaining <= 1e-9,
            "filled_notional": filled,
            "vwap": vwap,
            "slippage_pct": ((vwap / depth_mark - 1) * 100 if vwap and depth_mark else None)
        }
    exec_side = "asks"  # populated below; build_opportunity evaluates both directions.
    long_exec = _book_execution(depth.get("asks") if isinstance(depth, dict) else None, paper_max_notional)
    short_exec = _book_execution(depth.get("bids") if isinstance(depth, dict) else None, paper_max_notional)
    max_exec_slippage = max(
        safe_float(long_exec.get("slippage_pct")) or 999.0,
        safe_float(short_exec.get("slippage_pct")) or 999.0
    )

    data = {
        "symbol": symbol, "fetched_at": now().isoformat(),
        "mark_price": safe_float(premium.get("markPrice")),
        "index_price": safe_float(premium.get("indexPrice")),
        "funding_rate": safe_float(premium.get("lastFundingRate")),
        "next_funding_time": premium.get("nextFundingTime"),
        "quote_volume_24h_usdt": safe_float(ticker24.get("quoteVolume")) if isinstance(ticker24, dict) else None,
        "volume_ratio_recent_6h_vs_prior_24h": volume_ratio_6h,
        "base_volume_24h": safe_float(ticker24.get("volume")) if isinstance(ticker24, dict) else None,
        "best_bid": safe_float(book.get("bidPrice")) if isinstance(book, dict) else None,
        "best_ask": safe_float(book.get("askPrice")) if isinstance(book, dict) else None,
        "spread_pct": (((safe_float(book.get("askPrice")) or 0) - (safe_float(book.get("bidPrice")) or 0)) / max(safe_float(premium.get("markPrice")) or 1, 1e-12) * 100) if isinstance(book, dict) else None,
        "bid_depth_within_1pct_usdt": bid_depth_1pct,
        "ask_depth_within_1pct_usdt": ask_depth_1pct,
        "min_depth_within_1pct_usdt": min_depth_1pct,
        "paper_max_notional_usdt": paper_max_notional,
        "paper_long_book_covered": bool(long_exec.get("covered")),
        "paper_short_book_covered": bool(short_exec.get("covered")),
        "paper_long_book_filled_notional_usdt": long_exec.get("filled_notional"),
        "paper_short_book_filled_notional_usdt": short_exec.get("filled_notional"),
        "paper_long_vwap": long_exec.get("vwap"),
        "paper_short_vwap": short_exec.get("vwap"),
        "paper_long_vwap_slippage_pct": long_exec.get("slippage_pct"),
        "paper_short_vwap_slippage_pct": short_exec.get("slippage_pct"),
        "paper_max_vwap_slippage_pct": max_exec_slippage,
        "seven_day_return_pct": seven_return, "twenty_four_hour_return_pct": day_return,
        "atr_24h_pct": atr_pct, "rsi_1h": rsi_value(closes),
        "momentum_5m_pct": mom5, "closes_1h": closes[-30:],
        "live_price_fresh": True, "momentum_5m_fresh": True,
        "market_data_age_seconds": 0.0,
        "source": "Binance USDⓈ-M Futures public market data"
    }
    with SessionLocal() as db:
        row = db.get(MarketInsight, symbol)
        if row is None:
            row = MarketInsight(symbol=symbol)
            db.add(row)
        row.fetched_at = now()
        row.raw_json = json.dumps(data, separators=(",", ":"), default=str)
        db.commit()
    return data

def recovery_statistics(trader_id: str, position_key: str, symbol: str, side: str, entry: float | None) -> dict:
    position_id = position_key.split("|", 1)[0] if "|" in position_key else ""
    with SessionLocal() as db:
        history = db.scalars(select(HistoryPosition).where(
            HistoryPosition.trader_id == trader_id,
            HistoryPosition.symbol == symbol,
            HistoryPosition.side == side
        )).all()
        observed = db.scalars(select(PositionObservation).where(
            PositionObservation.trader_id == trader_id,
            PositionObservation.symbol == symbol,
            PositionObservation.side == side
        )).all()
    recovered = []
    for h in history:
        # Match each closed historical position to its own LAES-observed path ID.
        # Current-position observations alone must never be applied to unrelated history rows.
        matching_path = [o for o in observed if h.position_id and o.position_key.startswith(h.position_id + "|")]
        had_adverse = any(o.unrealized_pnl is not None and o.unrealized_pnl < 0 for o in matching_path)
        if had_adverse:
            profitable = h.net_pnl > 0 if h.net_pnl is not None else (
                (h.close_price > h.entry_price) if side == "LONG" and h.close_price and h.entry_price else
                (h.close_price < h.entry_price) if side == "SHORT" and h.close_price and h.entry_price else False
            )
            recovered.append(bool(profitable))
    n = len(recovered)
    if n < 10:
        return {
            "probability": None, "sample_size": n,
            "label": "Insufficient LAES-observed underwater-to-close samples",
            "note": "A recovery probability is withheld until at least 10 comparable closed paths have been observed by LAES."
        }
    p = sum(recovered) / n
    z = 1.96
    denom = 1 + z*z/n
    center = (p + z*z/(2*n)) / denom
    margin = (z * math.sqrt((p*(1-p)/n) + z*z/(4*n*n))) / denom
    return {
        "probability": round(p * 100, 1), "sample_size": n,
        "ci_low": round(max(0, center - margin) * 100, 1),
        "ci_high": round(min(1, center + margin) * 100, 1),
        "label": "Empirical LAES path recovery rate",
        "note": "Computed only from this trader/symbol/side positions that LAES observed underwater and later matched to a closed result."
    }

def holding_time(trader_id: str, position_key: str, raw: dict, observed_at: str | None) -> dict:
    opened = ms_datetime(raw.get("openedAt") or raw.get("opened_at"))
    source = "Provider open timestamp" if opened else None
    if opened is None and observed_at:
        try:
            opened = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            source = "First observed by LAES; true open time unavailable"
        except Exception:
            opened = None
    opened = as_utc(opened)
    hours = (now() - opened).total_seconds() / 3600 if opened else None
    with SessionLocal() as db:
        cache = db.get(TraderProfileCache, trader_id)
        durations = []
        if cache:
            histories = db.scalars(select(HistoryPosition).where(HistoryPosition.trader_id == trader_id)).all()
            durations = [(h.closed_at_ms - h.opened_at_ms) / 3600000 for h in histories if h.opened_at_ms and h.closed_at_ms and h.closed_at_ms > h.opened_at_ms]
    return {
        "current_hours": round(hours, 2) if hours is not None else None,
        "source": source or "Unavailable",
        "typical_median_hours": round(statistics.median(durations), 2) if durations else None,
        "completed_positions_sample": len(durations)
    }

def find_news_for_symbol(symbol: str, items: list[dict]) -> list[dict]:
    base = symbol.upper().replace("USDT", "").replace("USDC", "").replace("BUSD", "")
    aliases = {"BTC": ["bitcoin", "btc"], "ETH": ["ethereum", "ether", "eth"], "SOL": ["solana", "sol"], "BNB": ["binance coin", "bnb"], "XRP": ["ripple", "xrp"], "DOGE": ["dogecoin", "doge"], "CRCL": ["circle", "usdc", "crcl"], "XAU": ["gold", "bullion"], "MSTR": ["microstrategy", "strategy"], "ZEC": ["zcash", "zec"], "AAVE": ["aave"], "HYPE": ["hyperliquid"], "UNI": ["uniswap"], "SNDK": ["sandisk"], "SKHYNIX": ["sk hynix", "sk hynix memory"]}
    terms = aliases.get(base, [base.lower()])
    matches = []
    for item in items:
        title = (item.get("title") or "").lower()
        if any(term in title for term in terms):
            matches.append(item)
    return matches[:5]

def recovery_potential_score(*, adverse, atr_pct, target_pct, bounce_pct, reversal_confirmed, legacy_confirmed,
                             direction_ok_1m, win_rate, sharpe, recovery_prob, spread_pct, slippage_pct) -> dict:
    """0-100 ranking of how quickly and how likely an underwater leader position is to recover.
    speed 25 + reversal 30 + leader quality 15 + leader's own recovery history 15 + trading cost 15."""
    def clamp(x, lo=0.0, hi=1.0):
        return max(lo, min(hi, x))
    est_minutes = None
    speed = 0.0
    if adverse and atr_pct and atr_pct > 0:
        sigma_min = atr_pct / math.sqrt(60.0)               # hourly ATR% -> per-minute volatility
        distance = min(adverse, target_pct) if target_pct else adverse
        est_minutes = min(999.0, (distance / sigma_min) ** 2)   # random-walk time to cover the distance
        speed = 25.0 * clamp((300.0 - est_minutes) / 270.0)
    if reversal_confirmed and bounce_pct is not None and adverse:
        reversal = 15.0 + 15.0 * clamp(bounce_pct / (0.25 * adverse))
    elif legacy_confirmed:
        reversal = 15.0
    elif direction_ok_1m:
        reversal = 8.0
    else:
        reversal = 0.0
    leader = 7.5 * clamp(((win_rate if win_rate is not None else 0.5) - 0.5) / 0.3) + 7.5 * clamp((sharpe or 0.0) / 2.0)
    history = 15.0 * clamp(recovery_prob / 100.0) if recovery_prob is not None else 7.5
    cost = 7.5 * (1 - clamp((spread_pct if spread_pct is not None else 0.1) / 0.10)) + 7.5 * (1 - clamp((slippage_pct if slippage_pct is not None else 0.1) / 0.10))
    total = speed + reversal + leader + history + cost
    return {
        "score": round(total, 1), "est_minutes": round(est_minutes, 0) if est_minutes is not None else None,
        "components": {"speed": round(speed, 1), "reversal": round(reversal, 1), "leader_quality": round(leader, 1),
                       "leader_recovery_history": round(history, 1), "trading_cost": round(cost, 1)},
    }


async def build_opportunity(
    obs: PositionObservation, snapshot_time: str | None, trader_name: str | None,
    news: list[dict], allow_entry_creation: bool = False, deferred: list | None = None
) -> dict:
    raw = parse_json(obs.raw_json)
    entry = obs.entry_price
    cfg = paper_config()
    min_adverse = float(cfg["min_adverse_pct"])
    leader_mark = safe_float(obs.mark_price)
    leader_adverse = None
    if entry and leader_mark and entry > 0:
        leader_adverse = ((entry - leader_mark) / entry * 100) if obs.side == "LONG" else ((leader_mark - entry) / entry * 100)
    leader_underwater = (obs.unrealized_pnl is not None and obs.unrealized_pnl < 0) or (leader_adverse is not None and leader_adverse > 0)
    # Do not fetch six Binance REST endpoints for positions that cannot qualify.
    if leader_underwater and leader_adverse is not None and leader_adverse >= min_adverse:
        try:
            market = await market_features(obs.symbol, allow_rest=allow_entry_creation, wait=False)
        except Exception as exc:
            market = {"symbol": obs.symbol, "error": str(exc)[:220], "source": "Binance market data unavailable"}
    else:
        market = {"symbol": obs.symbol, "mark_price": leader_mark, "momentum_5m_pct": None, "not_requested": True}
    mark = safe_float(market.get("mark_price")) or leader_mark
    adverse = None
    if entry and mark and entry > 0:
        adverse = ((entry - mark) / entry * 100) if obs.side == "LONG" else ((mark - entry) / entry * 100)
    underwater = (obs.unrealized_pnl is not None and obs.unrealized_pnl < 0) or (adverse is not None and adverse > 0)
    momentum = safe_float(market.get("momentum_5m_pct"))
    legacy_confirmation = bool(market.get("momentum_5m_fresh")) and momentum is not None and ((momentum > 0) if obs.side == "LONG" else (momentum < 0))
    # Reversal = price has bounced off its 5-minute extreme in our favour AND the last minute points our way.
    mom_1m = safe_float(market.get("momentum_1m_pct"))
    bounce_pct = None
    if mark and mark > 0:
        if obs.side == "LONG" and safe_float(market.get("low_5m")):
            bounce_pct = (mark / safe_float(market.get("low_5m")) - 1) * 100
        elif obs.side == "SHORT" and safe_float(market.get("high_5m")):
            bounce_pct = (safe_float(market.get("high_5m")) / mark - 1) * 100
    direction_ok_1m = mom_1m is not None and ((mom_1m > 0) if obs.side == "LONG" else (mom_1m < 0))
    reversal_confirmed = bool(market.get("reversal_ready")) and bounce_pct is not None and bounce_pct >= float(cfg.get("reversal_min_bounce_pct", 0.03)) and direction_ok_1m
    confirmation = reversal_confirmed or legacy_confirmation
    quote_volume = safe_float(market.get("quote_volume_24h_usdt"))
    spread_pct = safe_float(market.get("spread_pct"))
    funding_rate = safe_float(market.get("funding_rate"))
    atr_quality = safe_float(market.get("atr_24h_pct"))
    dead_market_reasons = []
    market_not_requested = bool(market.get("not_requested"))
    if quote_volume is None or quote_volume < 5_000_000:
        dead_market_reasons.append("24h quote volume below $5M or unavailable")
    if spread_pct is None or spread_pct > 0.10:
        dead_market_reasons.append("spread above 0.10% or unavailable")
    depth_usdt = safe_float(market.get("min_depth_within_1pct_usdt"))
    if depth_usdt is None or depth_usdt < 10000:
        dead_market_reasons.append("order-book depth within 1% below $10K or unavailable")
    if obs.side == "LONG":
        if not market.get("paper_long_book_covered", False):
            dead_market_reasons.append("full simulated LONG notional is not covered by sampled asks within 1%")
        if (safe_float(market.get("paper_long_vwap_slippage_pct")) or 999.0) > 0.10:
            dead_market_reasons.append("full simulated LONG VWAP slippage exceeds 0.10%")
    else:
        if not market.get("paper_short_book_covered", False):
            dead_market_reasons.append("full simulated SHORT notional is not covered by sampled bids within 1%")
        if (safe_float(market.get("paper_short_vwap_slippage_pct")) or 999.0) > 0.10:
            dead_market_reasons.append("full simulated SHORT VWAP slippage exceeds 0.10%")
    volume_ratio = safe_float(market.get("volume_ratio_recent_6h_vs_prior_24h"))
    if volume_ratio is None or volume_ratio < 0.30:
        dead_market_reasons.append("recent 6h volume below 30% of prior 24h hourly baseline or unavailable")
    if atr_quality is None or atr_quality < 0.02 or atr_quality > 8.0:
        dead_market_reasons.append("volatility too low or disorderly")
    if not market.get("live_price_fresh", False):
        dead_market_reasons.append("live Binance mark-price stream not warmed")
    if not (market.get("momentum_5m_fresh", False) or market.get("reversal_ready", False)):
        dead_market_reasons.append("live price history still warming (needs ~90s of WebSocket data)")
    if funding_rate is not None and ((obs.side == "LONG" and funding_rate > 0.0005) or (obs.side == "SHORT" and funding_rate < -0.0005)):
        dead_market_reasons.append("funding cost is unusually adverse for this side")
    if market_not_requested:
        # Market gates are only evaluated once the leader is underwater by the minimum adverse %.
        dead_market_reasons = []
    market_quality_ok = not dead_market_reasons
    recovery = recovery_statistics(obs.trader_id, obs.position_key, obs.symbol, obs.side, entry)
    score_info = {"score": None}
    if not market_not_requested and underwater:
        with SessionLocal() as db_score:
            trader_row = db_score.get(Trader, obs.trader_id)
        score_info = recovery_potential_score(
            adverse=adverse, atr_pct=atr_quality, target_pct=float(cfg.get("target_pct", 0.70)),
            bounce_pct=bounce_pct, reversal_confirmed=reversal_confirmed, legacy_confirmed=legacy_confirmation,
            direction_ok_1m=direction_ok_1m, win_rate=trader_row.win_rate if trader_row else None,
            sharpe=trader_row.sharpe_ratio if trader_row else None, recovery_prob=recovery.get("probability"),
            spread_pct=spread_pct, slippage_pct=safe_float(market.get("paper_long_vwap_slippage_pct" if obs.side == "LONG" else "paper_short_vwap_slippage_pct")))
    min_score = float(cfg.get("recovery_min_score", 30.0))
    score_ok = score_info["score"] is None or score_info["score"] >= min_score
    below_min_adverse = bool(underwater and not (adverse is not None and adverse >= min_adverse))
    qualified = bool(underwater and adverse is not None and adverse >= min_adverse and confirmation and market_quality_ok and score_ok)
    if qualified:
        status = "QUALIFIED"
    elif below_min_adverse:
        status = "UNDERWATER · BELOW MIN ADVERSE"
    elif underwater and not market_quality_ok:
        status = "DEAD / COSTLY MARKET"
    else:
        status = "UNDERWATER WATCH" if underwater else "NOT UNDERWATER"
    rejection_reasons = list(dead_market_reasons)
    if not underwater:
        rejection_reasons.append("leader position is in profit or flat, so there is no adverse excursion to recover")
    elif below_min_adverse:
        rejection_reasons.append(f"adverse move {adverse if adverse is not None else 0:.2f}% is below the {min_adverse:.2f}% minimum")
    elif market_quality_ok and not confirmation:
        rejection_reasons.append(f"no reversal yet: needs a bounce of at least {float(cfg.get('reversal_min_bounce_pct', 0.03)):.2f}% off the 5-minute extreme with the last minute moving our way")
    elif market_quality_ok and confirmation and not score_ok:
        rejection_reasons.append(f"recovery score {score_info['score']} is below the minimum {min_score:.0f}")
    with SessionLocal() as db:
        latest_leader_snapshot = db.scalar(select(PositionSnapshot).where(
            PositionSnapshot.trader_id == obs.trader_id,
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).order_by(desc(PositionSnapshot.id)).limit(1))
    leader_snapshot_current = bool(latest_leader_snapshot and not latest_leader_snapshot.hidden and latest_leader_snapshot.id == obs.snapshot_id)
    snapshot_age_seconds = max(0.0, (now() - (as_utc(latest_leader_snapshot.observed_at) or now())).total_seconds()) if latest_leader_snapshot else None
    leader_snapshot_fresh = bool(leader_snapshot_current and snapshot_age_seconds is not None and snapshot_age_seconds <= 900)
    if not leader_snapshot_current:
        qualified = False
        status = "LEADER POSITIONS HIDDEN" if latest_leader_snapshot and latest_leader_snapshot.hidden else "POSITION NOT IN LATEST LEADER SNAPSHOT"
    elif not leader_snapshot_fresh:
        qualified = False
        status = "STALE LEADER SNAPSHOT"
    recovery_checks = {
        "leader_underwater": underwater, "leader_adverse_pct": adverse,
        "minimum_adverse_pct": min_adverse, "adverse_gate_passed": bool(adverse is not None and adverse >= min_adverse),
        "directional_5m_momentum_confirmed": confirmation,
        "live_price_fresh": bool(market.get("live_price_fresh", False)),
        "momentum_5m_fresh": bool(market.get("momentum_5m_fresh", False)),
        "quote_volume_24h_usdt": quote_volume, "spread_pct": spread_pct,
        "depth_within_1pct_usdt": depth_usdt, "volume_ratio_6h_vs_prior_24h": volume_ratio,
        "atr_24h_pct": atr_quality, "funding_rate": funding_rate,
        "leader_snapshot_current": leader_snapshot_current,
        "leader_snapshot_fresh_under_15m": leader_snapshot_fresh,
        "market_quality_passed": market_quality_ok,
        "reversal_confirmed": reversal_confirmed, "bounce_off_5m_extreme_pct": bounce_pct,
        "momentum_1m_pct": mom_1m, "recovery_score": score_info.get("score"),
        "est_recovery_minutes": score_info.get("est_minutes"),
    }
    paper_trade_id = None
    execution_status = "NOT_QUALIFIED"
    if qualified:
        if not cfg.get("auto_enabled"):
            execution_status = "AUTO_ENTRY_DISABLED"
        elif not allow_entry_creation:
            execution_status = "SCANNER_PREVIEW_ONLY"
        elif deferred is not None:
            execution_status = "PENDING_RANKED_ENTRY"  # entered later, best recovery score first
        else:
            execution_result = create_paper_trade(obs, trader_name, mark or entry or 0, cfg, market, "RECOVERY")
            paper_trade_id = execution_result.get("trade_id")
            execution_status = execution_result.get("reason", "ENTRY_REJECTED_UNKNOWN")
    holder: dict = {}
    def finalize_entry() -> None:
        er = create_paper_trade(obs, trader_name, mark or entry or 0, cfg, market, "RECOVERY")
        record_opportunity_decision(obs, "RECOVERY", qualified, status, rejection_reasons, recovery_checks,
                                    er.get("reason", "ENTRY_REJECTED_UNKNOWN"), er.get("trade_id"))
        holder["result"]["execution_status"] = er.get("reason", "ENTRY_REJECTED_UNKNOWN")
        holder["result"]["paper_trade_id"] = er.get("trade_id")
    if allow_entry_creation and execution_status == "PENDING_RANKED_ENTRY":
        deferred.append((score_info.get("score") or 0.0, finalize_entry, holder))
    elif allow_entry_creation:
        record_opportunity_decision(obs, "RECOVERY", qualified, status, rejection_reasons, recovery_checks, execution_status, paper_trade_id)
    causes = []
    week = safe_float(market.get("seven_day_return_pct"))
    day = safe_float(market.get("twenty_four_hour_return_pct"))
    atr = safe_float(market.get("atr_24h_pct"))
    if week is not None:
        against = (week < 0) if obs.side == "LONG" else (week > 0)
        causes.append({"type": "market_trend", "label": f"7-day market return {week:+.2f}%", "effect": "Against position" if against else "With position", "risk": "adverse" if against else "supportive"})
    if day is not None:
        against = (day < 0) if obs.side == "LONG" else (day > 0)
        causes.append({"type": "short_term", "label": f"24-hour market return {day:+.2f}%", "effect": "Against position" if against else "With position", "risk": "adverse" if against else "supportive"})
    if atr is not None:
        causes.append({"type": "volatility", "label": f"24-hour ATR / price {atr:.2f}%", "effect": "Elevated volatility" if atr >= 1.5 else "Normal-to-moderate volatility", "risk": "warning" if atr >= 1.5 else "neutral"})
    matched_news = find_news_for_symbol(obs.symbol, news)
    causes.extend([{"type": "news_context", "label": item["title"], "effect": "Contemporaneous headline; not proof of causation", "url": item["url"], "published_at": item["published_at"], "risk": "context"} for item in matched_news])
    hold = holding_time(obs.trader_id, obs.position_key, raw, snapshot_time)
    holder["result"] = result = {
        "strategy": "RECOVERY", "trader_id": obs.trader_id, "trader_name": trader_name or obs.trader_id,
        "position_key": obs.position_key, "symbol": obs.symbol, "side": obs.side,
        "entry_price": entry, "mark_price": mark, "adverse_pct": round(adverse, 3) if adverse is not None else None,
        "unrealized_pnl": obs.unrealized_pnl, "roi": obs.roi, "leverage": obs.leverage,
        "margin_mode": obs.margin_mode, "liquidation_price": obs.liquidation_price,
        "observed_at": snapshot_time, "underwater": underwater, "qualified": qualified,
        "status": status, "momentum_5m_pct": momentum, "market": market,
        "market_quality_ok": market_quality_ok, "dead_market_reasons": dead_market_reasons,
        "leader_snapshot_current": leader_snapshot_current,
        "leader_snapshot_fresh": leader_snapshot_fresh,
        "snapshot_age_seconds": round(snapshot_age_seconds, 1) if snapshot_age_seconds is not None else None,
        "recovery": recovery, "holding": hold, "likely_causes": causes,
        "news_context_count": len(matched_news),
        "recovery_warning": "Market trend/news are contextual risk factors, not a deterministic cause or guarantee of recovery.",
        "recovery_score": score_info.get("score"), "score_components": score_info.get("components"),
        "est_recovery_minutes": score_info.get("est_minutes"), "reversal_confirmed": reversal_confirmed,
        "bounce_off_5m_extreme_pct": round(bounce_pct, 3) if bounce_pct is not None else None,
        "momentum_1m_pct": round(mom_1m, 3) if mom_1m is not None else None,
        "rejection_reasons": rejection_reasons,
        "execution_status": execution_status, "paper_trade_id": paper_trade_id,
    }
    return result

async def momentum_market_features(symbol: str) -> dict:
    """
    Lightweight momentum feed that never depends on Binance REST availability.
    Static liquidity/volatility is taken from the persisted MarketInsight cache when
    available; directional 5-minute momentum and mark price come from the live
    Binance WebSocket. Unknown static fields are marked pending rather than silently
    rejecting a research-only opportunity.
    """
    symbol = str(symbol).upper()
    with SessionLocal() as db:
        cache_row = db.get(MarketInsight, symbol)
    cached = parse_json(cache_row.raw_json) if cache_row else {}
    live_at = PAPER_SYMBOL_LAST_PRICE_AT.get(symbol)
    live_fresh = bool(live_at and time.time() - live_at <= 10)
    live_price = PAPER_SYMBOL_LAST_PRICE.get(symbol) if live_fresh else safe_float(cached.get("mark_price"))
    history = PAPER_SYMBOL_PRICE_HISTORY.get(symbol, [])
    cutoff = time.time() - 300
    baseline = next((price for stamp, price in history if stamp <= cutoff), None)
    momentum_fresh = bool(live_fresh and baseline and baseline > 0 and live_price and live_price > 0)
    momentum = ((live_price / baseline) - 1) * 100 if momentum_fresh else None
    return {
        "symbol": symbol,
        "mark_price": live_price,
        "momentum_5m_pct": momentum,
        "momentum_5m_fresh": momentum_fresh,
        "live_price_fresh": live_fresh,
        "quote_volume_24h_usdt": safe_float(cached.get("quote_volume_24h_usdt")),
        "spread_pct": safe_float(cached.get("spread_pct")),
        "min_depth_within_1pct_usdt": safe_float(cached.get("min_depth_within_1pct_usdt")),
        "volume_ratio_recent_6h_vs_prior_24h": safe_float(cached.get("volume_ratio_recent_6h_vs_prior_24h")),
        "atr_24h_pct": safe_float(cached.get("atr_24h_pct")),
        "paper_long_book_covered": cached.get("paper_long_book_covered"),
        "paper_short_book_covered": cached.get("paper_short_book_covered"),
        "paper_long_vwap_slippage_pct": safe_float(cached.get("paper_long_vwap_slippage_pct")),
        "paper_short_vwap_slippage_pct": safe_float(cached.get("paper_short_vwap_slippage_pct")),
        "funding_rate": PAPER_SYMBOL_FUNDING_RATE.get(symbol, safe_float(cached.get("funding_rate"))),
        "next_funding_time": cached.get("next_funding_time"),
        "static_market_data_status": "CACHED" if cache_row else "PENDING",
        "source": "BINANCE_WS_MARK_PRICE_1S + PERSISTED_MARKET_INSIGHT",
    }

async def build_momentum_opportunity(
    obs: PositionObservation, snapshot_time: str | None, trader_name: str | None,
    news: list[dict], allow_entry_creation: bool = False
) -> dict:
    """Independent paper strategy: profitable leader + directional Binance momentum."""
    entry = safe_float(obs.entry_price)
    leader_mark = safe_float(obs.mark_price)
    cfg = paper_config()
    leader_profit_pct = None
    if entry and leader_mark and entry > 0:
        leader_profit_pct = ((leader_mark-entry)/entry*100) if obs.side == "LONG" else ((entry-leader_mark)/entry*100)
    leader_profitable = bool(leader_profit_pct is not None and leader_profit_pct >= float(cfg.get("momentum_min_leader_profit_pct", 0.05)))
    market = await momentum_market_features(obs.symbol) if leader_profitable else {
        "symbol": obs.symbol, "mark_price": leader_mark, "momentum_5m_pct": None,
        "momentum_5m_fresh": False, "live_price_fresh": False, "not_requested": True
    }
    mark = safe_float(market.get("mark_price")) or leader_mark
    momentum = safe_float(market.get("momentum_5m_pct"))
    threshold = float(cfg.get("momentum_min_5m_pct", 0.02))
    momentum_ok = bool(market.get("momentum_5m_fresh")) and momentum is not None and (
        momentum >= threshold if obs.side == "LONG" else momentum <= -threshold
    )
    reasons = []
    quote_volume = safe_float(market.get("quote_volume_24h_usdt"))
    spread_pct = safe_float(market.get("spread_pct"))
    depth_usdt = safe_float(market.get("min_depth_within_1pct_usdt"))
    atr = safe_float(market.get("atr_24h_pct"))
    # Unknown static fields are data-pending, not automatic rejection. This is a
    # research paper ledger, so missing cached depth/volume must not erase a live
    # profitable-leader signal. Known bad values remain hard rejects.
    if quote_volume is not None and quote_volume < 5_000_000: reasons.append("24h quote volume below $5M")
    if spread_pct is not None and spread_pct > 0.10: reasons.append("spread above 0.10%")
    if depth_usdt is not None and depth_usdt < 10000: reasons.append("order-book depth within 1% below $10K")
    if obs.side == "LONG":
        if market.get("paper_long_book_covered") is False: reasons.append("cached LONG book cannot cover simulated notional")
        slip = safe_float(market.get("paper_long_vwap_slippage_pct"))
        if slip is not None and slip > 0.10: reasons.append("cached LONG VWAP slippage exceeds 0.10%")
    else:
        if market.get("paper_short_book_covered") is False: reasons.append("cached SHORT book cannot cover simulated notional")
        slip = safe_float(market.get("paper_short_vwap_slippage_pct"))
        if slip is not None and slip > 0.10: reasons.append("cached SHORT VWAP slippage exceeds 0.10%")
    volume_ratio = safe_float(market.get("volume_ratio_recent_6h_vs_prior_24h"))
    if volume_ratio is not None and volume_ratio < 0.30: reasons.append("recent 6h volume below 30% baseline")
    if atr is not None and (atr < 0.02 or atr > 8.0): reasons.append("volatility too low or disorderly")
    if not market.get("live_price_fresh", False): reasons.append("live Binance mark-price stream not warmed")
    if not market.get("momentum_5m_fresh", False): reasons.append("live 5-minute momentum not warmed")
    with SessionLocal() as db:
        latest = db.scalar(select(PositionSnapshot).where(
            PositionSnapshot.trader_id == obs.trader_id,
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).order_by(desc(PositionSnapshot.id)).limit(1))
    current = bool(latest and not latest.hidden and latest.id == obs.snapshot_id)
    fresh = bool(current and (now() - (as_utc(latest.observed_at) or now())).total_seconds() <= 900)
    qualified = bool(leader_profitable and momentum_ok and not reasons and mark and entry and current and fresh)
    if not current:
        status = "LEADER POSITIONS HIDDEN" if latest and latest.hidden else "POSITION NOT IN LATEST LEADER SNAPSHOT"
    elif not fresh:
        status = "STALE LEADER SNAPSHOT"
    elif not leader_profitable:
        status = "LEADER NOT PROFITABLE"
    elif not momentum_ok:
        status = "PROFITABLE · MOMENTUM NOT CONFIRMED"
    elif reasons:
        status = "DEAD / COSTLY MARKET"
    else:
        status = "QUALIFIED MOMENTUM"
    momentum_checks = {
        "leader_profit_pct": leader_profit_pct,
        "minimum_leader_profit_pct": float(cfg.get("momentum_min_leader_profit_pct", 0.05)),
        "leader_profitable": leader_profitable, "momentum_5m_pct": momentum,
        "minimum_directional_5m_pct": threshold, "directional_momentum_confirmed": momentum_ok,
        "live_price_fresh": bool(market.get("live_price_fresh", False)),
        "momentum_5m_fresh": bool(market.get("momentum_5m_fresh", False)),
        "quote_volume_24h_usdt": quote_volume, "spread_pct": spread_pct,
        "depth_within_1pct_usdt": depth_usdt, "volume_ratio_6h_vs_prior_24h": volume_ratio,
        "atr_24h_pct": atr, "leader_snapshot_current": current,
        "leader_snapshot_fresh_under_15m": fresh
    }
    paper_trade_id = None
    execution_status = "NOT_QUALIFIED"
    if qualified:
        if not cfg.get("auto_enabled"):
            execution_status = "AUTO_ENTRY_DISABLED"
        elif not allow_entry_creation:
            execution_status = "SCANNER_PREVIEW_ONLY"
        else:
            execution_result = create_paper_trade(obs, trader_name, mark, cfg, market, "MOMENTUM")
            paper_trade_id = execution_result.get("trade_id")
            execution_status = execution_result.get("reason", "ENTRY_REJECTED_UNKNOWN")
    if allow_entry_creation:
        record_opportunity_decision(obs, "MOMENTUM", qualified, status, reasons, momentum_checks, execution_status, paper_trade_id)
    return {
        "strategy": "MOMENTUM", "trader_id": obs.trader_id, "trader_name": trader_name or obs.trader_id,
        "position_key": obs.position_key, "symbol": obs.symbol, "side": obs.side,
        "entry_price": entry, "mark_price": mark, "leader_profit_pct": leader_profit_pct,
        "unrealized_pnl": obs.unrealized_pnl, "roi": obs.roi, "leverage": obs.leverage,
        "observed_at": snapshot_time, "leader_profitable": leader_profitable,
        "momentum_5m_pct": momentum, "momentum_confirmed": momentum_ok,
        "qualified": qualified, "status": status, "market": market,
        "dead_market_reasons": reasons, "leader_snapshot_current": current,
        "leader_snapshot_fresh": fresh,
        "entry_model": "CURRENT_BINANCE_MARK_WHILE_PROFITABLE_LEADER_HAS_DIRECTIONAL_5M_MOMENTUM"
    }

def create_paper_trade(obs: PositionObservation, trader_name: str | None, mark: float, cfg: dict, market: dict, strategy: str = "RECOVERY") -> dict:
    if mark <= 0 or not obs.entry_price or obs.entry_price <= 0:
        return {"trade_id": None, "reason": "INVALID_ENTRY_OR_MARK_PRICE"}
    source_key = f"{obs.trader_id}|{obs.position_key}"
    with SessionLocal() as db:
        # Re-entry is allowed only against a fresh, visible Binance public-page snapshot
        # containing this exact leader position. Hidden/empty/stale data never authorizes entry.
        latest_snapshot = db.scalar(select(PositionSnapshot).where(
            PositionSnapshot.trader_id == obs.trader_id,
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).order_by(desc(PositionSnapshot.id)).limit(1))
        if latest_snapshot is None or latest_snapshot.hidden or latest_snapshot.id != obs.snapshot_id:
            return {"trade_id": None, "reason": "STALE_HIDDEN_OR_SUPERSEDED_LEADER_SNAPSHOT"}
        existing = db.scalar(select(PaperTrade.id).where(PaperTrade.source_key == source_key, PaperTrade.strategy == strategy, PaperTrade.status == "OPEN").limit(1))
        if existing:
            return {"trade_id": None, "reason": "DUPLICATE_OPEN_POSITION"}
        prior = db.scalars(select(PaperTrade).where(PaperTrade.source_key == source_key, PaperTrade.strategy == strategy).order_by(desc(PaperTrade.opened_at)).limit(1)).all()
        if prior:
            previous_raw = parse_json(prior[0].raw_json)
            if previous_raw.get("leader_snapshot_id") == obs.snapshot_id:
                return {"trade_id": None, "reason": "SAME_LEADER_SNAPSHOT_NO_REENTRY"}
            previous_close = prior[0].closed_at
            if previous_close and previous_close.tzinfo is None:
                previous_close = previous_close.replace(tzinfo=timezone.utc)
            if previous_close and (now() - previous_close).total_seconds() < 180:
                return {"trade_id": None, "reason": "REENTRY_COOLDOWN_180_SECONDS"}
        wallet_epoch = datetime.fromisoformat(str(cfg.get("wallet_epoch_at")).replace("Z", "+00:00")) if cfg.get("wallet_epoch_at") else now()
        if wallet_epoch.tzinfo is None:
            wallet_epoch = wallet_epoch.replace(tzinfo=timezone.utc)
        opened_trades_all = db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN")).all()
        opened_trades = [t for t in opened_trades_all if (_shadow_dt(t.opened_at) or wallet_epoch) >= wallet_epoch]
        used_margin = sum(float(t.margin_used or 0.0) for t in opened_trades)
        closed_trades_all = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED")).all()
        realized = sum(float(t.net_pnl or 0.0) for t in closed_trades_all
                       if (_shadow_dt(t.opened_at) or wallet_epoch) >= wallet_epoch)
        # One shared wallet across RECOVERY and MOMENTUM. Research discovery remains
        # uncapped; only executable paper entries reserve this finite wallet.
        capital = max(0.0, float(cfg["capital_usdt"]) + float(realized))
        free_capital = max(0.0, capital - used_margin)
        max_margin = capital * float(cfg["max_margin_pct"]) / 100.0
        max_total_margin = capital * float(cfg["max_total_margin_pct"]) / 100.0
        remaining_portfolio_margin = max(0.0, max_total_margin - used_margin)
        base_margin = capital * float(cfg["risk_pct"]) / 100.0 / max(float(cfg["stop_pct"]) / 100.0, 0.0001)
        leverage = max(1.0, min(float(cfg["max_leverage"]), float(obs.leverage or 1.0)))
        margin = min(base_margin, max_margin, free_capital, remaining_portfolio_margin)
        notional = min(margin * leverage, float(cfg.get("max_notional_usdt", 50.0)))
        # Recalculate margin after notional cap; this keeps reported leverage exact.
        margin = notional / leverage if leverage > 0 else 0.0
        if notional < 5.0 or margin <= 0.0:
            return {"trade_id": None, "reason": "SHARED_WALLET_INSUFFICIENT_FREE_MARGIN"}
        # Avoid stacking duplicate exposure to the same underlying direction across
        # different leaders/strategies. Candidates remain visible in the scanner.
        same_direction = sum(1 for t in opened_trades if t.symbol.upper() == obs.symbol.upper() and t.side.upper() == obs.side.upper())
        if same_direction >= int(cfg.get("max_positions_per_symbol_direction", 1)):
            return {"trade_id": None, "reason": "SAME_SYMBOL_DIRECTION_EXPOSURE_LIMIT"}
        quantity = notional / mark
        stop_frac = float(cfg["stop_pct"]) / 100
        target_frac = float(cfg["target_pct"]) / 100
        if obs.side == "LONG":
            stop = mark * (1 - stop_frac)
            target = mark * (1 + target_frac)
        else:
            stop = mark * (1 + stop_frac)
            target = mark * (1 - target_frac)
        new_trade = PaperTrade(
            source_key=source_key, trader_id=obs.trader_id, trader_name=trader_name,
            symbol=obs.symbol, side=obs.side, strategy=strategy, status="OPEN",
            entry_price=mark, mark_price=mark, quantity=quantity, notional=notional,
            margin_used=margin, leverage=leverage, stop_price=stop, target_price=target,
            raw_json=json.dumps({
                "strategy": strategy,
                "leader_source": BINANCE_COPY_SOURCE,
                "leader_entry": obs.entry_price,
                "leader_mark": obs.mark_price,
                "adverse_pct": ((obs.entry_price-mark)/obs.entry_price*100 if obs.side=="LONG" else (mark-obs.entry_price)/obs.entry_price*100),
                "funding_rate_at_entry": market.get("funding_rate"),
                "next_funding_time": market.get("next_funding_time"),
                "leader_snapshot_id": obs.snapshot_id,
                "funding_history_verified": False,
                "funding_audit_status": "PENDING_VERIFICATION",
                "data_quality_status": "CONTINUOUS",
                "mfe_net_pnl": 0.0,
                "mae_net_pnl": 0.0,
                "mfe_pct": 0.0,
                "mae_pct": 0.0,
                "max_data_gap_seconds": 0.0,
                "meaningful_profit_hits": {}
            }, separators=(",", ":"), default=str)
        )
        db.add(new_trade)
        db.flush()
        created_id = int(new_trade.id)
        db.commit()
        return {"trade_id": created_id, "reason": "PAPER_ENTRY_CREATED"}

async def _mark_prices(symbols: list[str]) -> dict[str, float]:
    """Single bulk REST safety net. Never fan out into one request per symbol."""
    if not symbols or time.time() < BINANCE_REST_BLOCKED_UNTIL:
        return {}
    try:
        payload, _ = await binance_get("/fapi/v1/premiumIndex")
        if not isinstance(payload, list):
            return {}
        wanted = {str(symbol).upper() for symbol in symbols}
        return {
            str(item.get("symbol", "")).upper(): price
            for item in payload
            if isinstance(item, dict)
            and str(item.get("symbol", "")).upper() in wanted
            and (price := safe_float(item.get("markPrice"))) is not None
            and price > 0
        }
    except Exception as exc:
        if time.time() >= BINANCE_REST_BLOCKED_UNTIL:
            log_check("paper_mark_price_bulk", False, None, None, str(exc)[:400])
        return {}

def _trade_uses_binance_source(trade: PaperTrade) -> bool:
    return parse_json(trade.raw_json).get("leader_source") in {
        BINANCE_COPY_SOURCE, "BINANCE_PUBLIC_COPY_TRADING"
    }


def _leader_snapshot_for_trade(db, trade: PaperTrade) -> PositionSnapshot | None:
    source_clause = (
        PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        if _trade_uses_binance_source(trade)
        else ~PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
    )
    return db.scalar(select(PositionSnapshot).where(
        PositionSnapshot.trader_id == trade.trader_id, source_clause
    ).order_by(desc(PositionSnapshot.id)).limit(1))


def _leader_for_trade(db, trade: PaperTrade) -> PositionObservation | None:
    prefix = trade.source_key
    if "|" not in prefix:
        return None
    trader_id, position_key = prefix.split("|", 1)
    snapshot = _leader_snapshot_for_trade(db, trade)
    if snapshot is None or snapshot.hidden:
        return None
    return db.scalar(select(PositionObservation).where(
        PositionObservation.snapshot_id == snapshot.id,
        PositionObservation.trader_id == trader_id,
        PositionObservation.position_key == position_key
    ).limit(1))

def _funding_settlement_count(trade: PaperTrade, raw: dict, at: datetime | None) -> int:
    """Count settlement boundaries after this paper entry, not before it."""
    if at is None:
        return 0
    next_funding_ms = safe_float(raw.get("next_funding_time"))
    if not next_funding_ms:
        return 0
    opened = trade.opened_at
    if opened.tzinfo is None:
        opened = opened.replace(tzinfo=timezone.utc)
    opened_ms = opened.timestamp() * 1000
    # MarketInsight can be cached across a funding boundary. Its nextFundingTime
    # then points to the previous settlement; roll it forward to the first one
    # that can actually occur after this paper trade was opened.
    while next_funding_ms <= opened_ms:
        next_funding_ms += 28800000
    at_ms = at.timestamp() * 1000
    if at_ms < next_funding_ms:
        return 0
    return 1 + int((at_ms - next_funding_ms) // 28800000)


def _paper_pnl(trade: PaperTrade, mark: float, cfg: dict, raw: dict | None = None, at: datetime | None = None) -> tuple[float,float,float,float]:
    directional = 1 if trade.side == "LONG" else -1
    gross = directional * (mark - trade.entry_price) * trade.quantity
    # Round-trip costs are charged on both actual simulated notionals: entry and
    # mark-based exit. This is more accurate than charging entry notional twice.
    entry_notional = max(0.0, float(trade.entry_price) * float(trade.quantity))
    exit_notional = max(0.0, float(mark) * float(trade.quantity))
    round_trip_turnover = entry_notional + exit_notional
    fee_cost = round_trip_turnover * (float(cfg["taker_fee_pct"]) / 100)
    slip_cost = round_trip_turnover * (float(cfg["slippage_pct"]) / 100)
    # When funding history is unavailable, estimate only settlements that occurred
    # after this paper entry. Never assume a funding credit: use the larger absolute
    # entry/live rate as a conservative debit.
    raw = raw or {}
    entry_rate = safe_float(raw.get("funding_rate_at_entry"))
    live_rate = safe_float(PAPER_SYMBOL_FUNDING_RATE.get(trade.symbol.upper()))
    rate_candidates = [abs(v) for v in (entry_rate, live_rate) if v is not None]
    funding_rate_abs = max(rate_candidates) if rate_candidates else 0.0
    funding_events = _funding_settlement_count(trade, raw, at)
    estimated_funding_cost = trade.notional * funding_rate_abs * funding_events
    raw["funding_estimate_current"] = 0.0 if raw.get("funding_history_verified") else estimated_funding_cost
    raw["funding_estimate_settlement_count"] = funding_events
    raw["funding_estimate_rate_abs"] = funding_rate_abs
    funding_cost = safe_float(raw.get("funding_cost_actual")) if raw.get("funding_history_verified") else None
    if funding_cost is None:
        funding_cost = estimated_funding_cost
    costs = fee_cost + slip_cost + funding_cost
    net = gross - costs
    pct = (net / max(trade.margin_used, 0.000001)) * 100
    return gross, costs, net, pct

async def _actual_funding_cost(trade: PaperTrade, at: datetime) -> tuple[float, int]:
    start = trade.opened_at
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    rows, _ = await binance_get("/fapi/v1/fundingRate", {
        "symbol": trade.symbol,
        "startTime": int(start.timestamp() * 1000),
        "endTime": int(at.timestamp() * 1000),
        "limit": 1000
    })
    if not isinstance(rows, list):
        raise RuntimeError("Binance funding history did not return a list")
    direction = 1 if trade.side == "LONG" else -1
    total = 0.0
    for item in rows:
        rate = safe_float(item.get("fundingRate")) or 0.0
        mark = safe_float(item.get("markPrice")) or trade.entry_price
        total += trade.quantity * mark * rate * direction
    return total, len(rows)


def _leader_state_for_trade(db, trade: PaperTrade) -> tuple[str, str]:
    """
    Determine leader state from exact visible evidence, exact provider history,
    or repeated visible omissions. Hidden/unavailable data is never a close.
    """
    if "|" not in trade.source_key:
        return "STALE_OR_UNAVAILABLE", "Trade has no leader position key"
    trader_id, position_key = trade.source_key.split("|", 1)
    latest = _leader_snapshot_for_trade(db, trade)
    if latest is None or latest.hidden:
        return "STALE_OR_UNAVAILABLE", "Latest leader snapshot is unavailable/hidden"

    exact = db.scalar(select(PositionObservation).where(
        PositionObservation.snapshot_id == latest.id,
        PositionObservation.trader_id == trader_id,
        PositionObservation.position_key == position_key
    ).limit(1))
    if exact is not None:
        if exact.side.upper() != trade.side.upper():
            return "REVERSED", f"Leader side changed to {exact.side}"
        return "OPEN_OBSERVED", "Exact leader position present in latest visible snapshot"

    opposite = "SHORT" if trade.side.upper() == "LONG" else "LONG"
    reversed_obs = db.scalar(select(PositionObservation).where(
        PositionObservation.snapshot_id == latest.id,
        PositionObservation.trader_id == trader_id,
        PositionObservation.symbol == trade.symbol.upper(),
        PositionObservation.side == opposite
    ).limit(1))
    if reversed_obs is not None:
        return "REVERSED", f"Visible opposite-side leader position {reversed_obs.position_key}"

    # Provider HistoryPosition is authoritative closure evidence when it matches
    # the exact leader position id. Unrelated historical trades are ignored.
    position_id = position_key.split("|", 1)[0] if "|" in position_key else ""
    if position_id:
        history_close = db.scalar(select(HistoryPosition).where(
            HistoryPosition.trader_id == trader_id,
            HistoryPosition.position_id == position_id,
            HistoryPosition.symbol == trade.symbol.upper(),
            HistoryPosition.side == trade.side.upper(),
            HistoryPosition.status.in_(["CLOSED", "CLOSE", "closed", "close"])
        ).order_by(desc(HistoryPosition.closed_at_ms), desc(HistoryPosition.id)).limit(1))
        if history_close is not None and history_close.closed_at_ms:
            return "CLOSED_CONFIRMED", (
                f"Provider HistoryPosition confirms exact leader position {position_id} "
                f"closed at {history_close.closed_at_ms}"
            )

    raw = parse_json(trade.raw_json)
    missing = int(raw.get("leader_missing_visible_snapshots", 0) or 0)
    if missing >= 2:
        return "STALE_OR_UNAVAILABLE", "Position absent from consecutive visible snapshots; omission is not authoritative closure evidence"
    return "STALE_OR_UNAVAILABLE", "Leader position not present in latest snapshot but closure is not yet confirmed"


def _paper_structure_break(
    db, trade: PaperTrade, mark: float, observed_at: datetime, cfg: dict,
    raw: dict, should_sample: bool
) -> tuple[bool, dict]:
    """Confirm a sustained break of the prior rolling range; MAE is not an input."""
    lookback_minutes = max(15, min(240, int(cfg.get("structure_lookback_minutes", 60) or 60)))
    confirmations = max(2, min(10, int(cfg.get("structure_break_confirmations", 3) or 3)))
    buffer_pct = max(0.05, min(2.0, float(cfg.get("structure_break_buffer_pct", 0.20) or 0.20)))
    since = observed_at - timedelta(minutes=lookback_minutes)
    history = db.scalars(
        select(PaperTradePath.paper_price).where(
            PaperTradePath.paper_trade_id == trade.id,
            PaperTradePath.observed_at >= since,
            PaperTradePath.observed_at < observed_at,
        ).order_by(desc(PaperTradePath.observed_at)).limit(720)
    ).all()
    prices = [float(price) for price in history if safe_float(price) is not None and float(price) > 0]
    state = raw.get("structure_break_monitor")
    state = dict(state) if isinstance(state, dict) else {}
    if len(prices) < 12:
        return False, {"status": "INSUFFICIENT_PRIOR_PATH", "observations": len(prices),
                       "lookback_minutes": lookback_minutes, "confirmed_samples": 0,
                       "required_samples": confirmations}
    # Freeze the boundary once a break begins so new lows/highs cannot move
    # the goalposts and make a continuing breakdown look like intact structure.
    stored_level = safe_float(state.get("level")) if state.get("side") == trade.side.upper() else None
    boundary = stored_level or (min(prices) if trade.side.upper() == "LONG" else max(prices))
    broken = (mark < boundary * (1 - buffer_pct / 100.0) if trade.side.upper() == "LONG"
              else mark > boundary * (1 + buffer_pct / 100.0))
    count = int(state.get("confirmed_samples", 0) or 0) if stored_level is not None else 0
    if should_sample:
        count = count + 1 if broken else 0
        state = {"side": trade.side.upper(), "level": boundary if broken else None,
                 "confirmed_samples": count, "last_sample_at": observed_at.isoformat()}
        raw["structure_break_monitor"] = state
    details = {
        "status": "CONFIRMED_BREAK" if count >= confirmations else "BREAK_WATCH" if broken else "STRUCTURE_INTACT",
        "boundary": round(float(boundary), 12), "mark": round(float(mark), 12),
        "buffer_pct": buffer_pct, "lookback_minutes": lookback_minutes,
        "observations": len(prices), "confirmed_samples": count,
        "required_samples": confirmations,
    }
    return count >= confirmations, details


async def run_post_exit_recovery_audit(max_trades: int = 5, rehydrate_for_strategy_replay: bool = False) -> dict:
    """Resumable read-only audit. Never bypasses the Binance REST circuit breaker."""
    async with recovery_audit_lock:
        cfg = paper_config()
        # One candle pull powers the requested 1h / 4h / 6h / 12h forensic windows.
        horizon_hours = 12.0
        with SessionLocal() as db:
            closed = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED").order_by(PaperTrade.closed_at)).all()
        audit_now = now()
        pending = []
        for item in closed:
            item_raw = parse_json(item.raw_json)
            version = int(item_raw.get("post_exit_recovery_audit_version", 0) or 0)
            status = item_raw.get("post_exit_recovery_audit_status")
            closed_time = _shadow_dt(item.closed_at)
            matured_partial = bool(status == "PARTIAL_WINDOW" and closed_time and (audit_now - closed_time).total_seconds() >= 12 * 3600 and int(item_raw.get("post_exit_audit_partial_attempts", 0) or 0) < 3)
            strategy_cached = item_raw.get("leader_following_replay_v1")
            strategy_cache_valid = bool(
                isinstance(strategy_cached, dict)
                and int(item_raw.get("leader_following_replay_version", 0) or 0) == 1
                and item_raw.get("leader_following_replay_source_audit_at")
                and item_raw.get("leader_following_replay_source_audit_at") == item_raw.get("post_exit_recovery_audit_updated_at")
            )
            candles_missing = not (isinstance(item_raw.get("entry_timing_mark_candles"), list)
                                   and item_raw.get("entry_timing_mark_candles")
                                   and isinstance(item_raw.get("post_exit_mark_candles"), list)
                                   and item_raw.get("post_exit_mark_candles"))
            needs_strategy_rehydrate = bool(rehydrate_for_strategy_replay and candles_missing and not strategy_cache_valid)
            if version != 9 or not item_raw.get("post_exit_recovery_audit_updated_at") or matured_partial or needs_strategy_rehydrate:
                pending.append(item)
        cooldown = binance_rest_status()
        if cooldown.get("blocked"):
            return {"audited": 0, "attempted": 0, "recovered_to_profit": 0, "failures": 0,
                    "blocked": True, "blocked_until": cooldown.get("blocked_until"),
                    "remaining_seconds": cooldown.get("remaining_seconds"), "pending": len(pending),
                    "status": "WAITING_FOR_BINANCE_REST_COOLDOWN"}
        attempted = audited = recovered = failures = 0
        for trade in pending:
            if attempted >= max(1, min(20, int(max_trades))):
                break
            if binance_rest_status().get("blocked"):
                break
            raw = parse_json(trade.raw_json)
            legacy_engine_cache = raw.get("new_exit_engine_audit_v1") if rehydrate_for_strategy_replay else None
            legacy_engine_version = raw.get("new_exit_engine_audit_version") if rehydrate_for_strategy_replay else None
            closed_at = _shadow_dt(trade.closed_at)
            if not closed_at or not trade.exit_price or not trade.entry_price:
                continue
            attempted += 1
            entry_at = _shadow_dt(trade.opened_at) or closed_at
            entry_window_minutes = max(5.0, min(120.0, float(cfg.get("entry_timing_audit_window_minutes", 30.0))))
            start_ms = int((entry_at - timedelta(minutes=entry_window_minutes)).timestamp() * 1000)
            exit_ms = int(closed_at.timestamp() * 1000)
            end_ms = exit_ms + int(horizon_hours * 3600000)
            try:
                rows_all = []
                cursor = start_ms
                while cursor < end_ms:
                    rows, _ = await binance_get("/fapi/v1/markPriceKlines", {
                        "symbol": trade.symbol, "interval": "1m",
                        "startTime": cursor, "endTime": end_ms, "limit": 1500
                    })
                    if not isinstance(rows, list) or not rows:
                        break
                    rows_all.extend(rows)
                    next_cursor = int(rows[-1][0]) + 60000
                    if next_cursor <= cursor or len(rows) < 1500:
                        break
                    cursor = next_cursor
                if not rows_all:
                    raise RuntimeError("No Binance mark-price candles returned for the closed-trade lifecycle")
                rows_all = sorted(rows_all, key=lambda row: int(row[0]) if isinstance(row, (list, tuple)) and row else 0)
                direction = 1.0 if trade.side == "LONG" else -1.0
                entry_rate = abs(safe_float(raw.get("funding_rate_at_entry")) or 0.0)
                next_funding = safe_float(raw.get("next_funding_time"))
                opened = _shadow_dt(trade.opened_at) or closed_at
                opened_ms = int(opened.timestamp() * 1000)
                funding_points = []
                funding_expected = _funding_settlement_count(trade, raw, datetime.fromtimestamp(end_ms / 1000, timezone.utc)) if next_funding else None
                funding_verified_replay = False
                try:
                    funding_cursor = opened_ms
                    funding_rows_all = []
                    while funding_cursor < end_ms:
                        funding_rows, _ = await binance_get("/fapi/v1/fundingRate", {
                            "symbol": trade.symbol, "startTime": funding_cursor,
                            "endTime": end_ms, "limit": 1000
                        })
                        if not isinstance(funding_rows, list) or not funding_rows:
                            break
                        funding_rows_all.extend(funding_rows)
                        next_funding_cursor = int(funding_rows[-1].get("fundingTime", 0) or 0) + 1
                        if next_funding_cursor <= funding_cursor or len(funding_rows) < 1000:
                            break
                        funding_cursor = next_funding_cursor
                    for funding_row in funding_rows_all:
                        funding_time = int(funding_row.get("fundingTime", 0) or 0)
                        funding_rate = safe_float(funding_row.get("fundingRate"))
                        funding_mark = safe_float(funding_row.get("markPrice")) or float(trade.entry_price)
                        if funding_time and funding_rate is not None:
                            funding_points.append({"funding_time": funding_time,
                                "funding_cost": round(float(trade.quantity) * funding_mark * funding_rate * direction, 12)})
                    funding_points.sort(key=lambda item: item["funding_time"])
                    funding_verified_replay = funding_expected is not None and len(funding_points) >= funding_expected
                    if funding_expected == 0 and next_funding:
                        # Same no-settlement-yet rule as the live engine: no funding
                        # debit can exist before the first settlement boundary.
                        funding_verified_replay = True
                except Exception as funding_audit_exc:
                    funding_verified_replay = False
                    raw["funding_history_replay_error"] = str(funding_audit_exc)[:240]
                if funding_expected == 0 and next_funding:
                    funding_verified_replay = True
                    funding_points = []
                    raw["funding_history_replay_source"] = "NO_SETTLEMENT_SINCE_ENTRY"
                def pnl_at(price, at_ms):
                    gross = direction * (price - trade.entry_price) * trade.quantity
                    turnover = (trade.entry_price + price) * trade.quantity
                    costs = turnover * (float(cfg["taker_fee_pct"]) + float(cfg["slippage_pct"])) / 100.0
                    if funding_verified_replay:
                        funding = sum(float(point["funding_cost"]) for point in funding_points if int(point["funding_time"]) <= at_ms)
                    else:
                        if next_funding:
                            next_ms = next_funding
                            while next_ms <= opened_ms:
                                next_ms += 28800000
                            count = 0 if at_ms < next_ms else 1 + int((at_ms - next_ms) // 28800000)
                        else:
                            count = max(0, int((at_ms - opened_ms) // 28800000))
                        funding = float(trade.notional or 0.0) * entry_rate * count
                    return gross - costs - funding, gross, costs + funding, funding
                best_close = None
                best_intrabar = None
                first_profit = first_entry = first_intrabar_profit = None
                worst = None
                last_valid_ms = None
                audit_windows = {h: {"max_close_net": None, "min_close_net": None,
                    "max_intrabar_net": None, "min_intrabar_net": None, "first_net_profit_at": None,
                    "checkpoint_net": None, "checkpoint_price": None,
                    "checkpoint_at": None, "first_at": None, "last_at": None, "last_net": None,
                    "last_price": None, "count": 0, "max_gap_ms": 0, "gap_count": 0} for h in (1, 4, 6, 12)}
                for row in rows_all:
                    if not isinstance(row, (list, tuple)) or len(row) < 7:
                        continue
                    at_ms = int(row[0])
                    # Never trust a provider/mock response that returns candles
                    # outside the exact requested post-exit observation window.
                    if at_ms < start_ms or at_ms > end_ms:
                        continue
                    last_valid_ms = at_ms
                    high, low, close = safe_float(row[2]), safe_float(row[3]), safe_float(row[4])
                    if high is None or low is None or close is None:
                        continue
                    net, gross, costs, funding = pnl_at(close, at_ms)
                    favorable = high if trade.side == "LONG" else low
                    adverse = low if trade.side == "LONG" else high
                    intra_net, _, _, _ = pnl_at(favorable, at_ms)
                    adverse_net, _, _, _ = pnl_at(adverse, at_ms)
                    elapsed_hours = (at_ms - exit_ms) / 3600000.0
                    for horizon, window in audit_windows.items():
                        if 0 <= elapsed_hours <= horizon:
                            if window["first_at"] is None:
                                window["first_at"] = at_ms
                            if window["last_at"] is not None:
                                gap_ms = max(0, at_ms - window["last_at"])
                                window["max_gap_ms"] = max(window["max_gap_ms"], gap_ms)
                                if gap_ms > 120000:
                                    window["gap_count"] += 1
                            window["count"] += 1
                            window["max_close_net"] = net if window["max_close_net"] is None else max(window["max_close_net"], net)
                            window["min_close_net"] = net if window["min_close_net"] is None else min(window["min_close_net"], net)
                            window["max_intrabar_net"] = intra_net if window["max_intrabar_net"] is None else max(window["max_intrabar_net"], intra_net)
                            window["min_intrabar_net"] = adverse_net if window["min_intrabar_net"] is None else min(window["min_intrabar_net"], adverse_net)
                            window["last_at"], window["last_net"], window["last_price"] = at_ms, net, close
                            if net > 0 and window["first_net_profit_at"] is None:
                                window["first_net_profit_at"] = at_ms
                        if elapsed_hours >= horizon and window["checkpoint_at"] is None:
                            window["checkpoint_at"], window["checkpoint_net"], window["checkpoint_price"] = at_ms, net, close
                    if best_close is None or net > best_close["net"]:
                        best_close = {"net": net, "gross": gross, "costs": costs, "funding": funding, "price": close, "at": at_ms}
                    if best_intrabar is None or intra_net > best_intrabar["net"]:
                        best_intrabar = {"net": intra_net, "price": favorable, "at": at_ms}
                    if net > 0 and first_profit is None:
                        first_profit = at_ms
                    if intra_net > 0 and first_intrabar_profit is None:
                        first_intrabar_profit = at_ms
                    if ((trade.side == "LONG" and close >= trade.entry_price) or
                        (trade.side == "SHORT" and close <= trade.entry_price)) and first_entry is None:
                        first_entry = at_ms
                    if worst is None or net < worst["net"]:
                        worst = {"net": net, "at": at_ms}
                if best_close is None or best_intrabar is None:
                    raise RuntimeError("No valid OHLC mark candles returned")
                lifecycle = [row for row in rows_all if isinstance(row, (list, tuple)) and len(row) >= 7
                             and opened_ms <= int(row[0]) <= exit_ms
                             and safe_float(row[2]) is not None and safe_float(row[3]) is not None
                             and safe_float(row[4]) is not None]
                lifecycle_best_close = lifecycle_worst_close = None
                lifecycle_best_intrabar = lifecycle_worst_intrabar = None
                lifecycle_first_profit = None
                for life_row in lifecycle:
                    life_at = int(life_row[0])
                    life_high, life_low, life_close = safe_float(life_row[2]), safe_float(life_row[3]), safe_float(life_row[4])
                    close_net, _, _, _ = pnl_at(life_close, life_at)
                    favorable_price = life_high if trade.side == "LONG" else life_low
                    adverse_price = life_low if trade.side == "LONG" else life_high
                    favorable_net, _, _, _ = pnl_at(favorable_price, life_at)
                    adverse_net, _, _, _ = pnl_at(adverse_price, life_at)
                    if lifecycle_best_close is None or close_net > lifecycle_best_close["net"]:
                        lifecycle_best_close = {"net": close_net, "at": life_at, "price": life_close}
                    if lifecycle_worst_close is None or close_net < lifecycle_worst_close["net"]:
                        lifecycle_worst_close = {"net": close_net, "at": life_at, "price": life_close}
                    if lifecycle_best_intrabar is None or favorable_net > lifecycle_best_intrabar["net"]:
                        lifecycle_best_intrabar = {"net": favorable_net, "at": life_at, "price": favorable_price}
                    if lifecycle_worst_intrabar is None or adverse_net < lifecycle_worst_intrabar["net"]:
                        lifecycle_worst_intrabar = {"net": adverse_net, "at": life_at, "price": adverse_price}
                    if close_net > 0 and lifecycle_first_profit is None:
                        lifecycle_first_profit = life_at
                lifecycle_first_at = int(lifecycle[0][0]) if lifecycle else None
                lifecycle_last_at = int(lifecycle[-1][0]) if lifecycle else None
                lifecycle_gaps = [max(0, int(lifecycle[i][0]) - int(lifecycle[i-1][0])) for i in range(1, len(lifecycle))]
                lifecycle_max_gap_ms = max(lifecycle_gaps, default=0)
                lifecycle_gap_count = sum(1 for gap in lifecycle_gaps if gap > 120000)
                lifecycle_expected = max(1, int((lifecycle_last_at - lifecycle_first_at) / 60000) + 1) if lifecycle_first_at is not None and lifecycle_last_at is not None else 1
                lifecycle_coverage_pct = min(100.0, len(lifecycle) / lifecycle_expected * 100.0)
                lifecycle_complete = bool(lifecycle_first_at is not None and lifecycle_last_at is not None
                    and lifecycle_first_at <= opened_ms + 60000 and lifecycle_last_at >= exit_ms - 60000
                    and lifecycle_max_gap_ms <= 120000 and lifecycle_coverage_pct >= 98.0)
                lifecycle_audit = {
                    "status": "COMPLETE" if lifecycle_complete else "PARTIAL_OR_MISSING_ENTRY_TO_EXIT_CANDLES",
                    "candle_count": len(lifecycle), "expected_candle_count": lifecycle_expected,
                    "coverage_pct": round(lifecycle_coverage_pct, 3),
                    "max_gap_seconds": round(lifecycle_max_gap_ms / 1000.0, 2),
                    "gap_count_over_2m": lifecycle_gap_count, "candle_interval": "1m",
                    "source": "BINANCE_USDM_MARK_PRICE_1M",
                    "max_close_mfe_net_estimate": round(lifecycle_best_close["net"], 8) if lifecycle_best_close else None,
                    "max_close_mfe_at": datetime.fromtimestamp(lifecycle_best_close["at"]/1000, timezone.utc).isoformat() if lifecycle_best_close else None,
                    "min_close_mae_net_estimate": round(lifecycle_worst_close["net"], 8) if lifecycle_worst_close else None,
                    "min_close_mae_at": datetime.fromtimestamp(lifecycle_worst_close["at"]/1000, timezone.utc).isoformat() if lifecycle_worst_close else None,
                    "max_intrabar_mfe_net_estimate": round(lifecycle_best_intrabar["net"], 8) if lifecycle_best_intrabar else None,
                    "max_intrabar_mfe_at": datetime.fromtimestamp(lifecycle_best_intrabar["at"]/1000, timezone.utc).isoformat() if lifecycle_best_intrabar else None,
                    "min_intrabar_mae_net_estimate": round(lifecycle_worst_intrabar["net"], 8) if lifecycle_worst_intrabar else None,
                    "min_intrabar_mae_at": datetime.fromtimestamp(lifecycle_worst_intrabar["at"]/1000, timezone.utc).isoformat() if lifecycle_worst_intrabar else None,
                    "first_close_confirmed_net_profit_at": datetime.fromtimestamp(lifecycle_first_profit/1000, timezone.utc).isoformat() if lifecycle_first_profit else None,
                    "actual_recorded_net_pnl": round(float(trade.net_pnl or 0.0), 8),
                    "actual_exit_capture_pct_of_close_mfe": round(float(trade.net_pnl or 0.0) / lifecycle_best_close["net"] * 100.0, 2) if lifecycle_best_close and lifecycle_best_close["net"] > 0 else None,
                    "close_mfe_left_on_table_estimate": round(max(0.0, lifecycle_best_close["net"] - float(trade.net_pnl or 0.0)), 8) if lifecycle_best_close else None,
                    "caveat": "1m mark-price candle extrema are excursion bounds, not guaranteed fills; intra-candle ordering is unknown. Costs/funding use the audit assumptions."
                }
                pre_entry = [row for row in rows_all if isinstance(row, (list, tuple)) and len(row) >= 7 and start_ms <= int(row[0]) < exit_ms]
                entry_best = None
                if pre_entry:
                    for row in pre_entry:
                        at_ms = int(row[0]); high = safe_float(row[2]); low = safe_float(row[3])
                        if high is None or low is None: continue
                        candidate = low if trade.side == "LONG" else high
                        improvement = ((trade.entry_price-candidate)/trade.entry_price*100.0) if trade.side == "LONG" else ((candidate-trade.entry_price)/trade.entry_price*100.0)
                        if improvement > 0 and (entry_best is None or improvement > entry_best["improvement_pct"]):
                            entry_best = {"price": candidate, "at": at_ms, "improvement_pct": improvement}
                entry_potential_gross = (trade.entry_price-entry_best["price"])*trade.quantity if entry_best and trade.side == "LONG" else ((entry_best["price"]-trade.entry_price)*trade.quantity if entry_best else None)
                horizon_summary = {}
                for horizon, window in audit_windows.items():
                    last_at = window["last_at"]
                    first_at = window["first_at"]
                    expected = horizon * 60
                    coverage_pct = min(100.0, float(window["count"]) / max(1, expected) * 100.0)
                    complete = bool(first_at and last_at and first_at <= exit_ms + 60000
                        and last_at >= exit_ms + horizon * 3600000 - 60000
                        and window["max_gap_ms"] <= 120000 and coverage_pct >= 98.0)
                    checkpoint_at = window["checkpoint_at"]
                    horizon_summary[f"{horizon}h"] = {
                        "complete": complete,
                        "coverage_minutes": round(max(0.0, ((last_at or exit_ms) - exit_ms) / 60000.0), 2),
                        "coverage_pct": round(coverage_pct, 3),
                        "candle_count": window["count"],
                        "max_gap_seconds": round(window["max_gap_ms"] / 1000.0, 2),
                        "gap_count_over_2m": window["gap_count"],
                        "checkpoint_at": datetime.fromtimestamp(checkpoint_at / 1000, timezone.utc).isoformat() if checkpoint_at else None,
                        "checkpoint_price": window["checkpoint_price"],
                        "checkpoint_net_pnl_estimate": round(window["checkpoint_net"], 8) if window["checkpoint_net"] is not None else None,
                        "max_close_net_pnl_estimate": round(window["max_close_net"], 8) if window["max_close_net"] is not None else None,
                        "min_close_net_pnl_estimate": round(window["min_close_net"], 8) if window["min_close_net"] is not None else None,
                        "max_intrabar_net_pnl_estimate": round(window["max_intrabar_net"], 8) if window["max_intrabar_net"] is not None else None,
                        "min_intrabar_net_pnl_estimate": round(window["min_intrabar_net"], 8) if window["min_intrabar_net"] is not None else None,
                        "first_net_profit_at": datetime.fromtimestamp(window["first_net_profit_at"] / 1000, timezone.utc).isoformat() if window["first_net_profit_at"] else None,
                        "first_net_profit_minutes": round((window["first_net_profit_at"] - exit_ms) / 60000.0, 2) if window["first_net_profit_at"] else None,
                        "status": "COMPLETE" if complete else "PARTIAL_WINDOW_NOT_COUNTED_AS_FINAL"
                    }
                recovered_profit = best_close["net"] > 0
                intrabar_only = not recovered_profit and best_intrabar["net"] > 0
                recovered_entry = first_entry is not None
                partial_attempts = int(raw.get("post_exit_audit_partial_attempts", 0) or 0) + 1
                twelve_hour_complete = bool(horizon_summary["12h"]["complete"])
                all_candle_windows_complete = bool(twelve_hour_complete and lifecycle_complete)
                partial_terminal = bool(not all_candle_windows_complete and closed_at and (now() - closed_at).total_seconds() >= 12 * 3600 and partial_attempts >= 3)
                audit_status = "COMPLETE" if all_candle_windows_complete else "INCOMPLETE_WINDOW_FINAL" if partial_terminal else "PARTIAL_WINDOW"
                status = ("RECOVERED_TO_NET_PROFIT_CLOSE_CONFIRMED" if recovered_profit else
                          "INTRABAR_NET_PROFIT_ONLY" if intrabar_only else
                          "RECOVERED_TO_ENTRY_CLOSE_CONFIRMED" if recovered_entry else "NO_ENTRY_RECOVERY")
                raw.update({
                    "post_exit_recovery_audit_version": 9,
                    "post_exit_recovery_audit_updated_at": now().isoformat(),
                    "post_exit_recovery_audit_status": audit_status,
                    "post_exit_audit_partial_attempts": partial_attempts,
                    "post_exit_audit_windows": horizon_summary,
                    "closed_trade_candle_audit": lifecycle_audit,
                    "post_exit_audit_window_hours": [1, 4, 6, 12],
                    "post_exit_audit_all_windows_complete": all_candle_windows_complete,
                    "post_exit_audit_candle_count": sum(1 for row in rows_all if isinstance(row, (list, tuple)) and len(row) >= 7 and exit_ms <= int(row[0]) <= end_ms),
                    "post_exit_audit_candle_source": "BINANCE_USDM_MARK_PRICE_1M",
                    "funding_history_replay_verified": funding_verified_replay,
                    "funding_history_replay_expected_rows": funding_expected,
                    "funding_history_replay_rows": len(funding_points),
                    "funding_history_replay_source": ("NO_SETTLEMENT_SINCE_ENTRY" if funding_expected == 0 and next_funding else "BINANCE_FUNDING_RATE_HISTORY" if funding_verified_replay else "CONSERVATIVE_ENTRY_RATE_ESTIMATE"),
                    "funding_history_replay_points": funding_points,
                    "new_exit_engine_audit_v1": None,
                    "new_exit_engine_audit_version": None,
                    "post_exit_mark_candles": [
                        [int(row[0]), round(float(row[2]), 12), round(float(row[3]), 12), round(float(row[4]), 12)]
                        for row in rows_all
                        if isinstance(row, (list, tuple)) and len(row) >= 7
                        and exit_ms <= int(row[0]) <= end_ms
                        and safe_float(row[2]) is not None and safe_float(row[3]) is not None
                        and safe_float(row[4]) is not None
                    ],
                    "entry_timing_mark_candles": [
                        [int(row[0]), round(float(row[2]), 12), round(float(row[3]), 12), round(float(row[4]), 12)]
                        for row in rows_all
                        if isinstance(row, (list, tuple)) and len(row) >= 7
                        and start_ms <= int(row[0]) < exit_ms
                        and safe_float(row[2]) is not None and safe_float(row[3]) is not None
                        and safe_float(row[4]) is not None
                    ],
                    "entry_timing_window_minutes": entry_window_minutes,
                    "entry_timing_best_price": entry_best["price"] if entry_best else None,
                    "entry_timing_best_at": datetime.fromtimestamp(entry_best["at"]/1000, timezone.utc).isoformat() if entry_best else None,
                    "entry_timing_improvement_pct": round(entry_best["improvement_pct"], 8) if entry_best else None,
                    "entry_timing_potential_gross_pnl": round(entry_potential_gross, 8) if entry_potential_gross is not None else None,
                    "entry_timing_verdict": ("HINDSIGHT_BETTER_ENTRY_VISIBLE" if entry_best else "NO_MATERIAL_PRE_ENTRY_IMPROVEMENT_VISIBLE") if pre_entry else "NO_PRE_ENTRY_CANDLES",
                    "post_exit_candle_interval": "1m",
                    "post_exit_candle_source_verified": "BINANCE_USDM_MARK_PRICE_1M",
                    "post_exit_recovery_status": status,
                    "post_exit_recovery_market_source": "BINANCE_USDM_MARK_PRICE_1M_CLOSE_CONFIRMED",
                    "post_exit_recovery_horizon_hours": horizon_hours,
                    "post_exit_max_net_pnl": round(best_close["net"], 8),
                    "post_exit_max_gross_pnl": round(best_close["gross"], 8),
                    "post_exit_max_price": best_close["price"],
                    "post_exit_max_at": datetime.fromtimestamp(best_close["at"]/1000, timezone.utc).isoformat(),
                    "post_exit_max_funding_estimate": round(best_close["funding"], 8),
                    "post_exit_intrabar_max_net_pnl": round(best_intrabar["net"], 8),
                    "post_exit_intrabar_max_price": best_intrabar["price"],
                    "post_exit_intrabar_max_at": datetime.fromtimestamp(best_intrabar["at"]/1000, timezone.utc).isoformat(),
                    "post_exit_return_to_entry_at": datetime.fromtimestamp(first_entry/1000, timezone.utc).isoformat() if first_entry else None,
                    "post_exit_minutes_to_entry": round((first_entry-exit_ms)/60000, 2) if first_entry else None,
                    "post_exit_first_net_profit_at": datetime.fromtimestamp(first_profit/1000, timezone.utc).isoformat() if first_profit else None,
                    "post_exit_minutes_to_net_profit": round((first_profit-exit_ms)/60000, 2) if first_profit else None,
                    "post_exit_intrabar_first_net_profit_at": datetime.fromtimestamp(first_intrabar_profit/1000, timezone.utc).isoformat() if first_intrabar_profit else None,
                    "post_exit_intrabar_minutes_to_net_profit": round((first_intrabar_profit-exit_ms)/60000, 2) if first_intrabar_profit else None,
                    "post_exit_minutes_to_best": round((best_close["at"]-exit_ms)/60000, 2),
                    "post_exit_observed_minutes": round((last_valid_ms-exit_ms)/60000, 2),
                    "post_exit_worst_net_pnl": round(worst["net"], 8) if worst else None,
                    "post_exit_worst_at": datetime.fromtimestamp(worst["at"]/1000, timezone.utc).isoformat() if worst else None,
                    "post_exit_recovery_profit_usdt": round(max(0.0, best_close["net"]), 8),
                    "post_exit_intrabar_potential_profit_usdt": round(max(0.0, best_intrabar["net"]), 8),
                    "post_exit_cost_assumption": "Current configured taker fee and slippage; historic settings were not stored.",
                    "post_exit_funding_assumption": "Verified Binance fundingRate history where all expected settlements are present; otherwise conservative absolute entry-rate debit. This is distinct from mark-price candle coverage.",
                    "post_exit_old_exit_reason": trade.exit_reason,
                    "post_exit_recovery_label": f"Post-exit peak {best_close['net']:+.4f} USDT at +{(best_close['at']-exit_ms)/60000:.0f}m; source Binance mark-price 1m close; old exit {trade.exit_reason}"
                })
                if isinstance(legacy_engine_cache, dict):
                    raw["new_exit_engine_audit_v1"] = legacy_engine_cache
                    raw["new_exit_engine_audit_version"] = legacy_engine_version
                    raw["new_exit_engine_audit_source_audit_updated_at"] = raw.get("post_exit_recovery_audit_updated_at")
                with SessionLocal() as db:
                    live = db.get(PaperTrade, trade.id)
                    if live:
                        live.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
                        if not db.scalar(select(PaperTradeEvent.id).where(
                            PaperTradeEvent.paper_trade_id == trade.id,
                            PaperTradeEvent.event_type.in_(["POST_EXIT_RECOVERY_AUDIT_V7", "POST_EXIT_RECOVERY_AUDIT_V8", "POST_EXIT_RECOVERY_AUDIT_V9"])
                        ).limit(1)):
                            db.add(PaperTradeEvent(paper_trade_id=trade.id, event_type="POST_EXIT_RECOVERY_AUDIT_V9",
                                                   observed_at=live.closed_at or now(), value=best_close["net"],
                                                   detail=raw["post_exit_recovery_label"] + " · Entry timing audit: " + str(raw.get("entry_timing_verdict"))))
                        db.commit()
                audited += 1
                recovered += int(recovered_profit)
            except Exception as exc:
                failures += 1
                with SessionLocal() as db:
                    live = db.get(PaperTrade, trade.id)
                    if live:
                        latest = parse_json(live.raw_json)
                        latest["post_exit_audit_last_attempt_at"] = now().isoformat()
                        latest["post_exit_audit_last_error"] = str(exc)[:500]
                        live.raw_json = json.dumps(latest, separators=(",", ":"), default=str)
                        db.commit()
                if binance_rest_status().get("blocked"):
                    break
                log_check("post_exit_recovery_audit", False, None, None, f"trade={trade.id} {trade.symbol}: {str(exc)[:500]}")
        with SessionLocal() as db:
            after = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED")).all()
        pending_count = sum(1 for t in after if parse_json(t.raw_json).get("post_exit_recovery_audit_version", 0) != 9 or not parse_json(t.raw_json).get("post_exit_recovery_audit_updated_at") or parse_json(t.raw_json).get("post_exit_recovery_audit_status") == "PARTIAL_WINDOW")
        cooldown = binance_rest_status()
        return {"audited": audited, "attempted": attempted, "recovered_to_profit": recovered,
                "failures": failures, "blocked": bool(cooldown.get("blocked")),
                "blocked_until": cooldown.get("blocked_until"), "remaining_seconds": cooldown.get("remaining_seconds"),
                "pending": pending_count,
                "status": "WAITING_FOR_BINANCE_REST_COOLDOWN" if cooldown.get("blocked") else "COMPLETE" if pending_count == 0 else "PARTIAL"}


def _shadow_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def evaluate_open_paper_trades(
    prices: dict[str, float] | None = None,
    source: str = "BINANCE_REST",
    event_times: dict[str, datetime] | None = None
) -> None:
    with SessionLocal() as db:
        open_trades = db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN").limit(500)).all()
        symbols = sorted({t.symbol for t in open_trades})
    prices = prices or await _mark_prices(symbols)
    cfg = paper_config()
    ts = now()
    with SessionLocal() as db:
        trades = db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN")).all()
        for trade in trades:
            mark = prices.get(trade.symbol)
            if mark is None or mark <= 0:
                continue
            prev_check = None
            raw = parse_json(trade.raw_json)
            # Correct a stale cached nextFundingTime against the actual paper-entry
            # timestamp. This prevents charging a funding settlement that occurred
            # before the simulation existed.
            next_funding_ms_at_entry = safe_float(raw.get("next_funding_time"))
            if next_funding_ms_at_entry:
                opened_at = trade.opened_at
                if opened_at.tzinfo is None:
                    opened_at = opened_at.replace(tzinfo=timezone.utc)
                opened_ms = opened_at.timestamp() * 1000
                while next_funding_ms_at_entry <= opened_ms:
                    next_funding_ms_at_entry += 28800000
                raw["next_funding_time"] = next_funding_ms_at_entry
            if raw.get("last_binance_price_at"):
                try:
                    prev_check = datetime.fromisoformat(raw["last_binance_price_at"].replace("Z", "+00:00"))
                    if prev_check.tzinfo is None:
                        prev_check = prev_check.replace(tzinfo=timezone.utc)
                except Exception:
                    prev_check = None
            price_observed_at = (event_times or {}).get(trade.symbol) or ts
            if price_observed_at.tzinfo is None:
                price_observed_at = price_observed_at.replace(tzinfo=timezone.utc)
            gap = max(0.0, (price_observed_at - prev_check).total_seconds()) if prev_check else 0.0
            gross, costs, net, pnl_pct = _paper_pnl(trade, mark, cfg, raw, ts)
            raw["last_binance_price_at"] = price_observed_at.isoformat()
            raw["last_price_received_at"] = ts.isoformat()
            raw["last_paper_check_at"] = ts.isoformat()
            raw["last_price_source"] = source
            raw["last_data_gap_seconds"] = gap
            raw["data_gap_seconds"] = max(float(raw.get("data_gap_seconds", 0.0) or 0.0), gap)
            raw["max_data_gap_seconds"] = max(float(raw.get("max_data_gap_seconds", 0.0) or 0.0), gap)
            trade.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
            trade.mark_price, trade.gross_pnl, trade.estimated_costs, trade.net_pnl = mark, gross, costs, net

            path_row = PaperTradePath(
                paper_trade_id=trade.id, observed_at=price_observed_at, paper_price=mark,
                gross_pnl=gross, net_pnl=net, pnl_pct=pnl_pct,
                source=source, data_gap_seconds=gap
            )
            leader = _leader_for_trade(db, trade)
            if leader:
                lentry, lmark = leader.entry_price, leader.mark_price
                if lmark:
                    ladverse = ((lentry-lmark)/lentry*100) if trade.side=="LONG" else ((lmark-lentry)/lentry*100) if lentry else None
                    path_row.leader_mark = lmark
                    path_row.leader_unrealized_pnl = leader.unrealized_pnl
                    path_row.leader_adverse_pct = ladverse
                    raw["last_leader_check_at"] = ts.isoformat()
                    raw["leader_position_key"] = leader.position_key
                    raw["leader_mark"] = lmark
                    raw["leader_status"] = "OBSERVED"
                else:
                    raw["leader_status"] = "OBSERVED_NO_MARK"
            else:
                raw["leader_status"] = "STALE_OR_NOT_OBSERVED"
            # Persist a compact 5-second path, while the live row itself is updated every second.
            last_path = db.scalar(select(PaperTradePath).where(PaperTradePath.paper_trade_id==trade.id).order_by(desc(PaperTradePath.id)).limit(1))
            last_path_at = last_path.observed_at if last_path else None
            if last_path_at is not None and last_path_at.tzinfo is None:
                last_path_at = last_path_at.replace(tzinfo=timezone.utc)
            should_sample = last_path_at is None or (ts-last_path_at).total_seconds() >= float(cfg.get("path_sample_seconds",5))
            if should_sample:
                db.add(path_row)
            raw.setdefault("mfe_net_pnl", net)
            raw.setdefault("mae_net_pnl", net)
            raw["mfe_net_pnl"] = max(float(raw["mfe_net_pnl"]), net)
            raw["mae_net_pnl"] = min(float(raw["mae_net_pnl"]), net)
            raw["mfe_pct"] = max(float(raw.get("mfe_pct",pnl_pct)), pnl_pct)
            raw["mae_pct"] = min(float(raw.get("mae_pct",pnl_pct)), pnl_pct)
            raw["meaningful_profit_hits"] = raw.get("meaningful_profit_hits", {})
            for step in cfg.get("meaningful_profit_steps", [0.1,0.2,0.3,0.5,0.7,1.0]):
                key = str(step)
                if pnl_pct >= float(step) and key not in raw["meaningful_profit_hits"]:
                    raw["meaningful_profit_hits"][key] = ts.isoformat()
                    db.add(PaperTradeEvent(paper_trade_id=trade.id,event_type=f"PROFIT_{key}PCT",observed_at=ts,value=net,detail=f"First observed net-margin profit >= {step}%"))
            raw["mfe_net_pnl"], raw["mae_net_pnl"] = max(float(raw["mfe_net_pnl"]), net), min(float(raw["mae_net_pnl"]), net)
            trade.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
            # Keep historical path quality separate from current monitor health.
            # Three fresh observations mean monitoring has resumed; the earlier missing
            # interval remains a permanent DATA_INTERRUPTED_REVIEW label.
            if gap > 15:
                if raw.get("data_quality_status") != "INFORMATION_CUTOFF":
                    raw["data_quality_status"] = "DATA_INTERRUPTED_REVIEW"
                    raw["data_quality_reason"] = (
                        "A price-feed interval exceeded 15 seconds. The missing historical "
                        "interval cannot be reconstructed; the financial outcome is preserved."
                    )
                raw["monitoring_status"] = "INTERRUPTED"
                raw["monitoring_interruption_started_at"] = ts.isoformat()
                raw["post_gap_confirmations"] = 0
                db.add(PaperTradeEvent(
                    paper_trade_id=trade.id, event_type="DATA_GAP",
                    observed_at=ts, value=net,
                    detail=f"Price feed gap {gap:.1f}s; a losing stop is deferred until three consecutive fresh updates."
                ))
            elif (
                raw.get("monitoring_status") == "INTERRUPTED"
                or (
                    raw.get("data_quality_status") == "DATA_INTERRUPTED_REVIEW"
                    and raw.get("monitoring_status") != "RESUMED_AFTER_GAP"
                )
            ):
                raw["post_gap_confirmations"] = int(raw.get("post_gap_confirmations", 0)) + 1
                if raw["post_gap_confirmations"] >= 3:
                    raw["monitoring_status"] = "RESUMED_AFTER_GAP"
                    raw["monitoring_resumed_at"] = ts.isoformat()
                    db.add(PaperTradeEvent(
                        paper_trade_id=trade.id, event_type="DATA_MONITOR_RESUMED",
                        observed_at=ts, value=net,
                        detail="Three fresh consecutive price observations received. Historical data-quality classification remains unchanged."
                    ))
            # Capture every sampled positive net amount. No minimum threshold: even
            # +0.0014 USDT is a realized paper exit once round-trip costs and funding are covered.
            gap_guard = (
                raw.get("monitoring_status") == "INTERRUPTED"
                and net <= 0
            )
            hit_target = mark >= trade.target_price if trade.side == "LONG" else mark <= trade.target_price
            leader_state, leader_state_detail = _leader_state_for_trade(db, trade)
            raw["leader_state_at_check"] = leader_state
            raw["leader_state_detail"] = leader_state_detail
            raw["leader_state_checked_at"] = ts.isoformat()
            exit_reason = None
            funding_verified = bool(raw.get("funding_history_verified"))
            next_funding_ms = safe_float(raw.get("next_funding_time"))
            # Before the next scheduled settlement, no funding event can yet have
            # accrued to this newly-opened paper position. This permits immediate
            # harvesting without an unnecessary REST funding-history round trip.
            if (not funding_verified and next_funding_ms
                    and ts.timestamp() * 1000 < next_funding_ms):
                raw["funding_cost_actual"] = 0.0
                raw["funding_history_verified"] = True
                raw["funding_history_checked_at"] = ts.isoformat()
                raw["funding_source"] = "NO_SETTLEMENT_SINCE_ENTRY"
                raw["funding_audit_status"] = "VERIFIED_ZERO_BEFORE_NEXT_SETTLEMENT"
                raw["funding_verification_error"] = None
                funding_verified = True
            funding_check_at = None
            if raw.get("funding_history_checked_at"):
                try:
                    funding_check_at = datetime.fromisoformat(raw["funding_history_checked_at"].replace("Z", "+00:00"))
                except Exception:
                    funding_check_at = None
            seconds_since_funding_check = (ts - funding_check_at).total_seconds() if funding_check_at else None
            next_funding_due = bool(next_funding_ms and ts.timestamp() * 1000 >= next_funding_ms)
            check_due = (
                (next_funding_due and (seconds_since_funding_check is None or seconds_since_funding_check >= 60))
                or ((net > 0 or hit_target) and (not funding_verified) and (seconds_since_funding_check is None or seconds_since_funding_check >= 60))
                or (net > 0 and funding_verified and seconds_since_funding_check is not None and seconds_since_funding_check >= 300)
            )
            if check_due:
                try:
                    actual_funding, funding_rows = await _actual_funding_cost(trade, ts)
                    expected_funding_rows = _funding_settlement_count(trade, raw, ts)
                    if funding_rows < expected_funding_rows:
                        raise RuntimeError(
                            f"Binance funding history incomplete: expected {expected_funding_rows} settlement rows, received {funding_rows}"
                        )
                    raw["funding_cost_actual"] = actual_funding
                    raw["funding_history_rows"] = funding_rows
                    raw["funding_history_checked_at"] = ts.isoformat()
                    raw["funding_history_verified"] = True
                    raw["funding_source"] = "BINANCE_FUNDING_HISTORY"
                    raw["funding_audit_status"] = "VERIFIED"
                    raw["funding_verification_error"] = None
                    round_trip_turnover = (
                        float(trade.entry_price) * float(trade.quantity)
                        + float(mark) * float(trade.quantity)
                    )
                    fee_and_slippage = round_trip_turnover * (
                        float(cfg["taker_fee_pct"]) / 100
                        + float(cfg["slippage_pct"]) / 100
                    )
                    costs = fee_and_slippage + actual_funding
                    net = gross - costs
                    pnl_pct = (net / max(trade.margin_used, 0.000001)) * 100
                    trade.estimated_costs, trade.net_pnl = costs, net
                    funding_verified = True
                    if next_funding_ms:
                        while next_funding_ms <= ts.timestamp() * 1000:
                            next_funding_ms += 28800000
                        raw["next_funding_time"] = next_funding_ms
                except Exception as funding_exc:
                    funding_verified = False
                    raw["funding_history_verified"] = False
                    raw["funding_audit_status"] = "PENDING_VERIFICATION"
                    raw["funding_history_checked_at"] = ts.isoformat()
                    raw["funding_verification_error"] = str(funding_exc)[:180]
                    last_pending_at = None
                    try:
                        if raw.get("funding_pending_event_at"):
                            last_pending_at = datetime.fromisoformat(raw["funding_pending_event_at"].replace("Z", "+00:00"))
                    except Exception:
                        last_pending_at = None
                    if last_pending_at is None or (ts - last_pending_at).total_seconds() >= 300:
                        db.add(PaperTradeEvent(
                            paper_trade_id=trade.id, event_type="FUNDING_CHECK_PENDING",
                            observed_at=ts, value=net,
                            detail="Funding history is temporarily unavailable. Live PnL includes a conservative debit estimate for settlements since entry; positive exits are allowed only when that estimated net remains positive."
                        ))
                        raw["funding_pending_event_at"] = ts.isoformat()
            current_mfe = max(float(raw.get("mfe_net_pnl", net)), net)
            giveback_pct = 0.0
            if current_mfe > 0:
                giveback_pct = max(0.0, (current_mfe - net) / current_mfe * 100.0)
            strategy = str(trade.strategy or raw.get("strategy") or "RECOVERY").upper()
            meaningful_mfe_multiple = 1.15 if strategy == "MOMENTUM" else 1.35
            cost_floor = max(float(costs or 0.0) * 1.25, 0.02)
            meaningful_mfe = current_mfe >= cost_floor * meaningful_mfe_multiple
            profit_lock = current_mfe >= cost_floor * meaningful_mfe_multiple and giveback_pct >= float(cfg.get("profit_giveback_pct", 35.0)) and net >= cost_floor
            target_or_min_profit = bool(hit_target and net >= cost_floor)
            raw["profit_management"] = {
                "mfe_net_pnl": current_mfe, "giveback_pct": giveback_pct,
                "meaningful_mfe_threshold_net": round(cost_floor * meaningful_mfe_multiple, 8),
                "meaningful_mfe_observed": meaningful_mfe,
                "target_hit": hit_target, "profit_lock_triggered": profit_lock,
                "mae_is_exit_signal": False
            }
            # Provider-confirmed leader closure/reversal is the hard lifecycle end.
            if leader_state == "REVERSED":
                exit_reason = "LEADER_POSITION_REVERSED"
            elif leader_state == "CLOSED_CONFIRMED":
                exit_reason = "LEADER_POSITION_CLOSED"
            elif net >= cost_floor and funding_verified and (meaningful_mfe or target_or_min_profit or profit_lock):
                exit_reason = "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK"
            elif net >= cost_floor and not funding_verified:
                # Funding REST may be blocked. _paper_pnl has already deducted
                # round-trip fees/slippage and a conservative funding debit for every
                # settlement since entry. Harvest if that estimated net remains positive;
                # preserve the pending audit flag so actual funding can be reconciled.
                funding_events_est = _funding_settlement_count(trade, raw, ts)
                funding_rate_known = (
                    safe_float(raw.get("funding_rate_at_entry")) is not None
                    or safe_float(PAPER_SYMBOL_FUNDING_RATE.get(trade.symbol.upper())) is not None
                )
                funding_schedule_known = bool(safe_float(raw.get("next_funding_time")))
                if funding_schedule_known and (funding_events_est == 0 or funding_rate_known):
                    target_or_meaningful_profit = bool(hit_target and net >= cost_floor)
                    profit_lock = meaningful_mfe and giveback_pct >= float(cfg.get("profit_giveback_pct", 35.0)) and net >= cost_floor
                    if target_or_meaningful_profit or profit_lock:
                        exit_reason = "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK_FUNDING_ESTIMATED"
                    elif meaningful_mfe:
                        exit_reason = "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK_FUNDING_ESTIMATED"
                    else:
                        exit_reason = None
                    raw["funding_audit_status"] = "PENDING_POST_CLOSE_RECONCILIATION"
                    raw["funding_source"] = "CONSERVATIVE_ESTIMATE_PENDING_BINANCE_HISTORY"
                    raw["funding_estimate_at_exit"] = max(0.0, costs - (
                        (float(trade.entry_price) + float(mark)) * float(trade.quantity)
                        * ((float(cfg["taker_fee_pct"]) + float(cfg["slippage_pct"])) / 100.0)
                    ))
                    raw["funding_estimate_settlement_count"] = funding_events_est
                    raw["realized_pnl_is_estimate"] = True
            if exit_reason is None and not gap_guard:
                opened_at = trade.opened_at.replace(tzinfo=timezone.utc) if trade.opened_at.tzinfo is None else trade.opened_at
                held_hours = max(0.0, (ts - opened_at).total_seconds() / 3600.0)
                max_hold_hours = float(cfg.get("momentum_max_hold_hours", 24.0) if strategy == "MOMENTUM"
                                       else cfg.get("recovery_max_hold_hours", 72.0))
                structure_broken, structure_detail = _paper_structure_break(
                    db, trade, mark, price_observed_at, cfg, raw, should_sample
                )
                raw["market_structure_at_check"] = structure_detail
                if structure_broken:
                    exit_reason = "MARKET_STRUCTURE_BROKEN"
                elif held_hours >= max_hold_hours:
                    exit_reason = "MAX_HOLD_TIME_EXCEEDED"
                raw["hold_management"] = {
                    "held_hours": round(held_hours, 4), "max_hold_hours": max_hold_hours,
                    "exit_due_to_mae": False,
                }
            # Keep the persisted last path point and ledger aligned with the funding-
            # verified net amount used by the actual simulated exit decision.
            path_row.net_pnl = net
            path_row.pnl_pct = pnl_pct
            trade.estimated_costs = costs
            trade.net_pnl = net
            if exit_reason:
                trade.status = "CLOSED"
                trade.exit_price = mark
                trade.exit_reason = exit_reason
                trade.closed_at = ts
                raw["realized_net_pnl"] = net
                raw["realized_exit_price"] = mark
                raw["realized_exit_at"] = ts.isoformat()
                db.add(PaperTradeEvent(paper_trade_id=trade.id,event_type="REALIZED_EXIT",observed_at=ts,value=net,detail=exit_reason))
            raw["funding_history_verified"] = bool(funding_verified)
            raw["funding_audit_status"] = (
                raw.get("funding_audit_status")
                or ("VERIFIED" if funding_verified else "PENDING_VERIFICATION")
            )
            raw["last_net_pnl_calculated_at"] = ts.isoformat()
            raw["last_net_pnl_source"] = source
            trade.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
        db.commit()

async def _open_paper_symbols() -> list[str]:
    with SessionLocal() as db:
        open_symbols = {
            str(trade.symbol).upper()
            for trade in db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN")).all()
            if trade.symbol
        }
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).where(
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).group_by(PositionSnapshot.trader_id).subquery()
        latest = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
        visible_ids = [snap.id for snap in latest if not snap.hidden and is_binance_snapshot(snap)]
        leader_symbols = set(db.scalars(
            select(PositionObservation.symbol).where(PositionObservation.snapshot_id.in_(visible_ids))
        ).all()) if visible_ids else set()
        return sorted(open_symbols | {str(symbol).upper() for symbol in leader_symbols if symbol})


async def _ws_send_symbol_action(ws, action: str, symbols: set[str], request_id: int, pending: dict) -> int:
    # Binance limits client-to-server WebSocket control messages; pace below 5/sec.
    for symbol in sorted(symbols):
        request_id += 1
        stream = f"{symbol.lower()}@markPrice@1s"
        await ws.send(json.dumps({"method": action, "params": [stream], "id": request_id}))
        pending[request_id] = {"action": action, "symbol": symbol}
        await asyncio.sleep(0.23)
    return request_id


async def paper_price_worker() -> None:
    """
    Persistent USD-M mark-price subscriber:
    - /market/stream + per-symbol SUBSCRIBE prevents one invalid futures symbol poisoning a combined URL.
    - Price frames are batched and evaluated together, not one blocking DB transaction per tick.
    - New open symbols are subscribed within five seconds.
    - REST backup is one bulk request/minute and is disabled during a Binance 418/429 cooldown.
    """
    reconnect_delay = 2.0
    last_rest_backup_at = 0.0
    last_health_log_at = 0.0
    request_id = 0

    while True:
        connection_started = time.monotonic()
        received_price_this_connection = False
        try:
            symbols = set(await _open_paper_symbols())
            if not symbols:
                PAPER_PRICE_STREAM_STATE.update({
                    "status": "idle_no_open_trades", "connected": False,
                    "subscribed_symbols": [], "last_error": None
                })
                await asyncio.sleep(2.0)
                continue

            PAPER_PRICE_STREAM_STATE.update({
                "status": "connecting", "connected": False,
                "subscribed_symbols": sorted(symbols), "last_error": None
            })
            pending_requests: dict[int, dict] = {}
            requested_symbols: set[str] = set()
            active_symbols: set[str] = set()
            unsupported_symbols: set[str] = set()
            pending_prices: dict[str, float] = {}
            pending_event_times: dict[str, datetime] = {}
            connection_started = time.monotonic()
            last_price_message_at = 0.0
            last_flush_at = time.monotonic()
            last_symbol_refresh_at = time.monotonic()
            connection_id = int(PAPER_PRICE_STREAM_STATE.get("reconnect_count", 0)) + 1

            # Current USDⓈ-M API requires the market stream control endpoint
            # for dynamic SUBSCRIBE/UNSUBSCRIBE requests. The legacy /ws base can
            # accept a socket but does not serve this control-subscription channel.
            async with websockets.connect(
                "wss://fstream.binance.com/market/stream",
                ping_interval=20, ping_timeout=20, close_timeout=5,
                open_timeout=12, max_size=1048576, max_queue=4096
            ) as ws:
                PAPER_PRICE_STREAM_STATE.update({
                    "status": "subscribing", "connected": True,
                    "connected_at": now().isoformat(), "reconnect_count": connection_id,
                    "last_error": None
                })
                request_id = await _ws_send_symbol_action(
                    ws, "SUBSCRIBE", symbols, request_id, pending_requests
                )
                requested_symbols.update(symbols)

                while time.monotonic() - connection_started < 23 * 3600:
                    message = None
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=0.25)
                    except asyncio.TimeoutError:
                        pass

                    if message is not None:
                        packet = json.loads(message)
                        if isinstance(packet, dict):
                            PAPER_PRICE_STREAM_STATE["messages_received"] = int(
                                PAPER_PRICE_STREAM_STATE.get("messages_received", 0)
                            ) + 1
                            PAPER_PRICE_STREAM_STATE["last_message_at"] = now().isoformat()
                            ack_id = packet.get("id")
                            try:
                                ack_id = int(ack_id) if ack_id is not None else None
                            except (TypeError, ValueError):
                                ack_id = None
                            if ack_id is not None and ack_id in pending_requests:
                                request = pending_requests.pop(ack_id)
                                symbol = request["symbol"]
                                if packet.get("code") is not None or packet.get("error"):
                                    unsupported_symbols.add(symbol)
                                    requested_symbols.discard(symbol)
                                    PAPER_PRICE_STREAM_STATE["last_error"] = (
                                        f"{symbol} {request['action']} rejected: "
                                        f"{packet.get('msg') or packet.get('error') or packet.get('code')}"
                                    )
                                elif request["action"] == "SUBSCRIBE":
                                    active_symbols.add(symbol)
                                    if not last_price_message_at:
                                        last_price_message_at = time.monotonic()
                                    PAPER_PRICE_STREAM_STATE["status"] = "live"
                                else:
                                    active_symbols.discard(symbol)
                                PAPER_PRICE_STREAM_STATE["subscribed_symbols"] = sorted(active_symbols)
                                PAPER_PRICE_STREAM_STATE["unsupported_symbols"] = sorted(unsupported_symbols)
                                continue

                            data = packet.get("data", packet)
                            if isinstance(data, dict):
                                symbol = str(data.get("s") or "").upper()
                                price = safe_float(data.get("p"))
                                if symbol and price is not None and price > 0 and symbol in requested_symbols:
                                    event_ms = safe_float(data.get("E"))
                                    event_at = (
                                        datetime.fromtimestamp(event_ms / 1000.0, timezone.utc)
                                        if event_ms else now()
                                    )
                                    pending_prices[symbol] = price
                                    pending_event_times[symbol] = event_at
                                    event_epoch = event_at.timestamp()
                                    PAPER_SYMBOL_LAST_PRICE[symbol] = price
                                    PAPER_SYMBOL_LAST_PRICE_AT[symbol] = event_epoch
                                    funding_value = safe_float(data.get("r"))
                                    index_value = safe_float(data.get("i"))
                                    if funding_value is not None:
                                        PAPER_SYMBOL_FUNDING_RATE[symbol] = funding_value
                                    if index_value is not None:
                                        PAPER_SYMBOL_INDEX_PRICE[symbol] = index_value
                                    history = PAPER_SYMBOL_PRICE_HISTORY.setdefault(symbol, [])
                                    history.append((event_epoch, price))
                                    cutoff_epoch = event_epoch - 600
                                    while history and history[0][0] < cutoff_epoch:
                                        history.pop(0)
                                    last_price_message_at = time.monotonic()
                                    received_price_this_connection = True
                                    PAPER_SYMBOL_LAST_UPDATE[symbol] = event_at.isoformat()
                                    PAPER_PRICE_STREAM_STATE["price_updates_received"] = int(
                                        PAPER_PRICE_STREAM_STATE.get("price_updates_received", 0)
                                    ) + 1
                                    PAPER_PRICE_STREAM_STATE["last_price_at"] = event_at.isoformat()
                                    PAPER_PRICE_STREAM_STATE["status"] = "live"

                    mono = time.monotonic()
                    if pending_prices and mono - last_flush_at >= 0.5:
                        batch_prices = dict(pending_prices)
                        batch_times = dict(pending_event_times)
                        pending_prices.clear()
                        pending_event_times.clear()
                        await evaluate_open_paper_trades(
                            batch_prices, "BINANCE_WS_MARK_PRICE_1S", batch_times
                        )
                        last_flush_at = mono
                        PAPER_PRICE_STREAM_STATE["last_flush_at"] = now().isoformat()

                    if mono - last_symbol_refresh_at >= 5.0:
                        current_symbols = set(await _open_paper_symbols())
                        removed = requested_symbols - current_symbols
                        added = current_symbols - requested_symbols - unsupported_symbols
                        if removed:
                            request_id = await _ws_send_symbol_action(
                                ws, "UNSUBSCRIBE", removed, request_id, pending_requests
                            )
                            requested_symbols -= removed
                            active_symbols -= removed
                        if added:
                            request_id = await _ws_send_symbol_action(
                                ws, "SUBSCRIBE", added, request_id, pending_requests
                            )
                            requested_symbols.update(added)
                        last_symbol_refresh_at = time.monotonic()
                        PAPER_PRICE_STREAM_STATE["subscribed_symbols"] = sorted(active_symbols)

                    if active_symbols and last_price_message_at and mono - last_price_message_at > 12:
                        PAPER_PRICE_STREAM_STATE.update({
                            "status": "stale_no_mark_messages",
                            "last_error": "No mark-price event received for more than 12 seconds; reconnecting."
                        })
                        break
                    if not active_symbols and mono - connection_started > 12:
                        PAPER_PRICE_STREAM_STATE.update({
                            "status": "no_valid_subscriptions",
                            "last_error": "No active USD-M mark-price subscriptions were acknowledged."
                        })
                        break

                    if mono - last_health_log_at >= 60:
                        last_health_log_at = mono
                        log_check(
                            "paper_price_stream", True, None, None,
                            f"connected={bool(active_symbols)} active_symbols={len(active_symbols)} "
                            f"price_updates={PAPER_PRICE_STREAM_STATE.get('price_updates_received', 0)}"
                        )

                if pending_prices:
                    await evaluate_open_paper_trades(
                        dict(pending_prices), "BINANCE_WS_MARK_PRICE_1S", dict(pending_event_times)
                    )
            PAPER_PRICE_STREAM_STATE["connected"] = False
            if received_price_this_connection and time.monotonic() - connection_started >= 60:
                reconnect_delay = 2.0
            else:
                reconnect_delay = min(30.0, reconnect_delay * 2)

        except asyncio.CancelledError:
            PAPER_PRICE_STREAM_STATE.update({"status": "stopped", "connected": False})
            raise
        except Exception as exc:
            PAPER_PRICE_STREAM_STATE.update({
                "status": "reconnecting", "connected": False,
                "last_error": str(exc)[:350],
                "reconnect_count": int(PAPER_PRICE_STREAM_STATE.get("reconnect_count", 0)) + 1
            })
            if time.monotonic() - last_health_log_at >= 30:
                last_health_log_at = time.monotonic()
                log_check("paper_price_stream", False, None, None, str(exc)[:850])
            if received_price_this_connection and time.monotonic() - connection_started >= 60:
                reconnect_delay = 2.0
            else:
                reconnect_delay = min(30.0, reconnect_delay * 2)

        # Backup only when the WebSocket is unavailable: a single bulk REST read
        # at most once per minute, and never while Binance has an active 418/429 ban.
        if time.monotonic() - last_rest_backup_at >= 60:
            last_rest_backup_at = time.monotonic()
            current_symbols = await _open_paper_symbols()
            if current_symbols and time.time() >= BINANCE_REST_BLOCKED_UNTIL:
                try:
                    backup_prices = await _mark_prices(current_symbols)
                    if backup_prices:
                        await evaluate_open_paper_trades(backup_prices, "BINANCE_REST_BULK_BACKUP")
                except Exception as backup_exc:
                    log_check("binance_rest_backup", False, None, None, str(backup_exc)[:500])

        await asyncio.sleep(reconnect_delay)



@app.get("/api/opportunities")
async def api_opportunities(limit: int = 100, strategy: str = ""):
    # Read-only UI calls are strategy-scoped, cache-friendly, REST-free and never create trades.
    if strategy and strategy.upper() not in {"RECOVERY", "MOMENTUM"}:
        raise HTTPException(400, "strategy must be RECOVERY or MOMENTUM")
    return await scan_paper_opportunities(limit=limit, allow_entry_creation=False, strategy=strategy.upper() or None)


async def scan_paper_opportunities(
    limit: int = 100, allow_entry_creation: bool = True, strategy: str | None = None
):
    strategies = [strategy.upper()] if strategy else ["RECOVERY", "MOMENTUM"]
    if any(item not in {"RECOVERY", "MOMENTUM"} for item in strategies):
        raise ValueError("strategy must be RECOVERY or MOMENTUM")
    if not allow_entry_creation:
        fresh = all(time.monotonic() - opportunity_result_cache[item]["at"] <= OPPORTUNITY_CACHE_TTL_SECONDS
                    for item in strategies)
        if fresh:
            cached = [row for item in strategies for row in opportunity_result_cache[item]["items"]]
            return cached[:max(1, min(int(limit), 2000))]
    results = []
    for item in strategies:
        async with opportunity_strategy_locks[item]:
            rows = await _api_opportunities_locked(limit, allow_entry_creation, [item])
            opportunity_result_cache[item] = {"at": time.monotonic(), "items": rows}
            results.extend(rows)
    return results[:max(1, min(int(limit), 2000))]


async def _api_opportunities_locked(
    limit: int = 2000, allow_entry_creation: bool = False,
    strategies: list[str] | None = None
):
    limit = max(1, min(limit, 2000))
    strategies = strategies or ["RECOVERY", "MOMENTUM"]
    with SessionLocal() as db:
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).where(
            PositionSnapshot.raw_json.like(f'%"source":"{BINANCE_COPY_SOURCE}"%')
        ).group_by(PositionSnapshot.trader_id).subquery()
        latest_snapshots = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
        visible = {snap.id: snap for snap in latest_snapshots if not snap.hidden and is_binance_snapshot(snap)}
        observations = db.scalars(
            select(PositionObservation).where(PositionObservation.snapshot_id.in_(list(visible.keys())))
            .order_by(desc(PositionObservation.id))
        ).all() if visible else []
        traders = {t.trader_id: t.name for t in db.scalars(select(Trader)).all()}
    latest = {}
    for obs in observations:
        key = f"{obs.trader_id}|{obs.position_key}"
        if key not in latest:
            latest[key] = obs
    news = await fetch_news_items() if allow_entry_creation else news_cache["items"]
    result = []
    deferred: list = []
    for obs in list(latest.values())[:limit]:
        snapshot = visible.get(obs.snapshot_id)
        observed = snapshot.observed_at.isoformat() if snapshot else None
        if "RECOVERY" in strategies:
            result.append(await build_opportunity(obs, observed, traders.get(obs.trader_id), news, allow_entry_creation=allow_entry_creation,
                                                  deferred=deferred if allow_entry_creation else None))
        if "MOMENTUM" in strategies:
            result.append(await build_momentum_opportunity(obs, observed, traders.get(obs.trader_id), news, allow_entry_creation=allow_entry_creation))
    # Enter the best recovery candidates first so wallet limits are spent on the highest-potential ones.
    for _score, finalize, _holder in sorted(deferred, key=lambda item: item[0], reverse=True):
        finalize()
    return sorted(result, key=lambda x: (x["qualified"], x.get("recovery_score") if x.get("recovery_score") is not None else x.get("leader_profit_pct", x.get("adverse_pct", 0)) or 0), reverse=True)


def paper_exit_reason_display(trade: PaperTrade, raw: dict) -> str:
    reason = trade.exit_reason or ""
    labels = {
        "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK": "Meaningful MFE captured after costs",
        "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK_FUNDING_ESTIMATED": "Meaningful MFE captured; conservative funding estimate pending audit",
        "LEADER_POSITION_CLOSED": "Leader position confirmed closed",
        "LEADER_POSITION_REVERSED": "Leader position reversed",
        "MARKET_STRUCTURE_BROKEN": "Confirmed market structure break",
        "MAX_HOLD_TIME_EXCEEDED": "Strategy-specific maximum hold reached",
    }
    return labels.get(reason, reason.replace("_", " ").title() if reason else "Not closed")


@app.get("/api/paper")
async def api_paper(strategy: str = "RECOVERY"):
    strategy = strategy.upper().strip() or "RECOVERY"
    if strategy not in {"RECOVERY", "MOMENTUM"}:
        raise HTTPException(400, "strategy must be RECOVERY or MOMENTUM")
    cfg = paper_config()
    with SessionLocal() as db:
        trades = db.scalars(select(PaperTrade).where(PaperTrade.strategy == strategy).order_by(desc(PaperTrade.opened_at)).limit(1000)).all()
        # Portfolio totals and risk clusters must use the complete ledger, not only
        # the newest 300 rows rendered in the table.
        shadow_trades = db.scalars(
            select(PaperTrade).where(PaperTrade.strategy == strategy).order_by(PaperTrade.opened_at, PaperTrade.id)
        ).all()
        all_portfolio_trades = db.scalars(
            select(PaperTrade).order_by(PaperTrade.opened_at, PaperTrade.id)
        ).all()
        gap_rows = db.execute(
            select(PaperTradePath.paper_trade_id, func.max(PaperTradePath.data_gap_seconds))
            .group_by(PaperTradePath.paper_trade_id)
        ).all()
        max_gaps = {int(trade_id): float(gap or 0.0) for trade_id, gap in gap_rows}
        leader_checks = {}
        for open_trade in [item for item in shadow_trades if item.status == "OPEN"]:
            leader_state, leader_detail = _leader_state_for_trade(db, open_trade)
            latest_visible = _leader_snapshot_for_trade(db, open_trade)
            leader_checks[open_trade.id] = {
                "status": leader_state, "detail": leader_detail,
                "checked_at": latest_visible.observed_at.isoformat() if latest_visible else None,
                "source": ("LATEST_VISIBLE_BINANCE_PUBLIC_SNAPSHOT" if _trade_uses_binance_source(open_trade)
                           else "LATEST_VISIBLE_LEGACY_SNAPSHOT") if latest_visible else "NO_MATCHING_SOURCE_SNAPSHOT"
            }

    open_trades = [trade for trade in shadow_trades if trade.status == "OPEN"]
    closed_trades = [trade for trade in shadow_trades if trade.status == "CLOSED"]
    wallet_epoch = datetime.fromisoformat(str(cfg.get("wallet_epoch_at")).replace("Z", "+00:00")) if cfg.get("wallet_epoch_at") else now()
    if wallet_epoch.tzinfo is None:
        wallet_epoch = wallet_epoch.replace(tzinfo=timezone.utc)
    wallet_trades = [trade for trade in all_portfolio_trades if (_shadow_dt(trade.opened_at) or wallet_epoch) >= wallet_epoch]
    wallet_open_trades = [trade for trade in wallet_trades if trade.status == "OPEN"]
    wallet_closed_trades = [trade for trade in wallet_trades if trade.status == "CLOSED"]
    all_history_realized = sum(float(trade.net_pnl or 0.0) for trade in all_portfolio_trades if trade.status == "CLOSED")
    realized = sum(float(trade.net_pnl or 0.0) for trade in wallet_closed_trades)
    unrealized = sum(float(trade.net_pnl or 0.0) for trade in wallet_open_trades)
    deployed_margin = sum(float(trade.margin_used or 0.0) for trade in wallet_open_trades)
    equity = float(cfg["capital_usdt"]) + realized + unrealized
    margin_cap = float(cfg["capital_usdt"]) * float(cfg["max_total_margin_pct"]) / 100

    # Aggregate simultaneous positions by underlying instrument and direction.
    # Separate leader signals on the same coin are correlated exposures, not
    # statistically independent bets. These are warnings, not hidden trade filters.
    risk_groups: dict[str, dict] = {}
    for trade in open_trades:
        key = f"{trade.symbol}|{trade.side}"
        group = risk_groups.setdefault(key, {
            "symbol": trade.symbol, "side": trade.side, "open_count": 0,
            "open_notional": 0.0, "margin_in_use": 0.0,
            "current_net_pnl": 0.0, "estimated_stop_loss_usdt": 0.0,
            "leaders": []
        })
        group["open_count"] += 1
        group["open_notional"] += float(trade.notional or 0.0)
        group["margin_in_use"] += float(trade.margin_used or 0.0)
        group["current_net_pnl"] += float(trade.net_pnl or 0.0)
        direction = 1.0 if trade.side == "LONG" else -1.0
        stop_gross = direction * (float(trade.stop_price) - float(trade.entry_price)) * float(trade.quantity)
        stop_net = stop_gross - float(trade.estimated_costs or 0.0)
        group["estimated_stop_loss_usdt"] += max(0.0, -stop_net)
        if trade.trader_name and trade.trader_name not in group["leaders"]:
            group["leaders"].append(trade.trader_name)
    concentrations = sorted(risk_groups.values(), key=lambda group: group["estimated_stop_loss_usdt"], reverse=True)
    for group in concentrations:
        for key in ("open_notional", "margin_in_use", "current_net_pnl", "estimated_stop_loss_usdt"):
            group[key] = round(group[key], 8)
    correlated_groups = [group for group in concentrations if group["open_count"] > 1]

    # Counterfactual shadow portfolio: replay every recorded research entry in time
    # order, but accept only positions that fit a fixed margin cap and the configured
    # maximum simultaneous positions per underlying symbol/direction. This NEVER
    # changes, suppresses, or closes any research PaperTrade row.
    def _shadow_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    shadow_capital = max(0.0, float(cfg["capital_usdt"]))
    shadow_margin_cap_pct = min(
        100.0, max(5.0, float(cfg.get("shadow_max_total_margin_pct", 40.0)))
    )
    shadow_margin_cap = shadow_capital * shadow_margin_cap_pct / 100.0
    shadow_symbol_limit = min(
        5, max(1, int(float(cfg.get("shadow_max_positions_per_symbol_direction", 1))))
    )
    shadow_now = now()
    shadow_active: list[dict] = []
    shadow_selected: list[PaperTrade] = []
    shadow_skipped_correlated = 0
    shadow_skipped_margin = 0

    for candidate in shadow_trades:
        opened_at = _shadow_utc(candidate.opened_at)
        if opened_at is None or opened_at > shadow_now:
            continue

        # Free shadow capital only when a previously selected trade had actually
        # closed by this candidate's entry timestamp.
        still_active = []
        for active in shadow_active:
            closed_at = active["closed_at"]
            if closed_at is not None and closed_at <= opened_at:
                continue
            still_active.append(active)
        shadow_active = still_active

        symbol_key = (candidate.symbol.upper(), candidate.side.upper())
        same_direction_open = sum(
            1 for active in shadow_active if active["symbol_key"] == symbol_key
        )
        if same_direction_open >= shadow_symbol_limit:
            shadow_skipped_correlated += 1
            continue

        margin = max(0.0, float(candidate.margin_used or 0.0))
        active_margin = sum(active["margin"] for active in shadow_active)
        if margin > shadow_margin_cap or active_margin + margin > shadow_margin_cap + 1e-9:
            shadow_skipped_margin += 1
            continue

        close_at = _shadow_utc(candidate.closed_at) if candidate.status == "CLOSED" else None
        shadow_selected.append(candidate)
        shadow_active.append({
            "trade_id": candidate.id,
            "symbol_key": symbol_key,
            "symbol": candidate.symbol,
            "margin": margin,
            "closed_at": close_at
        })

    shadow_closed = [trade for trade in shadow_selected if trade.status == "CLOSED"]
    shadow_open = [trade for trade in shadow_selected if trade.status == "OPEN"]
    shadow_realized = sum(float(trade.net_pnl or 0.0) for trade in shadow_closed)
    shadow_unrealized = sum(float(trade.net_pnl or 0.0) for trade in shadow_open)
    shadow_deployed_margin = sum(float(trade.margin_used or 0.0) for trade in shadow_open)
    shadow_equity = shadow_capital + shadow_realized + shadow_unrealized
    shadow_free_capital = max(0.0, shadow_equity - shadow_deployed_margin)
    shadow_wins = sum(1 for trade in shadow_closed if float(trade.net_pnl or 0.0) > 0)
    shadow_losses = sum(1 for trade in shadow_closed if float(trade.net_pnl or 0.0) < 0)
    shadow_gross_wins = sum(
        float(trade.net_pnl or 0.0) for trade in shadow_closed if float(trade.net_pnl or 0.0) > 0
    )
    shadow_gross_losses = sum(
        float(trade.net_pnl or 0.0) for trade in shadow_closed if float(trade.net_pnl or 0.0) < 0
    )
    shadow_pf = (
        shadow_gross_wins / abs(shadow_gross_losses)
        if shadow_gross_losses < 0 else (None if shadow_gross_wins == 0 else None)
    )
    uncertain_quality = {
        "INFORMATION_CUTOFF", "DATA_INTERRUPTED_REVIEW",
        "PATH_NOT_RECORDED", "LEGACY_UNCLASSIFIED", "LEGACY_FUNDING_UNVERIFIED"
    }
    shadow_uncertain_count = 0
    for trade in shadow_selected:
        trade_audit = parse_json(trade.raw_json)
        quality_flag = trade_audit.get("data_quality_status", "LEGACY_UNCLASSIFIED")
        funding_flag = trade_audit.get("funding_audit_status", "LEGACY_FUNDING_UNVERIFIED")
        if quality_flag in uncertain_quality or funding_flag in uncertain_quality:
            shadow_uncertain_count += 1
    shadow_portfolio = {
        "model": "COUNTERFACTUAL_SHADOW_PORTFOLIO",
        "capital": round(shadow_capital, 8),
        "margin_cap_pct": shadow_margin_cap_pct,
        "margin_cap": round(shadow_margin_cap, 8),
        "max_positions_per_symbol_direction": shadow_symbol_limit,
        "deployed_margin": round(shadow_deployed_margin, 8),
        "margin_utilization_pct": round(
            shadow_deployed_margin / max(shadow_capital, 0.01) * 100, 4
        ),
        "realized_pnl": round(shadow_realized, 8),
        "unrealized_pnl": round(shadow_unrealized, 8),
        "equity": round(shadow_equity, 8),
        "free_capital": round(shadow_free_capital, 8),
        "selected_count": len(shadow_selected),
        "selected_closed_count": len(shadow_closed),
        "selected_open_count": len(shadow_open),
        "skipped_correlated": shadow_skipped_correlated,
        "skipped_margin_cap": shadow_skipped_margin,
        "closed_wins": shadow_wins,
        "closed_losses": shadow_losses,
        "realized_profit_factor": round(shadow_pf, 6) if shadow_pf is not None else None,
        "uncertain_data_quality_count": shadow_uncertain_count,
        "uncertain_data_quality_pct": round(
            shadow_uncertain_count / max(len(shadow_selected), 1) * 100, 2
        ),
        "note": (
            "Counterfactual replay of recorded research entries/exits. It applies the configured "
            "margin cap and symbol-direction overlap limit but does not simulate new fills, "
            "intrabar ordering, queue priority, or alternate exits. Trades with information cutoffs, "
            "data gaps, or legacy-unclassified paths remain uncertain. This is not a guarantee of "
            "real-world execution or profitability."
        )
    }

    trade_rows = []
    for trade in trades:
        raw = parse_json(trade.raw_json)
        max_gap = max(max_gaps.get(trade.id, 0.0), float(raw.get("max_data_gap_seconds", 0.0) or 0.0))
        opened_dt = trade.opened_at if trade.opened_at.tzinfo else trade.opened_at.replace(tzinfo=timezone.utc)
        closed_dt = trade.closed_at or now()
        if closed_dt.tzinfo is None:
            closed_dt = closed_dt.replace(tzinfo=timezone.utc)
        hold_seconds = max(0.0, (closed_dt - opened_dt).total_seconds())
        trade_rows.append({
            "id": trade.id, "strategy": trade.strategy, "trader_id": trade.trader_id, "trader_name": trade.trader_name,
            "leader_position_key": (trade.source_key.split("|", 1)[1] if "|" in trade.source_key else trade.source_key),
            "symbol": trade.symbol, "side": trade.side, "status": trade.status,
            "wallet_epoch_eligible": bool((_shadow_dt(trade.opened_at) or wallet_epoch) >= wallet_epoch),
            "entry_price": trade.entry_price, "mark_price": trade.mark_price, "exit_price": trade.exit_price,
            "quantity": trade.quantity, "notional": trade.notional, "margin_used": trade.margin_used,
            "leverage": trade.leverage, "stop_price": trade.stop_price, "target_price": trade.target_price,
            "gross_pnl": round(float(trade.gross_pnl or 0.0), 8),
            "estimated_costs": round(float(trade.estimated_costs or 0.0), 8),
            "net_pnl": round(float(trade.net_pnl or 0.0), 8),
            "exit_reason": trade.exit_reason,
            "exit_reason_display": paper_exit_reason_display(trade, raw),
            "post_exit_recovery_label": raw.get("post_exit_recovery_label"),
            "post_exit_recovery": {
                "status": raw.get("post_exit_recovery_status"),
                "max_net_pnl": safe_float(raw.get("post_exit_max_net_pnl")),
                "max_price": safe_float(raw.get("post_exit_max_price")),
                "max_at": raw.get("post_exit_max_at"),
                "return_to_entry_at": raw.get("post_exit_return_to_entry_at"),
                "market_source": raw.get("post_exit_recovery_market_source")
            },
            "mfe_net_pnl": safe_float(raw.get("mfe_net_pnl")),
            "mae_net_pnl": safe_float(raw.get("mae_net_pnl")),
            "closed_trade_candle_audit": raw.get("closed_trade_candle_audit", {}),
            "post_exit_audit_windows": raw.get("post_exit_audit_windows", {}),
            "new_exit_engine_audit": raw.get("new_exit_engine_audit_v1", {}),
            "mfe_pct": safe_float(raw.get("mfe_pct")), "mae_pct": safe_float(raw.get("mae_pct")),
            "entry_timing_improvement_pct": safe_float(raw.get("entry_timing_improvement_pct")),
            "entry_timing_best_price": safe_float(raw.get("entry_timing_best_price")),
            "entry_timing_best_at": raw.get("entry_timing_best_at"),
            "entry_timing_potential_gross_pnl": safe_float(raw.get("entry_timing_potential_gross_pnl")),
            "entry_timing_verdict": raw.get("entry_timing_verdict"),
            "data_quality_status": raw.get("data_quality_status", "LEGACY_UNCLASSIFIED"),
            "monitoring_status": raw.get("monitoring_status", "NOT_RECORDED"),
            "max_data_gap_seconds": round(max_gap, 3),
            "funding_history_verified": raw.get("funding_history_verified"),
            "funding_audit_status": raw.get("funding_audit_status", "LEGACY_FUNDING_UNVERIFIED"),
            "funding_cost_actual": safe_float(raw.get("funding_cost_actual")) if raw.get("funding_history_verified") else None,
            "funding_cost_last_verified": safe_float(raw.get("funding_cost_actual")) if not raw.get("funding_history_verified") else None,
            "funding_estimate_current": safe_float(raw.get("funding_estimate_current")),
            "funding_estimate_at_exit": safe_float(raw.get("funding_estimate_at_exit")),
            "funding_history_rows": safe_float(raw.get("funding_history_rows")),
            "funding_source": (
                raw.get("funding_source") if raw.get("funding_history_verified")
                else "CONSERVATIVE_ESTIMATE_PENDING_BINANCE_HISTORY"
                if raw.get("funding_cost_actual") is not None else raw.get("funding_source")
            ),
            "funding_source_last_verified": raw.get("funding_source") if not raw.get("funding_history_verified") else None,
            "hold_duration_seconds": round(hold_seconds, 2),
            "hold_duration_hours": round(hold_seconds / 3600, 3),
            "post_exit_recovery_minutes_to_entry": safe_float(raw.get("post_exit_minutes_to_entry")),
            "post_exit_recovery_minutes_to_net_profit": safe_float(raw.get("post_exit_minutes_to_net_profit")),
            "post_exit_recovery_minutes_to_best": safe_float(raw.get("post_exit_minutes_to_best")),
            "post_exit_recovery_worst_net_pnl": safe_float(raw.get("post_exit_worst_net_pnl")),
            "post_exit_recovery_observed_minutes": safe_float(raw.get("post_exit_observed_minutes")),
            "post_exit_recovery_audit_version": safe_float(raw.get("post_exit_recovery_audit_version")),
            "post_exit_recovery_audit_status": raw.get("post_exit_recovery_audit_status", "PENDING"),
            "post_exit_audit_windows": raw.get("post_exit_audit_windows", {}),
            "closed_trade_candle_audit": raw.get("closed_trade_candle_audit", {}),
            "new_exit_engine_audit": raw.get("new_exit_engine_audit_v1", {}),
            "last_leader_check_at": raw.get("last_leader_check_at"),
            "leader_state_checked_at": raw.get("leader_state_checked_at"),
            "leader_state_detail": leader_checks.get(trade.id, {}).get("detail") or raw.get("leader_state_detail"),
            "leader_status": leader_checks.get(trade.id, {}).get("status") or raw.get("leader_state_at_check") or raw.get("leader_status") or "NOT_CHECKED",
            "leader_check_source": leader_checks.get(trade.id, {}).get("source") or ("LEGACY_VISIBLE_POSITION_SNAPSHOT" if raw.get("leader_state_checked_at") or raw.get("last_leader_check_at") else "NO_STORED_CHECK"),
            "leader_check_at": leader_checks.get(trade.id, {}).get("checked_at") or raw.get("leader_state_checked_at") or raw.get("last_leader_check_at"),
            "opened_at": trade.opened_at.isoformat(),
            "closed_at": trade.closed_at.isoformat() if trade.closed_at else None
        })

    return {
        "config": cfg, "capital": round(float(cfg["capital_usdt"]), 8),
        "realized_pnl": round(realized, 8), "unrealized_pnl": round(unrealized, 8),
        "all_history_realized_pnl": round(all_history_realized, 8),
        "wallet_epoch_at": wallet_epoch.isoformat(),
        "wallet_open_count": len(wallet_open_trades), "wallet_closed_count": len(wallet_closed_trades),
        "wallet_total_trade_count": len(wallet_trades),
        "equity": round(equity, 8), "deployed_margin": round(deployed_margin, 8),
        "margin_cap": round(margin_cap, 8),
        "margin_utilization_pct": round(deployed_margin / max(float(cfg["capital_usdt"]), 0.01) * 100, 4),
        "margin_cap_exceeded": deployed_margin > margin_cap,
        "free_capital": round(max(0.0, equity - deployed_margin), 8),
        "open_count": len(open_trades), "closed_count": len(closed_trades),
        "correlated_exposure_groups": correlated_groups,
        "recovery_audit": {
            "complete": sum(1 for t in closed_trades if parse_json(t.raw_json).get("post_exit_recovery_audit_version") == 9 and parse_json(t.raw_json).get("post_exit_recovery_audit_status") == "COMPLETE"),
            "pending": sum(1 for t in closed_trades if parse_json(t.raw_json).get("post_exit_recovery_audit_version") != 9 or not parse_json(t.raw_json).get("post_exit_recovery_audit_updated_at") or parse_json(t.raw_json).get("post_exit_recovery_audit_status") == "PARTIAL_WINDOW"),
            "incomplete_final": sum(1 for t in closed_trades if parse_json(t.raw_json).get("post_exit_recovery_audit_status") == "INCOMPLETE_WINDOW_FINAL"),
            "engine_replay_complete": sum(1 for t in closed_trades if parse_json(t.raw_json).get("new_exit_engine_audit_status") in {"POLICY_EXIT_SIMULATED", "OPEN_UNREALIZED_AT_LAST_CANDLE"}),
            "engine_replay_pending": sum(1 for t in closed_trades if parse_json(t.raw_json).get("new_exit_engine_audit_status") not in {"POLICY_EXIT_SIMULATED", "OPEN_UNREALIZED_AT_LAST_CANDLE"}),
            "version6_or_older": sum(1 for t in closed_trades if parse_json(t.raw_json).get("post_exit_recovery_audit_version", 0) < 7),
            "blocked": bool(binance_rest_status().get("blocked")),
            "remaining_seconds": binance_rest_status().get("remaining_seconds"),
            "blocked_until": binance_rest_status().get("blocked_until")
        },
        "shadow_portfolio": shadow_portfolio,
        "price_feed": {
            "status": PAPER_PRICE_STREAM_STATE.get("status"),
            "connected": bool(PAPER_PRICE_STREAM_STATE.get("connected")),
            "last_price_at": PAPER_PRICE_STREAM_STATE.get("last_price_at"),
            "last_error": PAPER_PRICE_STREAM_STATE.get("last_error"),
            "active_symbols": len(PAPER_PRICE_STREAM_STATE.get("subscribed_symbols") or []),
            "binance_rest": binance_rest_status()
        },
        "risk_summary": {
            "correlated_groups": len(correlated_groups),
            "largest_shared_direction_stop_risk_usdt": max((g["estimated_stop_loss_usdt"] for g in concentrations), default=0.0),
            "estimated_open_stop_risk_usdt": round(sum(g["estimated_stop_loss_usdt"] for g in concentrations), 8),
            "mode": "RESEARCH_UNCONSTRAINED" if cfg.get("research_unconstrained", True) else "CAPITAL_CONSTRAINED",
            "warning": "The current shared wallet is isolated to the wallet epoch. All-history ledger totals include legacy pre-epoch paper records; use the $200 chronological replay for the historical counterfactual."
        },
        "trades": trade_rows
    }


def build_new_exit_policy_replay(trades: list[PaperTrade], cfg: dict) -> dict:
    """Replay the policy on stored Binance mark candles; never treat price peaks as fills."""
    closed = [t for t in trades if t.status == "CLOSED"]
    if not closed:
        return {"summary": {}, "trades": [], "source": "No closed paper trades"}
    trader_ids = sorted({t.trader_id for t in closed})
    earliest = min((_shadow_dt(t.opened_at) or now()) for t in closed)
    with SessionLocal() as db:
        snapshots = db.scalars(select(PositionSnapshot).where(
            PositionSnapshot.trader_id.in_(trader_ids),
            PositionSnapshot.observed_at >= earliest
        ).order_by(PositionSnapshot.trader_id, PositionSnapshot.observed_at, PositionSnapshot.id)).all()
        snapshot_ids = [x.id for x in snapshots]
        observations = db.scalars(select(PositionObservation).where(
            PositionObservation.snapshot_id.in_(snapshot_ids)
        ).order_by(PositionObservation.snapshot_id, PositionObservation.id)).all() if snapshot_ids else []
        history = db.scalars(select(HistoryPosition).where(HistoryPosition.trader_id.in_(trader_ids))).all()
    obs_by_snapshot = {}
    snaps_by_trader = {}
    for obs in observations:
        obs_by_snapshot.setdefault(obs.snapshot_id, []).append(obs)
    for snap in snapshots:
        snaps_by_trader.setdefault(snap.trader_id, []).append(snap)
    history_by_id = {}
    for item in history:
        if item.position_id:
            history_by_id.setdefault((item.trader_id, str(item.position_id)), []).append(item)

    def pnl_at(trade, mark, at_ms):
        direction = 1.0 if trade.side.upper() == "LONG" else -1.0
        gross = direction * (mark - float(trade.entry_price)) * float(trade.quantity)
        turnover = (float(trade.entry_price) + mark) * float(trade.quantity)
        fee_slippage = turnover * (float(cfg["taker_fee_pct"]) + float(cfg["slippage_pct"])) / 100.0
        raw = parse_json(trade.raw_json)
        rate = safe_float(raw.get("funding_rate_at_entry"))
        next_funding = safe_float(raw.get("next_funding_time"))
        funding = None
        funding_source = "UNVERIFIED"
        if raw.get("funding_history_replay_verified"):
            points = raw.get("funding_history_replay_points") or []
            funding = sum(float(point.get("funding_cost") or 0.0) for point in points
                          if int(point.get("funding_time") or 0) <= at_ms)
            funding_source = raw.get("funding_history_replay_source") or "BINANCE_FUNDING_RATE_HISTORY"
        elif rate is not None and next_funding:
            opened = _shadow_dt(trade.opened_at) or datetime.fromtimestamp(at_ms / 1000, timezone.utc)
            next_ms = next_funding
            while next_ms <= int(opened.timestamp() * 1000):
                next_ms += 28800000
            count = 0 if at_ms < next_ms else 1 + int((at_ms - next_ms) // 28800000)
            funding = float(trade.notional or 0.0) * abs(rate) * count
            funding_source = "CONSERVATIVE_ENTRY_RATE_ESTIMATE"
        net = gross - fee_slippage - (funding or 0.0)
        return {"gross": round(gross, 8), "fees_slippage_estimate": round(fee_slippage, 8),
                "funding_estimate": round(funding, 8) if funding is not None else None,
                "funding_source": funding_source,
                "net_estimate": round(net, 8) if funding is not None else None}

    rows = []
    old_total = sum(float(t.net_pnl or 0.0) for t in closed)
    old_wins = sum(1 for t in closed if float(t.net_pnl or 0.0) > 0)
    old_losses = sum(1 for t in closed if float(t.net_pnl or 0.0) < 0)
    for trade in closed:
        raw = parse_json(trade.raw_json)
        stored_replay = raw.get("new_exit_engine_audit_v1")
        replay_source_audit_at = raw.get("new_exit_engine_audit_source_audit_updated_at")
        current_audit_at = raw.get("post_exit_recovery_audit_updated_at")
        if isinstance(stored_replay, dict) and int(raw.get("new_exit_engine_audit_version", 0) or 0) == 1 and replay_source_audit_at and replay_source_audit_at == current_audit_at:
            rows.append(stored_replay)
            continue
        old_exit = _shadow_dt(trade.closed_at)
        key = trade.source_key.split("|", 1)[1] if "|" in trade.source_key else ""
        position_id = key.split("|", 1)[0] if "|" in key else ""
        common = {
            "trade_id": trade.id, "trader_id": trade.trader_id, "trader_name": trade.trader_name,
            "symbol": trade.symbol, "side": trade.side,
            "old_exit_at": old_exit.isoformat() if old_exit else None,
            "old_exit_reason": trade.exit_reason,
            "old_realized_net_pnl": round(float(trade.net_pnl or 0.0), 8),
            "leader_identity": "PROVIDER_POSITION_ID" if position_id else "SYMBOL_SIDE_ONLY",
            "leader_identity_note": "Exact provider position ID is available." if position_id else "Provider position ID unavailable; symbol/side identity is not unique.",
            "baseline_leader_open_confirmed": raw.get("leader_state_at_check") == "OPEN_OBSERVED"
        }
        if old_exit is None:
            rows.append({**common, "replay_status": "MISSING_OLD_EXIT_TIMESTAMP"})
            continue
        if int(raw.get("post_exit_recovery_audit_version", 0) or 0) != 9 or not raw.get("post_exit_recovery_audit_updated_at"):
            rows.append({**common, "replay_status": "PENDING_VERSION_9_CLOSED_TRADE_AUDIT"})
            continue
        pre_exit_candles = raw.get("entry_timing_mark_candles")
        post_exit_candles = raw.get("post_exit_mark_candles")
        pre_exit_candles = pre_exit_candles if isinstance(pre_exit_candles, list) else []
        post_exit_candles = post_exit_candles if isinstance(post_exit_candles, list) else []
        candles = sorted(pre_exit_candles + post_exit_candles,
                         key=lambda c: int(c[0]) if isinstance(c, (list, tuple)) and c else 0)
        lifecycle_audit = raw.get("closed_trade_candle_audit", {})
        if not candles or not pre_exit_candles:
            rows.append({**common, "replay_status": "MISSING_ENTRY_TO_EXIT_CANDLE_PATH"})
            continue
        if lifecycle_audit and lifecycle_audit.get("status") != "COMPLETE":
            rows.append({**common, "replay_status": "INCOMPLETE_ENTRY_TO_EXIT_CANDLE_PATH"})
            continue
        timeline = snaps_by_trader.get(trade.trader_id, [])
        closes = history_by_id.get((trade.trader_id, position_id), []) if position_id else []
        confirmed_close_ms = max((int(h.closed_at_ms) for h in closes
            if h.closed_at_ms and h.symbol.upper() == trade.symbol.upper()
            and h.side.upper() == trade.side.upper()
            and str(h.status or "").upper() in ("CLOSED", "CLOSE")), default=None)
        entry_dt = _shadow_dt(trade.opened_at) or old_exit
        entry_ms = int(entry_dt.timestamp() * 1000)
        old_exit_ms = int(old_exit.timestamp() * 1000)
        last = None
        exit_result = None
        checkpoints = []
        checkpoint_hours = (3, 6, 12, 24, 48, 72)
        next_checkpoint = 0
        # Re-run the existing engine from original entry. The candle path itself
        # rebuilds MFE; old ledger MFE is not injected into this counterfactual.
        replay_mfe = None
        replay_leverage = max(1.0, min(float(cfg.get("max_leverage", 3.0)), float(trade.leverage or 1.0)))
        replay_margin_cap = 200.0 * float(cfg.get("max_margin_pct", 10.0)) / 100.0
        replay_base_margin = min(replay_margin_cap, float(cfg.get("max_notional_usdt", 50.0)) / replay_leverage)
        replay_base_notional = replay_base_margin * replay_leverage
        replay_scale = replay_base_notional / max(float(trade.notional or 0.0), 1e-12)
        replay_min_harvest_margin_pct = float(cfg.get("profit_harvest_min_margin_pct", 0.70))
        replay_giveback_pct = float(cfg.get("profit_giveback_pct", 35.0))
        for candle in sorted(candles, key=lambda c: int(c[0]) if isinstance(c, (list, tuple)) and c else 0):
            if not isinstance(candle, (list, tuple)) or len(candle) < 4:
                continue
            at_ms = int(candle[0])
            high, low, close = safe_float(candle[1]), safe_float(candle[2]), safe_float(candle[3])
            if at_ms < entry_ms or high is None or low is None or close is None:
                continue
            last = (at_ms, close)
            elapsed = (at_ms - entry_ms) / 3600000
            p = pnl_at(trade, close, at_ms)
            p = {k: (round(v * replay_scale, 8) if isinstance(v, (int, float)) and v is not None else v) for k, v in p.items()}
            while next_checkpoint < len(checkpoint_hours) and elapsed >= checkpoint_hours[next_checkpoint]:
                checkpoints.append({"target_hours": checkpoint_hours[next_checkpoint],
                    "observed_at": datetime.fromtimestamp(at_ms / 1000, timezone.utc).isoformat(),
                    "actual_elapsed_hours": round(elapsed, 3),
                    "mark_price": close, "net_pnl_estimate": p["net_estimate"],
                    "status": "MARK_TO_MARKET_NOT_REALIZED"})
                next_checkpoint += 1
            state = "UNKNOWN"
            for snap in timeline:
                snap_at = _shadow_dt(snap.observed_at)
                if snap_at is None or int(snap_at.timestamp() * 1000) > at_ms:
                    break
                if snap_at < entry_dt:
                    continue
                if snap.hidden:
                    state = "UNKNOWN"
                    continue
                payload = unwrap_data(parse_json(snap.raw_json))
                if not isinstance(payload.get("positions"), list):
                    continue
                obs_rows = obs_by_snapshot.get(snap.id, [])
                exact = next((o for o in obs_rows if o.position_key == key), None)
                opposite = "SHORT" if trade.side.upper() == "LONG" else "LONG"
                reversed_position = next((o for o in obs_rows if o.symbol.upper() == trade.symbol.upper()
                                          and o.side.upper() == opposite), None)
                state = "OPEN" if exact is not None else "REVERSED" if reversed_position is not None else "UNKNOWN"
            if confirmed_close_ms and confirmed_close_ms <= at_ms:
                state = "CLOSED"
            stop = safe_float(trade.stop_price)
            target = safe_float(trade.target_price)
            stop_hit = stop is not None and ((trade.side.upper() == "LONG" and low <= stop) or
                                             (trade.side.upper() == "SHORT" and high >= stop))
            target_hit = target is not None and ((trade.side.upper() == "LONG" and high >= target) or
                                                 (trade.side.upper() == "SHORT" and low <= target))
            if p["net_estimate"] is not None:
                replay_mfe = max(float(replay_mfe if replay_mfe is not None else p["net_estimate"]), float(p["net_estimate"]))
            # The live engine's stop branch is gated by net <= 0 and is deferred
            # during an unresolved monitoring interruption, not by leader visibility.
            interruption_start = _shadow_dt(datetime.fromisoformat(raw["monitoring_interruption_started_at"].replace("Z", "+00:00"))) if raw.get("monitoring_interruption_started_at") else None
            resumed_at = _shadow_dt(datetime.fromisoformat(raw["monitoring_resumed_at"].replace("Z", "+00:00"))) if raw.get("monitoring_resumed_at") else None
            gap_guard = bool(
                (raw.get("monitoring_status") == "INTERRUPTED" and resumed_at is None)
                or (interruption_start and (resumed_at is None or at_ms < int(resumed_at.timestamp() * 1000)))
            )
            stop_pnl = None
            if stop_hit and not gap_guard:
                stop_pnl = pnl_at(trade, stop, at_ms)
                stop_pnl = {k: (round(v * replay_scale, 8) if isinstance(v, (int, float)) and v is not None else v) for k, v in stop_pnl.items()}
            target_pnl = None
            if target_hit:
                target_pnl = pnl_at(trade, target, at_ms)
                target_pnl = {k: (round(v * replay_scale, 8) if isinstance(v, (int, float)) and v is not None else v) for k, v in target_pnl.items()}
            stop_effective = bool(stop_pnl is not None and (stop_pnl["net_estimate"] is None or stop_pnl["net_estimate"] <= 0))
            target_effective = bool(target_pnl is not None and target_pnl["net_estimate"] is not None and target_pnl["net_estimate"] > 0)
            if stop_effective and target_effective:
                exit_result = {"at_ms": at_ms, "price": None,
                    "reason": "AMBIGUOUS_INTRABAR_STOP_AND_TARGET",
                    "quality": "BINANCE_1M_OHLC_ORDER_UNKNOWN",
                    "leader_state": state, "pnl": None}
                break
            if stop_effective:
                stop_reason = (
                    "LEADER_POSITION_REVERSED" if state == "REVERSED" and bool(cfg.get("leader_reversal_exit_enabled", True))
                    else "LEADER_POSITION_CLOSED" if state == "CLOSED"
                    else "STOP_LOSS"
                )
                exit_result = {"at_ms": at_ms, "price": stop,
                    "reason": stop_reason,
                    "quality": "BINANCE_MARK_PRICE_1M_OHLC_STOP_TOUCH_ESTIMATE",
                    "leader_state": state, "pnl": stop_pnl}
                break
            if target_effective:
                p = target_pnl
                replay_mfe = max(float(replay_mfe if replay_mfe is not None else p["net_estimate"]), float(p["net_estimate"]))
            target_hit = target_effective
            if p["net_estimate"] is not None and p["net_estimate"] > 0:
                margin_pct = float(p["net_estimate"]) / max(float(replay_base_margin), 1e-9) * 100.0
                giveback = max(0.0, (replay_mfe - float(p["net_estimate"])) / replay_mfe * 100.0) if replay_mfe > 0 else 0.0
                funding_verified = bool(raw.get("funding_history_replay_verified"))
                if funding_verified:
                    harvest = target_hit or margin_pct >= replay_min_harvest_margin_pct or giveback >= replay_giveback_pct
                    harvest_reason = "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK"
                else:
                    funding_rate_known = safe_float(raw.get("funding_rate_at_entry")) is not None
                    funding_schedule_known = bool(safe_float(raw.get("next_funding_time")))
                    if funding_schedule_known and funding_rate_known:
                        cost_floor = max((float(p.get("fees_slippage_estimate") or 0.0) + float(p.get("funding_estimate") or 0.0)) * 1.5, 0.02)
                        meaningful = target_hit or (float(p["net_estimate"]) >= cost_floor and replay_mfe >= cost_floor * 1.5)
                        profit_lock = replay_mfe >= cost_floor * 1.5 and giveback >= replay_giveback_pct and float(p["net_estimate"]) > 0
                        harvest = meaningful or profit_lock
                        harvest_reason = "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK_FUNDING_ESTIMATED"
                    else:
                        harvest = False
                        harvest_reason = "PROFIT_HARVEST_NOT_VERIFIABLE_WITHOUT_FUNDING_SCHEDULE"
                if harvest:
                    exit_result = {"at_ms": at_ms, "price": target if target_hit else close, "reason": harvest_reason,
                        "quality": "BINANCE_MARK_PRICE_1M_OHLC_TARGET_TOUCH_ESTIMATE" if target_hit else "BINANCE_MARK_PRICE_1M_CLOSE",
                        "leader_state": "NOT_REQUIRED_FOR_PROFIT_HARVEST", "pnl": p}
                    break
        if exit_result and exit_result.get("reason") == "AMBIGUOUS_INTRABAR_STOP_AND_TARGET":
            ambiguous_at = datetime.fromtimestamp(exit_result["at_ms"] / 1000, timezone.utc)
            rows.append({**common, "replay_status": "AMBIGUOUS_INTRABAR_ORDERING",
                "new_exit_reason": exit_result["reason"], "new_exit_at": ambiguous_at.isoformat(),
                "execution_quality": exit_result["quality"], "leader_state_at_exit": exit_result["leader_state"],
                "new_policy_net_pnl_estimate": None, "new_policy_gross_pnl_estimate": None,
                "new_policy_fees_slippage_estimate": None, "new_policy_funding_estimate": None,
                "replay_base_notional": round(replay_base_notional, 8),
                "replay_base_margin": round(replay_base_margin, 8),
                "replay_base_leverage": round(replay_leverage, 4),
                "hold_duration_hours_from_entry": round((exit_result["at_ms"] - entry_ms) / 3600000, 3),
                "new_exit_offset_from_old_exit_hours": round((exit_result["at_ms"] - old_exit_ms) / 3600000, 3),
                "checkpoints": checkpoints})
        elif exit_result:
            p = exit_result["pnl"]
            rows.append({**common, "replay_status": "POLICY_EXIT_SIMULATED",
                "new_exit_reason": exit_result["reason"],
                "new_exit_at": datetime.fromtimestamp(exit_result["at_ms"] / 1000, timezone.utc).isoformat(),
                "new_exit_price": exit_result["price"], "new_exit_price_quality": exit_result["quality"],
                "leader_state_at_exit": exit_result["leader_state"],
                "new_gross_pnl": p["gross"], "new_fee_slippage_estimate": p["fees_slippage_estimate"],
                "new_funding_estimate": p["funding_estimate"],
                "replay_base_notional": round(replay_base_notional, 8),
                "replay_base_margin": round(replay_base_margin, 8),
                "replay_base_leverage": round(replay_leverage, 4),
                "new_policy_net_pnl_estimate": p["net_estimate"],
                "hold_duration_hours_from_entry": round((exit_result["at_ms"] - entry_ms) / 3600000, 3),
                "new_exit_offset_from_old_exit_hours": round((exit_result["at_ms"] - old_exit_ms) / 3600000, 3),
                "checkpoints": checkpoints})
        elif last:
            at_ms, price = last
            p = pnl_at(trade, price, at_ms)
            p = {k: (round(v * replay_scale, 8) if isinstance(v, (int, float)) and v is not None else v) for k, v in p.items()}
            rows.append({**common, "replay_status": "OPEN_UNREALIZED_AT_LAST_CANDLE",
                "last_observed_at": datetime.fromtimestamp(at_ms / 1000, timezone.utc).isoformat(),
                "last_observed_mark": price, "new_unrealized_net_pnl_estimate": p["net_estimate"],
                "new_unrealized_gross_pnl": p["gross"],
                "replay_base_notional": round(replay_base_notional, 8),
                "replay_base_margin": round(replay_base_margin, 8),
                "replay_base_leverage": round(replay_leverage, 4),
                "hold_duration_hours_from_entry": round((at_ms - entry_ms) / 3600000, 3),
                "new_exit_offset_from_old_exit_hours": round((at_ms - old_exit_ms) / 3600000, 3),
                "checkpoints": checkpoints})
        else:
            rows.append({**common, "replay_status": "NO_VALID_CANDLE_IN_REPLAY_PATH", "checkpoints": checkpoints})

    exits = [x for x in rows if x.get("replay_status") == "POLICY_EXIT_SIMULATED"]
    priced = [x for x in exits if x.get("new_policy_net_pnl_estimate") is not None]
    opened = [x for x in rows if x.get("replay_status") == "OPEN_UNREALIZED_AT_LAST_CANDLE"]
    open_priced = [x for x in opened if x.get("new_unrealized_net_pnl_estimate") is not None]
    summary = {
        "old_system_realized_net_pnl": round(old_total, 8), "old_system_closed_trades": len(closed),
        "old_system_profitable_trades": old_wins, "old_system_losing_trades": old_losses,
        "new_policy_exit_net_pnl_estimate": round(sum(x["new_policy_net_pnl_estimate"] for x in priced), 8),
        "new_policy_exits_simulated": len(exits), "new_policy_exits_with_cost_estimate": len(priced),
        "new_policy_profitable_exit_estimates": sum(1 for x in priced if x["new_policy_net_pnl_estimate"] > 0),
        "new_policy_losing_exit_estimates": sum(1 for x in priced if x["new_policy_net_pnl_estimate"] < 0),
        "new_policy_open_unrealized_count": len(opened),
        "new_policy_open_unrealized_net_estimate": round(sum(x["new_unrealized_net_pnl_estimate"] for x in open_priced), 8),
        "audit_pending_trades": sum(1 for x in rows if x.get("replay_status") == "PENDING_VERSION_9_CLOSED_TRADE_AUDIT"),
        "missing_verified_candle_trades": sum(1 for x in rows if x.get("replay_status") in {"MISSING_VERIFIED_BINANCE_CANDLES", "MISSING_ENTRY_TO_EXIT_CANDLE_PATH"}),
        "incomplete_entry_to_exit_candle_trades": sum(1 for x in rows if x.get("replay_status") == "INCOMPLETE_ENTRY_TO_EXIT_CANDLE_PATH"),
        "ambiguous_intrabar_ordering_trades": sum(1 for x in rows if x.get("replay_status") == "AMBIGUOUS_INTRABAR_ORDERING"),
        "no_valid_candle_trades": sum(1 for x in rows if x.get("replay_status") == "NO_VALID_CANDLE_IN_REPLAY_PATH"),
        "leader_open_evidence_at_old_exit": sum(1 for x in rows if x.get("baseline_leader_open_confirmed")),
        "price_source": "Persisted Binance USD-M mark-price 1m OHLC candles",
        "exit_policy": "Replay of the application's existing paper exit engine: net-positive target/profit-lock harvesting, protective stop behavior, confirmed leader reversal/closure, and existing funding/data-gap safeguards. This is not a replacement policy.",
        "cost_assumption": "Configured taker fee and slippage; historical per-trade settings were not persisted.",
        "funding_assumption": "Uses Binance fundingRate settlement rows when all expected settlements are present; otherwise applies a conservative absolute entry-rate estimate. Incomplete history is explicitly marked unverified.",
        "execution_caveat": "Full entry-to-12h-post-old-exit replay of the existing engine, normalized to its $200-wallet per-trade sizing basis before portfolio overlap. 1m OHLC cannot prove intrabar fill order. Not actual exchange fills or realized PnL.",
        "sizing_basis": "$200 starting wallet; 10% per-trade margin cap; $50 notional cap; max 3x leverage. The separate chronological wallet replay then applies actual overlap and shared margin constraints.",
        "paper_only": True
    }
    # Persist each counterfactual result on its own closed-trade row. This makes
    # historical audits survive refreshes/restarts and avoids recomputing them from
    # UI-only state. Recorded financial fields are never rewritten.
    with SessionLocal() as db:
        for row in rows:
            if row.get("replay_status") == "PENDING_VERSION_9_CLOSED_TRADE_AUDIT":
                continue
            trade = db.get(PaperTrade, int(row["trade_id"]))
            if not trade:
                continue
            raw = parse_json(trade.raw_json)
            raw["new_exit_engine_audit_v1"] = row
            raw["new_exit_engine_audit_version"] = 1
            raw["new_exit_engine_audit_status"] = row.get("replay_status", "UNKNOWN")
            raw["new_exit_engine_audit_source_audit_updated_at"] = raw.get("post_exit_recovery_audit_updated_at")
            raw["new_exit_engine_audit_updated_at"] = now().isoformat()
            # Keep the durable audit metrics/replay row, not hundreds or thousands
            # of raw 1m candles per trade. If a partial audit is retried later, the
            # next Binance fetch repopulates the temporary candle arrays first.
            raw.pop("post_exit_mark_candles", None)
            raw.pop("entry_timing_mark_candles", None)
            trade.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
        db.commit()
    return {"summary": summary, "trades": rows,
            "source": "Binance USD-M mark-price 1m candles from original entry through 12h after recorded exit + time-aligned leader snapshots retained with the trade",
            "persisted": True}


def build_shared_wallet_new_engine_replay(trades: list[PaperTrade], cfg: dict, replay_rows: list[dict], capital: float = 200.0) -> dict:
    """Chronological, shared-wallet simulation using the persisted existing exit-engine replay."""
    by_id = {int(t.id): t for t in trades}
    candidates = []
    unknown = 0
    unknown_rows = []
    for row in replay_rows:
        trade = by_id.get(int(row.get("trade_id", 0) or 0))
        if not trade:
            continue
        status = row.get("replay_status")
        if status not in {"POLICY_EXIT_SIMULATED", "OPEN_UNREALIZED_AT_LAST_CANDLE"}:
            unknown += 1
            unknown_rows.append({"trade_id": trade.id, "strategy": trade.strategy,
                "symbol": trade.symbol, "side": trade.side, "reason": status or "UNKNOWN_REPLAY_STATUS"})
            continue
        opened = _shadow_dt(trade.opened_at)
        if not opened:
            unknown += 1
            continue
        if status == "POLICY_EXIT_SIMULATED":
            exit_at = _shadow_dt(datetime.fromisoformat(row["new_exit_at"].replace("Z", "+00:00")))
            original_net = safe_float(row.get("new_policy_net_pnl_estimate"))
            realized = True
        else:
            exit_at = None  # no policy exit was observed; capital stays reserved
            original_net = safe_float(row.get("new_unrealized_net_pnl_estimate"))
            realized = False
        audit_end = _shadow_dt(datetime.fromisoformat(row["last_observed_at"].replace("Z", "+00:00"))) if not realized and row.get("last_observed_at") else exit_at
        if original_net is None:
            unknown += 1
            continue
        candidates.append({"trade": trade, "row": row, "opened": opened, "exit_at": exit_at,
            "original_net": original_net, "realized": realized, "audit_end": audit_end})
    candidates.sort(key=lambda x: (x["opened"], int(x["trade"].id)))

    initial = max(0.0, float(capital))
    realized_pnl = 0.0
    active: list[dict] = []
    accepted = []
    skipped = []
    skipped_margin = skipped_correlation = 0
    total_margin_deployed = total_notional_deployed = margin_hours = 0.0
    peak_margin = 0.0
    peak_wallet_utilization_pct = 0.0
    peak_equity = initial
    max_drawdown_abs = 0.0
    max_drawdown_pct = 0.0
    equity_curve = []
    max_positions = 0
    last_time = candidates[0]["opened"] if candidates else now()

    def settle_until(at):
        nonlocal realized_pnl, active, margin_hours, last_time, peak_equity, max_drawdown_abs, max_drawdown_pct
        at = _shadow_dt(at) or last_time
        cursor = last_time
        while True:
            due = [x["exit_at"] for x in active if x["realized"] and x["exit_at"] and cursor < x["exit_at"] <= at]
            if not due:
                break
            event_at = min(due)
            if event_at > cursor:
                margin_hours += sum(x["margin"] for x in active) * (event_at - cursor).total_seconds() / 3600.0
                cursor = event_at
            remaining = []
            closed_now = []
            for position in active:
                if position["realized"] and position["exit_at"] and position["exit_at"] <= event_at:
                    realized_pnl += position["net_pnl"]
                    closed_now.append(position)
                else:
                    remaining.append(position)
            active = remaining
            realized_equity = initial + realized_pnl
            peak_equity = max(peak_equity, realized_equity)
            drawdown_abs = max(0.0, peak_equity - realized_equity)
            drawdown_pct = drawdown_abs / max(peak_equity, 1e-9) * 100.0
            max_drawdown_abs = max(max_drawdown_abs, drawdown_abs)
            max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
            equity_curve.append({"at": event_at.isoformat(), "event": "EXIT",
                "trade_ids": [x["id"] for x in closed_now], "realized_equity": round(realized_equity, 8),
                "realized_pnl_cumulative": round(realized_pnl, 8),
                "deployed_margin_after_exit": round(sum(x["margin"] for x in active), 8),
                "drawdown_from_realized_peak": round(drawdown_abs, 8),
                "drawdown_pct_from_realized_peak": round(drawdown_pct, 6)})
        if at > cursor:
            margin_hours += sum(x["margin"] for x in active) * (at - cursor).total_seconds() / 3600.0
        last_time = max(last_time, at)

    for candidate in candidates:
        trade = candidate["trade"]
        opened = candidate["opened"]
        settle_until(opened)
        symbol_side = (trade.symbol.upper(), trade.side.upper())
        if sum(1 for x in active if x["symbol_side"] == symbol_side) >= int(cfg.get("max_positions_per_symbol_direction", 1)):
            skipped_correlation += 1
            skipped.append({"trade_id": trade.id, "strategy": trade.strategy, "symbol": trade.symbol,
                "side": trade.side, "opened_at": opened.isoformat(), "reason": "SAME_SYMBOL_DIRECTION_ALREADY_ACTIVE"})
            continue
        equity = max(0.0, initial + realized_pnl)
        deployed = sum(x["margin"] for x in active)
        total_cap = equity * float(cfg.get("max_total_margin_pct", 40.0)) / 100.0
        per_trade_cap = equity * float(cfg.get("max_margin_pct", 10.0)) / 100.0
        free = max(0.0, equity - deployed)
        remaining_cap = max(0.0, total_cap - deployed)
        leverage = max(1.0, min(float(cfg.get("max_leverage", 3.0)), float(trade.leverage or 1.0)))
        margin = min(per_trade_cap, free, remaining_cap)
        notional = min(margin * leverage, float(cfg.get("max_notional_usdt", 50.0)))
        margin = notional / leverage if leverage else 0.0
        if notional < 5.0 or margin <= 0.0:
            skipped_margin += 1
            skipped.append({"trade_id": trade.id, "strategy": trade.strategy, "symbol": trade.symbol,
                "side": trade.side, "opened_at": opened.isoformat(), "reason": "INSUFFICIENT_SHARED_WALLET_FREE_MARGIN"})
            continue
        replay_base_notional = max(float(candidate["row"].get("replay_base_notional") or trade.notional or 0.0), 1e-12)
        scale = notional / replay_base_notional
        net = candidate["original_net"] * scale
        end_at = candidate["exit_at"]
        active.append({"id": trade.id, "symbol_side": symbol_side, "margin": margin,
            "notional": notional, "net_pnl": net, "exit_at": end_at,
            "realized": candidate["realized"], "opened": opened})
        accepted.append({"trade_id": trade.id, "strategy": trade.strategy, "symbol": trade.symbol,
            "side": trade.side, "opened_at": opened.isoformat(),
            "new_exit_at": end_at.isoformat() if end_at else None,
            "new_exit_reason": candidate["row"].get("new_exit_reason"),
            "margin_used": round(margin, 8), "notional": round(notional, 8),
            "leverage": round(leverage, 4), "size_scale_vs_old_trade": round(scale, 8),
            "new_engine_net_pnl_estimate": round(net, 8),
            "pnl_status": "COUNTERFACTUAL_REALIZED_ESTIMATE" if candidate["realized"] else "UNREALIZED_AT_LAST_CANDLE",
            "allocation_status": "ACCEPTED"})
        total_margin_deployed += margin
        total_notional_deployed += notional
        simultaneous_margin = sum(x["margin"] for x in active)
        peak_margin = max(peak_margin, simultaneous_margin)
        peak_wallet_utilization_pct = max(peak_wallet_utilization_pct, simultaneous_margin / max(equity, 0.01) * 100.0)
        max_positions = max(max_positions, len(active))
        equity_curve.append({"at": opened.isoformat(), "equity_before_entry": round(equity, 8),
            "deployed_margin": round(deployed + margin, 8), "free_margin": round(max(0.0, equity-deployed-margin), 8)})

    # Settle policy exits through the last observed audit time; unresolved positions
    # remain reserved and are reported as unrealized, never silently realized.
    final_candidates = [x["exit_at"] or x.get("audit_end") or x["opened"] for x in active]
    final_time = max([last_time, *final_candidates]) if final_candidates else last_time
    settle_until(final_time)
    unresolved = [x for x in active if not x["realized"]]
    unresolved_pnl = sum(x["net_pnl"] for x in unresolved)
    final_equity = initial + realized_pnl + unresolved_pnl
    wins = sum(1 for x in accepted if x["pnl_status"] == "COUNTERFACTUAL_REALIZED_ESTIMATE" and x["new_engine_net_pnl_estimate"] > 0)
    losses = sum(1 for x in accepted if x["pnl_status"] == "COUNTERFACTUAL_REALIZED_ESTIMATE" and x["new_engine_net_pnl_estimate"] < 0)
    gross_wins = sum(x["new_engine_net_pnl_estimate"] for x in accepted if x["pnl_status"] == "COUNTERFACTUAL_REALIZED_ESTIMATE" and x["new_engine_net_pnl_estimate"] > 0)
    gross_losses = sum(x["new_engine_net_pnl_estimate"] for x in accepted if x["pnl_status"] == "COUNTERFACTUAL_REALIZED_ESTIMATE" and x["new_engine_net_pnl_estimate"] < 0)
    period_start = min((x["opened"] for x in candidates), default=last_time)
    period_end = max([last_time, *[x.get("audit_end") or x.get("exit_at") or x["opened"] for x in candidates]]) if candidates else last_time
    duration_hours = max(0.0, (period_end - period_start).total_seconds() / 3600.0)
    summary = {
        "capital_usdt": round(initial, 8), "final_equity_including_open_mark_estimate": round(final_equity, 8),
        "counterfactual_realized_net_pnl": round(realized_pnl, 8),
        "unrealized_net_pnl_at_last_candle": round(unresolved_pnl, 8),
        "accepted_trades": len(accepted), "rejected_or_unknown_candle_audit": unknown,
        "skipped_insufficient_shared_wallet": skipped_margin, "skipped_same_symbol_direction_overlap": skipped_correlation,
        "total_margin_deployed_across_trades": round(total_margin_deployed, 8),
        "total_notional_deployed_across_trades": round(total_notional_deployed, 8),
        "peak_simultaneous_margin": round(peak_margin, 8),
        "peak_wallet_utilization_pct": round(peak_wallet_utilization_pct, 4),
        "max_realized_equity_drawdown_usdt": round(max_drawdown_abs, 8),
        "max_realized_equity_drawdown_pct": round(max_drawdown_pct, 6),
        "return_on_starting_wallet_pct_including_open_marks": round((final_equity - initial) / max(initial, 0.01) * 100.0, 6),
        "margin_hours": round(margin_hours, 6),
        "average_margin_over_observation_period": round(margin_hours / max(duration_hours, 1e-9), 8),
        "maximum_simultaneous_positions": max_positions,
        "still_open_unrealized_positions": len(unresolved),
        "closed_wins": wins, "closed_losses": losses,
        "realized_profit_factor": round(gross_wins / abs(gross_losses), 6) if gross_losses < 0 else None,
        "fee_slippage_funding": "Scaled from the existing exit-engine candle replay estimates; historic fee/funding inputs may be estimates, not verified exchange settlements.",
        "method": "Chronological $200 shared-wallet replay across RECOVERY and MOMENTUM. Existing new exit engine policy replay supplies the exit time/PnL; allocation is recalculated at entry using current configured caps. Unknown candle paths are excluded, not fabricated.",
        "paper_only": True
    }
    return {"summary": summary, "trades": accepted, "skipped_trades": skipped,
            "unknown_or_incomplete_trades": unknown_rows, "equity_curve": equity_curve}


def build_leader_following_strategy_replay(trades: list[PaperTrade], cfg: dict, persist: bool = False) -> dict:
    """Replay the revised leader-following policy, including re-entry cycles."""
    closed = [t for t in trades if t.status == "CLOSED"]
    if not closed:
        return {"summary": {}, "trades": [], "source": "No closed paper trades"}
    trader_ids = sorted({t.trader_id for t in closed})
    earliest = min((_shadow_dt(t.opened_at) or now()) for t in closed)
    with SessionLocal() as db:
        snapshots = db.scalars(select(PositionSnapshot).where(
            PositionSnapshot.trader_id.in_(trader_ids), PositionSnapshot.observed_at >= earliest
        ).order_by(PositionSnapshot.trader_id, PositionSnapshot.observed_at, PositionSnapshot.id)).all()
        snapshot_ids = [s.id for s in snapshots]
        observations = db.scalars(select(PositionObservation).where(
            PositionObservation.snapshot_id.in_(snapshot_ids)
        ).order_by(PositionObservation.snapshot_id, PositionObservation.id)).all() if snapshot_ids else []
        history = db.scalars(select(HistoryPosition).where(HistoryPosition.trader_id.in_(trader_ids))).all()
    obs_by_snapshot: dict[int, list[PositionObservation]] = {}
    snaps_by_trader: dict[str, list[PositionSnapshot]] = {}
    for obs in observations:
        obs_by_snapshot.setdefault(obs.snapshot_id, []).append(obs)
    for snap in snapshots:
        snaps_by_trader.setdefault(snap.trader_id, []).append(snap)
    history_by_id: dict[tuple[str, str], list[HistoryPosition]] = {}
    for item in history:
        if item.position_id:
            history_by_id.setdefault((item.trader_id, str(item.position_id)), []).append(item)

    rows = []
    for trade in closed:
        raw = parse_json(trade.raw_json)
        audit_at = raw.get("post_exit_recovery_audit_updated_at")
        cached = raw.get("leader_following_replay_v1")
        if (isinstance(cached, dict) and int(raw.get("leader_following_replay_version", 0) or 0) == 1
                and raw.get("leader_following_replay_source_audit_at") == audit_at):
            rows.append(cached)
            continue
        entry_dt, old_exit = _shadow_dt(trade.opened_at), _shadow_dt(trade.closed_at)
        common = {"trade_id": trade.id, "trader_id": trade.trader_id, "trader_name": trade.trader_name,
                  "strategy": (trade.strategy or raw.get("strategy") or "RECOVERY").upper(),
                  "symbol": trade.symbol, "side": trade.side,
                  "old_realized_net_pnl": round(float(trade.net_pnl or 0.0), 8),
                  "old_exit_reason": trade.exit_reason}
        if not entry_dt or not old_exit:
            rows.append({**common, "replay_status": "MISSING_ENTRY_OR_EXIT_TIMESTAMP"})
            continue
        pre = raw.get("entry_timing_mark_candles")
        post = raw.get("post_exit_mark_candles")
        if int(raw.get("post_exit_recovery_audit_version", 0) or 0) != 9 or not isinstance(pre, list) or not isinstance(post, list) or not pre or not post:
            rows.append({**common, "replay_status": "PENDING_CANDLE_REHYDRATION"})
            continue
        lifecycle_audit = raw.get("closed_trade_candle_audit") or {}
        if lifecycle_audit.get("status") not in (None, "COMPLETE"):
            rows.append({**common, "replay_status": "INCOMPLETE_ENTRY_TO_EXIT_CANDLE_PATH"})
            continue
        candles = sorted(pre + post, key=lambda c: int(c[0]) if isinstance(c, (list, tuple)) and c else 0)
        entry_ms, old_exit_ms = int(entry_dt.timestamp() * 1000), int(old_exit.timestamp() * 1000)
        leader_key = trade.source_key.split("|", 1)[1] if "|" in trade.source_key else ""
        position_id = leader_key.split("|", 1)[0] if leader_key else ""
        closes = history_by_id.get((trade.trader_id, position_id), []) if position_id else []
        confirmed_close_ms = max((int(h.closed_at_ms) for h in closes if h.closed_at_ms
            and h.symbol.upper() == trade.symbol.upper() and h.side.upper() == trade.side.upper()
            and str(h.status or "").upper() in ("CLOSED", "CLOSE")), default=None)
        leader_timeline = []
        for snap in snaps_by_trader.get(trade.trader_id, []):
            snap_at = _shadow_dt(snap.observed_at)
            if snap_at is None:
                continue
            exact = next((o for o in obs_by_snapshot.get(snap.id, []) if o.position_key == leader_key), None)
            opposite_side = "SHORT" if trade.side.upper() == "LONG" else "LONG"
            opposite = next((o for o in obs_by_snapshot.get(snap.id, []) if o.symbol.upper() == trade.symbol.upper()
                             and o.side.upper() == opposite_side), None)
            leader_timeline.append((int(snap_at.timestamp() * 1000), bool(snap.hidden), exact, opposite))
        timeline_i = 0
        latest_leader = None
        candle_closes: list[float] = []
        processed_candles: list = []
        last_replay_ms = None
        cycles = []
        active = None
        realized_total = 0.0
        replay_leverage = max(1.0, min(float(cfg.get("max_leverage", 3.0)), float(trade.leverage or 1.0)))
        base_margin = min(200.0 * float(cfg.get("max_margin_pct", 10.0)) / 100.0,
                          float(cfg.get("max_notional_usdt", 50.0)) / replay_leverage)
        base_notional = base_margin * replay_leverage
        qty_per_cycle = base_notional
        strategy = common["strategy"]
        hold_limit = float(cfg.get("momentum_max_hold_hours", 24.0) if strategy == "MOMENTUM"
                           else cfg.get("recovery_max_hold_hours", 72.0))
        lookback = max(15, min(240, int(cfg.get("structure_lookback_minutes", 60) or 60)))
        structure_buffer = max(0.05, min(2.0, float(cfg.get("structure_break_buffer_pct", 0.20) or 0.20)))
        structure_required = max(2, min(10, int(cfg.get("structure_break_confirmations", 3) or 3)))
        funding_points = raw.get("funding_history_replay_points") or []
        funding_verified = bool(raw.get("funding_history_replay_verified"))
        funding_rate = abs(safe_float(raw.get("funding_rate_at_entry")) or 0.0)
        next_funding = safe_float(raw.get("next_funding_time"))
        policy_multiple = 1.15 if strategy == "MOMENTUM" else 1.35

        def leader_state(at_ms):
            nonlocal timeline_i, latest_leader
            while timeline_i < len(leader_timeline) and leader_timeline[timeline_i][0] <= at_ms:
                latest_leader = leader_timeline[timeline_i]
                timeline_i += 1
            if confirmed_close_ms and confirmed_close_ms <= at_ms:
                return "CLOSED", None
            if latest_leader is None or latest_leader[1] or at_ms - latest_leader[0] > 15 * 60 * 1000:
                return "UNKNOWN", None
            _, _, exact, opposite = latest_leader
            if exact is not None:
                return ("OPEN", exact) if exact.side.upper() == trade.side.upper() else ("REVERSED", exact)
            if opposite is not None:
                return "REVERSED", opposite
            return "UNKNOWN", None

        def cycle_pnl(cycle, price, at_ms):
            direction = 1.0 if trade.side.upper() == "LONG" else -1.0
            gross = direction * (price - cycle["entry_price"]) * cycle["quantity"]
            fees = (cycle["entry_price"] + price) * cycle["quantity"] * (float(cfg["taker_fee_pct"]) + float(cfg["slippage_pct"])) / 100.0
            if funding_verified:
                fund = sum(float(point.get("funding_cost") or 0.0)
                           * cycle["quantity"] / max(float(trade.quantity or 0.0), 1e-12)
                           for point in funding_points
                           if cycle["opened_ms"] < int(point.get("funding_time") or 0) <= at_ms)
                funding_source = "BINANCE_FUNDING_RATE_HISTORY"
            elif next_funding:
                next_ms = int(next_funding)
                while next_ms <= cycle["opened_ms"]:
                    next_ms += 28800000
                n = 0 if at_ms < next_ms else 1 + int((at_ms - next_ms) // 28800000)
                fund = base_notional * funding_rate * n
                funding_source = "CONSERVATIVE_ENTRY_RATE_ESTIMATE"
            else:
                fund, funding_source = 0.0, "UNVERIFIED_NO_FUNDING_SCHEDULE"
            return gross - fees - fund, gross, fees, fund, funding_source

        def setup_valid(obs, momentum_pct):
            if obs is None or not obs.entry_price or not obs.mark_price or not momentum_pct:
                return False
            direction = 1.0 if trade.side.upper() == "LONG" else -1.0
            aligned_momentum = direction * momentum_pct
            if aligned_momentum < float(cfg.get("momentum_min_5m_pct", 0.02)):
                return False
            if strategy == "MOMENTUM":
                leader_profit = direction * (float(obs.mark_price) / float(obs.entry_price) - 1.0) * 100.0
                return leader_profit >= float(cfg.get("momentum_min_leader_profit_pct", 0.05))
            adverse = ((float(obs.entry_price) - float(obs.mark_price)) / float(obs.entry_price) * 100.0
                       if trade.side.upper() == "LONG" else
                       (float(obs.mark_price) - float(obs.entry_price)) / float(obs.entry_price) * 100.0)
            return adverse >= float(cfg.get("min_adverse_pct", 0.30))

        for candle in candles:
            if not isinstance(candle, (list, tuple)) or len(candle) < 4:
                continue
            at_ms = int(candle[0]); high, low, close = safe_float(candle[1]), safe_float(candle[2]), safe_float(candle[3])
            if at_ms < entry_ms or high is None or low is None or close is None:
                continue
            state, leader_obs = leader_state(at_ms)
            candle_closes.append(float(close))
            processed_candles.append(candle)
            last_replay_ms = at_ms
            if active is None:
                if not cycles:
                    if at_ms < entry_ms:
                        continue
                    entry_price, opened_ms = float(trade.entry_price), entry_ms
                else:
                    if state != "OPEN" or len(candle_closes) <= 5:
                        continue
                    momentum_pct = (close / candle_closes[-6] - 1.0) * 100.0
                    if not setup_valid(leader_obs, momentum_pct):
                        continue
                    entry_price, opened_ms = close, at_ms
                qty = qty_per_cycle / max(entry_price, 1e-12)
                active = {"entry_price": entry_price, "opened_ms": opened_ms, "quantity": qty,
                          "mfe": None, "support_level": None, "break_count": 0}
            pnl, gross, fees, fund, fund_source = cycle_pnl(active, close, at_ms)
            active["mfe"] = max(float(active["mfe"] if active["mfe"] is not None else pnl), pnl)
            held_hours = (at_ms - active["opened_ms"]) / 3600000.0
            cost_floor = max((fees + max(fund, 0.0)) * 1.25, 0.02)
            meaningful_mfe = active["mfe"] >= cost_floor * policy_multiple
            exit_reason = None
            if state == "REVERSED":
                exit_reason = "LEADER_POSITION_REVERSED"
            elif state == "CLOSED":
                exit_reason = "LEADER_POSITION_CLOSED"
            elif pnl >= cost_floor and meaningful_mfe:
                exit_reason = "MEANINGFUL_MFE_PROFIT_CAPTURE"
            if exit_reason is None:
                prior = processed_candles[max(0, len(processed_candles) - lookback - 1):len(processed_candles) - 1]
                prior_prices = [(safe_float(c[2]) if trade.side.upper() == "LONG" else safe_float(c[1]))
                                for c in prior if isinstance(c, (list, tuple)) and len(c) >= 3]
                prior_prices = [v for v in prior_prices if v is not None and v > 0]
                if len(prior_prices) >= 12:
                    boundary = safe_float(active.get("support_level"))
                    boundary = boundary or (min(prior_prices) if trade.side.upper() == "LONG" else max(prior_prices))
                    broken = (close < boundary * (1 - structure_buffer / 100.0) if trade.side.upper() == "LONG"
                              else close > boundary * (1 + structure_buffer / 100.0))
                    active["break_count"] = int(active.get("break_count", 0)) + 1 if broken else 0
                    active["support_level"] = boundary if broken else None
                    if active["break_count"] >= structure_required:
                        exit_reason = "MARKET_STRUCTURE_BROKEN"
            if exit_reason is None and held_hours >= hold_limit:
                exit_reason = "MAX_HOLD_TIME_EXCEEDED"
            if exit_reason:
                cycle = {"cycle": len(cycles) + 1, "entry_at": datetime.fromtimestamp(active["opened_ms"] / 1000, timezone.utc).isoformat(),
                         "entry_price": active["entry_price"], "exit_at": datetime.fromtimestamp(at_ms / 1000, timezone.utc).isoformat(),
                         "exit_price": close, "exit_reason": exit_reason, "hold_hours": round(held_hours, 4),
                         "net_pnl_estimate": round(pnl, 8), "gross_pnl_estimate": round(gross, 8),
                         "fee_slippage_estimate": round(fees, 8), "funding_estimate": round(fund, 8),
                         "funding_source": fund_source, "mfe_net_estimate": round(float(active["mfe"]), 8),
                         "leader_state_at_exit": state, "execution_quality": "BINANCE_MARK_PRICE_1M_CLOSE; OHLC EXTREMA NOT ASSUMED FILLS"}
                cycles.append(cycle)
                realized_total += pnl
                active = None

        if active is not None and candle_closes:
            at_ms = int(last_replay_ms); price = float(candle_closes[-1])
            pnl, gross, fees, fund, fund_source = cycle_pnl(active, price, at_ms)
            open_cycle = {"cycle": len(cycles) + 1, "entry_at": datetime.fromtimestamp(active["opened_ms"] / 1000, timezone.utc).isoformat(),
                          "entry_price": active["entry_price"], "last_observed_at": datetime.fromtimestamp(at_ms / 1000, timezone.utc).isoformat(),
                          "last_observed_price": price, "net_pnl_estimate": round(pnl, 8),
                          "gross_pnl_estimate": round(gross, 8), "exit_reason": "STILL_OPEN_AT_LAST_CANDLE",
                          "funding_source": fund_source}
            cycles.append(open_cycle)
            open_unrealized = pnl
            status = "OPEN_UNREALIZED_AT_LAST_CANDLE"
        else:
            open_unrealized = 0.0
            status = "STRATEGY_REPLAY_COMPLETE" if cycles else "NO_VALID_CANDLE_IN_REPLAY_PATH"
        row = {**common, "replay_status": status, "cycles": cycles,
               "completed_cycles": sum(1 for c in cycles if c.get("exit_reason") != "STILL_OPEN_AT_LAST_CANDLE"),
               "reentry_count": max(0, len(cycles) - 1),
               "revised_policy_realized_net_estimate": round(realized_total, 8),
               "open_unrealized_net_estimate": round(open_unrealized, 8),
               "total_net_estimate_including_open": round(realized_total + open_unrealized, 8),
               "replay_base_notional": round(base_notional, 8), "replay_base_margin": round(base_margin, 8),
               "policy": "MAE_ONLY_NEVER_EXITS; HARVESTS_COST_COVERED_MEANINGFUL_MFE; REENTERS_ONLY_WHILE_EXACT_LEADER_IS_FRESH_OPEN_AND_SETUP_QUALIFIES; EXITS_ON_CONFIRMED_LEADER_CLOSE_OR_REVERSAL, CONFIRMED_STRUCTURE_BREAK, OR STRATEGY_HOLD_LIMIT",
               "candle_source": "BINANCE_USDM_MARK_PRICE_1M_CLOSES; close-based fills only; no OHLC wick fills"}
        rows.append(row)

    priced = [r for r in rows if r.get("replay_status") in ("STRATEGY_REPLAY_COMPLETE", "OPEN_UNREALIZED_AT_LAST_CANDLE")]
    summary = {
        "closed_trade_cohort": len(closed), "replayable_trades": len(priced),
        "pending_or_incomplete_trades": len(rows) - len(priced),
        "old_system_realized_net_pnl": round(sum(float(t.net_pnl or 0.0) for t in closed), 8),
        "revised_policy_realized_net_estimate": round(sum(float(r.get("revised_policy_realized_net_estimate") or 0.0) for r in priced), 8),
        "revised_policy_open_unrealized_net_estimate": round(sum(float(r.get("open_unrealized_net_estimate") or 0.0) for r in priced), 8),
        "revised_policy_total_including_open_estimate": round(sum(float(r.get("total_net_estimate_including_open") or 0.0) for r in priced), 8),
        "completed_cycles": sum(int(r.get("completed_cycles", 0) or 0) for r in priced),
        "reentries": sum(int(r.get("reentry_count", 0) or 0) for r in priced),
        "trades_with_no_mae_only_exit": len(priced),
        "pending_candle_rehydration": sum(1 for r in rows if r.get("replay_status") == "PENDING_CANDLE_REHYDRATION"),
        "incomplete_entry_path": sum(1 for r in rows if r.get("replay_status") == "INCOMPLETE_ENTRY_TO_EXIT_CANDLE_PATH"),
        "paper_only": True,
        "caveat": "Counterfactual simulation from available Binance 1m mark-price candles and time-aligned public leader observations; re-entries require fresh exact leader evidence and qualifying strategy setup. It is not actual execution or realized PnL."
    }
    if persist:
        with SessionLocal() as db:
            for row in rows:
                if row.get("replay_status") == "PENDING_CANDLE_REHYDRATION":
                    continue
                live = db.get(PaperTrade, int(row["trade_id"]))
                if not live:
                    continue
                live_raw = parse_json(live.raw_json)
                live_raw["leader_following_replay_v1"] = row
                live_raw["leader_following_replay_version"] = 1
                live_raw["leader_following_replay_source_audit_at"] = live_raw.get("post_exit_recovery_audit_updated_at")
                live_raw["leader_following_replay_updated_at"] = now().isoformat()
                live_raw.pop("post_exit_mark_candles", None)
                live_raw.pop("entry_timing_mark_candles", None)
                live.raw_json = json.dumps(live_raw, separators=(",", ":"), default=str)
            db.commit()
    return {"summary": summary, "trades": rows,
            "source": "Stored Binance USD-M mark-price candles + time-aligned leader observations; historical input source follows each trade's recorded provenance",
            "persisted": bool(persist)}


@app.get("/api/paper/strategy-replay")
async def api_paper_strategy_replay():
    with SessionLocal() as db:
        closed = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED").order_by(PaperTrade.opened_at, PaperTrade.id)).all()
    result = build_leader_following_strategy_replay(closed, paper_config())
    result["rehydration"] = {"audited": 0, "attempted": 0,
                             "pending": result.get("summary", {}).get("pending_candle_rehydration", 0),
                             "blocked": binance_rest_status()["blocked"],
                             "status": "read_only_no_automatic_fetch"}
    return result


@app.get("/api/paper/research-replay")
async def api_paper_research_replay():
    cfg = paper_config()
    with SessionLocal() as db:
        closed = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED").order_by(PaperTrade.opened_at, PaperTrade.id)).all()
    return build_new_exit_policy_replay(closed, cfg)


@app.get("/api/paper/capital-replay")
async def api_paper_capital_replay():
    cfg = paper_config()
    wallet_epoch = _shadow_dt(datetime.fromisoformat(str(cfg.get("wallet_epoch_at")).replace("Z", "+00:00"))) or now()
    with SessionLocal() as db:
        closed = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED").order_by(PaperTrade.opened_at, PaperTrade.id)).all()
        open_rows = db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN")).all()
    legacy_open = [t for t in open_rows if (_shadow_dt(t.opened_at) or wallet_epoch) < wallet_epoch]
    replay = build_new_exit_policy_replay(closed, cfg)
    result = build_shared_wallet_new_engine_replay(closed, cfg, replay.get("trades", []), 200.0)
    result["summary"]["legacy_open_positions_excluded_from_historical_closed_trade_replay"] = len(legacy_open)
    result["summary"]["legacy_open_recorded_margin_excluded"] = round(sum(float(t.margin_used or 0.0) for t in legacy_open), 8)
    result["summary"]["legacy_open_recorded_notional_excluded"] = round(sum(float(t.notional or 0.0) for t in legacy_open), 8)
    result["summary"]["wallet_epoch_at"] = wallet_epoch.isoformat()
    set_setting("capital_replay_200", {"summary": result["summary"], "updated_at": now().isoformat()})
    return result


@app.post("/api/paper/audit/recovery")
async def api_paper_run_recovery_audit():
    """Run a bounded candle batch, then persist the existing exit-engine replay."""
    result = await run_post_exit_recovery_audit(max_trades=20)
    with SessionLocal() as db:
        closed = db.scalars(select(PaperTrade).where(PaperTrade.status == "CLOSED").order_by(PaperTrade.opened_at, PaperTrade.id)).all()
    replay = build_new_exit_policy_replay(closed, paper_config())
    result["engine_replay_persisted"] = sum(1 for row in replay.get("trades", []) if row.get("replay_status") in {"POLICY_EXIT_SIMULATED", "OPEN_UNREALIZED_AT_LAST_CANDLE"})
    result["engine_replay_pending"] = sum(1 for row in replay.get("trades", []) if row.get("replay_status") not in {"POLICY_EXIT_SIMULATED", "OPEN_UNREALIZED_AT_LAST_CANDLE"})
    result["engine_replay_summary"] = replay.get("summary", {})
    return result


@app.get("/api/paper/{trade_id}")
async def api_paper_trade_detail(trade_id: int):
    with SessionLocal() as db:
        trade = db.get(PaperTrade, trade_id)
        if not trade:
            raise HTTPException(404, "Paper trade not found")
        raw = parse_json(trade.raw_json)
        trader = db.get(Trader, trade.trader_id)
        leader_obs = _leader_for_trade(db, trade)
        path = db.scalars(
            select(PaperTradePath).where(PaperTradePath.paper_trade_id == trade.id)
            .order_by(PaperTradePath.observed_at)
        ).all()
        events = db.scalars(
            select(PaperTradeEvent).where(PaperTradeEvent.paper_trade_id == trade.id)
            .order_by(PaperTradeEvent.observed_at)
        ).all()
        campaign_rows = db.scalars(
            select(PaperTrade).where(PaperTrade.source_key == trade.source_key)
            .order_by(PaperTrade.opened_at)
        ).all()

        leader_path = []
        leader_snapshot = None
        latest_leader_snapshot = db.scalar(
            select(PositionSnapshot).where(PositionSnapshot.trader_id == trade.trader_id)
            .order_by(desc(PositionSnapshot.id)).limit(1)
        )
        if "|" in trade.source_key:
            leader_trader_id, leader_position_key = trade.source_key.split("|", 1)
            leader_observations = db.execute(
                select(PositionObservation, PositionSnapshot)
                .join(PositionSnapshot, PositionSnapshot.id == PositionObservation.snapshot_id)
                .where(
                    PositionObservation.trader_id == leader_trader_id,
                    PositionObservation.position_key == leader_position_key
                )
                .order_by(PositionSnapshot.observed_at)
                .limit(500)
            ).all()
            leader_path = [{
                "at": snapshot.observed_at.isoformat(),
                "mark_price": observation.mark_price,
                "entry_price": observation.entry_price,
                "unrealized_pnl": observation.unrealized_pnl,
                "snapshot_id": snapshot.id
            } for observation, snapshot in leader_observations if not snapshot.hidden]
        if leader_obs:
            leader_snapshot = db.get(PositionSnapshot, leader_obs.snapshot_id)

        latest_contains_position = False
        if latest_leader_snapshot and not latest_leader_snapshot.hidden:
            latest_contains_position = bool(db.scalar(
                select(PositionObservation.id).where(
                    PositionObservation.snapshot_id == latest_leader_snapshot.id,
                    PositionObservation.position_key == (
                        trade.source_key.split("|", 1)[1] if "|" in trade.source_key else ""
                    )
                ).limit(1)
            ))

    max_gap = max(
        [float(path_point.data_gap_seconds or 0.0) for path_point in path] +
        [float(raw.get("max_data_gap_seconds", 0.0) or 0.0)]
    )
    quality_status = raw.get("data_quality_status")
    if not quality_status:
        quality_status = "DATA_INTERRUPTED_REVIEW" if max_gap > 15 else ("PATH_NOT_RECORDED" if not path else "LEGACY_UNCLASSIFIED")

    campaign_trade_details = []
    for item in campaign_rows:
        item_raw = parse_json(item.raw_json)
        item_opened = _shadow_dt(item.opened_at)
        item_closed = _shadow_dt(item.closed_at)
        hold_seconds = max(0.0, (item_closed - item_opened).total_seconds()) if item_opened and item_closed else None
        campaign_trade_details.append({
            "id": item.id, "attempt_number": len(campaign_trade_details) + 1,
            "status": item.status, "entry_price": item.entry_price,
            "exit_price": item.exit_price, "gross_pnl": round(float(item.gross_pnl or 0.0), 8),
            "estimated_costs": round(float(item.estimated_costs or 0.0), 8),
            "net_pnl": round(float(item.net_pnl or 0.0), 8),
            "mfe_net_pnl": safe_float(item_raw.get("mfe_net_pnl")),
            "mae_net_pnl": safe_float(item_raw.get("mae_net_pnl")),
            "post_exit_status": item_raw.get("post_exit_recovery_status"),
            "post_exit_audit_status": item_raw.get("post_exit_recovery_audit_status", "PENDING"),
            "post_exit_max_net_pnl": safe_float(item_raw.get("post_exit_max_net_pnl")),
            "post_exit_minutes_to_entry": safe_float(item_raw.get("post_exit_minutes_to_entry")),
            "post_exit_minutes_to_net_profit": safe_float(item_raw.get("post_exit_minutes_to_net_profit")),
            "post_exit_minutes_to_best": safe_float(item_raw.get("post_exit_minutes_to_best")),
            "post_exit_max_at": item_raw.get("post_exit_max_at"),
            "opened_at": item.opened_at.isoformat(),
            "closed_at": item.closed_at.isoformat() if item.closed_at else None,
            "hold_duration_seconds": round(hold_seconds, 2) if hold_seconds is not None else None,
            "hold_duration_hours": round(hold_seconds / 3600, 3) if hold_seconds is not None else None,
            "exit_reason": item.exit_reason
        })
    campaign = {
        "trade_count": len(campaign_rows),
        "reentry_count": max(0, len(campaign_rows) - 1),
        "open_count": sum(1 for item in campaign_rows if item.status == "OPEN"),
        "realized_net_pnl": round(sum(float(item.net_pnl or 0.0) for item in campaign_rows if item.status == "CLOSED"), 8),
        "total_net_pnl": round(sum(float(item.net_pnl or 0.0) for item in campaign_rows), 8),
        "sum_positive_attempt_pnl": round(sum(max(0.0, float(item.net_pnl or 0.0)) for item in campaign_rows), 8),
        "sum_negative_attempt_pnl": round(sum(min(0.0, float(item.net_pnl or 0.0)) for item in campaign_rows), 8),
        "trade_ids": [item.id for item in campaign_rows],
        "trades": campaign_trade_details
    }

    leader = None
    recovery = None
    if leader_obs:
        lentry = leader_obs.entry_price
        lmark = leader_obs.mark_price
        if trade.side == "LONG" and lentry and lmark:
            leader_adverse = (lentry - lmark) / lentry * 100
        elif trade.side == "SHORT" and lentry and lmark:
            leader_adverse = (lmark - lentry) / lentry * 100
        else:
            leader_adverse = None

        if trade.side == "LONG" and lentry and trade.entry_price:
            recovery_profit_pct = (lentry - trade.entry_price) / trade.entry_price * 100
            recovery_profit_usdt = (lentry - trade.entry_price) * trade.quantity
        elif trade.side == "SHORT" and lentry and trade.entry_price:
            recovery_profit_pct = (trade.entry_price - lentry) / trade.entry_price * 100
            recovery_profit_usdt = (trade.entry_price - lentry) * trade.quantity
        else:
            recovery_profit_pct = recovery_profit_usdt = None

        if trade.exit_reason == "LEADER_POSITION_CLOSED":
            leader_status = "CLOSED_CONFIRMED"
        elif latest_leader_snapshot and latest_leader_snapshot.hidden:
            leader_status = "HIDDEN_OR_UNAVAILABLE"
        elif latest_leader_snapshot and latest_contains_position:
            leader_status = "OPEN_OBSERVED"
        else:
            leader_status = "LAST_OBSERVED_STALE_OR_MISSING"

        leader_entry_cross_after_exit_at = None
        leader_max_favorable_after_exit = None
        leader_recovery_after_exit = False
        leader_recovery_after_exit_minutes = None
        leader_max_pnl_after_exit = None
        if trade.closed_at and lentry:
            closed_epoch = trade.closed_at.timestamp()
            post_exit_leader = [p for p in leader_path if datetime.fromisoformat(p["at"]).timestamp() >= closed_epoch]
            for p in post_exit_leader:
                mp = safe_float(p.get("mark_price"))
                if mp is None:
                    continue
                lpnl = safe_float(p.get("unrealized_pnl"))
                if trade.side == "LONG":
                    favorable = mp - lentry
                    crossed = mp >= lentry
                else:
                    favorable = lentry - mp
                    crossed = mp <= lentry
                if leader_max_favorable_after_exit is None or favorable > leader_max_favorable_after_exit:
                    leader_max_favorable_after_exit = favorable
                if lpnl is not None and (leader_max_pnl_after_exit is None or lpnl > leader_max_pnl_after_exit):
                    leader_max_pnl_after_exit = lpnl
                if crossed and leader_entry_cross_after_exit_at is None:
                    leader_entry_cross_after_exit_at = p["at"]
            if leader_entry_cross_after_exit_at:
                leader_recovery_after_exit = True
                leader_recovery_after_exit_minutes = (datetime.fromisoformat(leader_entry_cross_after_exit_at).timestamp() - closed_epoch) / 60.0
        leader = {
            "trader_id": trade.trader_id,
            "trader_name": trader.name if trader else trade.trader_name,
            "position_key": trade.source_key.split("|", 1)[1] if "|" in trade.source_key else trade.source_key,
            "symbol": leader_obs.symbol, "side": leader_obs.side,
            "entry_price": lentry, "mark_price": lmark,
            "unrealized_pnl": leader_obs.unrealized_pnl, "adverse_pct": leader_adverse,
            "leverage": leader_obs.leverage, "margin_mode": leader_obs.margin_mode,
            "observed_at": leader_snapshot.observed_at.isoformat() if leader_snapshot else None,
            "recovery_price": lentry, "our_recovery_profit_pct": recovery_profit_pct,
            "our_recovery_profit_usdt": recovery_profit_usdt, "status": leader_status,
            "leader_reached_entry_after_exit": leader_recovery_after_exit,
            "leader_reached_entry_after_exit_at": leader_entry_cross_after_exit_at,
            "leader_minutes_to_entry_after_exit": round(leader_recovery_after_exit_minutes, 2) if leader_recovery_after_exit_minutes is not None else None,
            "leader_max_favorable_after_exit": leader_max_favorable_after_exit,
            "leader_max_pnl_after_exit": leader_max_pnl_after_exit
        }
        recovery = recovery_statistics(
            trade.trader_id,
            trade.source_key.split("|", 1)[1] if "|" in trade.source_key else "",
            trade.symbol, trade.side, lentry
        )

    ladder = []
    directional = 1 if trade.side == "LONG" else -1
    known_costs = safe_float(trade.estimated_costs) or 0.0
    if trade.entry_price and trade.mark_price:
        for swing in (0.10, 0.20, 0.30, 0.50, 0.70, 1.00):
            target_price = trade.mark_price * (1 + directional * swing / 100)
            gross = directional * (target_price - trade.entry_price) * trade.quantity
            ladder.append({
                "target_type": "INTERMEDIATE_SWING_FROM_LATEST_MARK",
                "target_pct_from_reference": swing, "reference_price": trade.mark_price,
                "price": target_price, "gross_pnl_usdt": gross,
                "estimated_net_pnl_usdt": gross - known_costs
            })
    if leader and leader.get("recovery_price") and trade.entry_price:
        anchor = leader["recovery_price"]
        for extra in (0.0, 0.3, 0.5, 0.7, 1.0):
            target_price = anchor * (1 + extra / 100) if trade.side == "LONG" else anchor * (1 - extra / 100)
            gross = directional * (target_price - trade.entry_price) * trade.quantity
            ladder.append({
                "target_type": "LEADER_RECOVERY_AND_CONTINUATION",
                "target_pct_from_reference": extra, "reference_price": anchor,
                "price": target_price, "gross_pnl_usdt": gross,
                "estimated_net_pnl_usdt": gross - known_costs
            })

    # Binance candlesticks are cached for 20 seconds. The live paper mark is read
    # from Postgres on each detail refresh, so the UI can refresh frequently without
    # issuing a new historical klines request every time.
    candles = []
    candle_interval = "5m"
    candle_error = None
    try:
        opened = trade.opened_at
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
        age_hours = max(0.0, (now() - opened).total_seconds() / 3600)
        if age_hours > 24:
            candle_interval = "1h"
        cache_key = f"{trade.symbol}:{candle_interval}"
        cached = PAPER_CANDLE_CACHE.get(cache_key)
        if cached and time.monotonic() - float(cached.get("fetched_at", 0.0)) < 60:
            candles = list(cached.get("candles") or [])
            candle_error = cached.get("error")
        else:
            limit = 168 if candle_interval == "1h" else 288
            raw_candles, _ = await binance_get(
                "/fapi/v1/klines",
                {"symbol": trade.symbol, "interval": candle_interval, "limit": limit}
            )
            if isinstance(raw_candles, list):
                candles = [{
                    "open_time": int(item[0]), "open": safe_float(item[1]),
                    "high": safe_float(item[2]), "low": safe_float(item[3]),
                    "close": safe_float(item[4]), "volume": safe_float(item[5]),
                    "close_time": int(item[6])
                } for item in raw_candles if isinstance(item, (list, tuple)) and len(item) >= 7]
            PAPER_CANDLE_CACHE[cache_key] = {
                "fetched_at": time.monotonic(), "candles": candles, "error": None
            }
    except Exception as exc:
        candle_error = str(exc)[:220]
        cache_key = f"{trade.symbol}:{candle_interval}"
        cached = PAPER_CANDLE_CACHE.get(cache_key)
        if cached and cached.get("candles"):
            candles = list(cached["candles"])
        else:
            PAPER_CANDLE_CACHE[cache_key] = {
                "fetched_at": time.monotonic(), "candles": [], "error": candle_error
            }

    detail_cfg = paper_config()
    detail_wallet_epoch = _shadow_dt(datetime.fromisoformat(str(detail_cfg.get("wallet_epoch_at")).replace("Z", "+00:00"))) or now()
    single_replay = build_new_exit_policy_replay([trade], detail_cfg)
    replay_trade = (single_replay.get("trades") or [None])[0]
    return {
        "new_system_replay": replay_trade,
        "legacy_exit_engine_replay": replay_trade,
        "leader_following_strategy_replay": raw.get("leader_following_replay_v1", {}),
        "trade": {
            "id": trade.id, "status": trade.status, "trader_id": trade.trader_id,
            "wallet_epoch_eligible": bool((_shadow_dt(trade.opened_at) or detail_wallet_epoch) >= detail_wallet_epoch),
            "leader_position_key": trade.source_key.split("|", 1)[1] if "|" in trade.source_key else trade.source_key,
            "symbol": trade.symbol, "side": trade.side, "entry_price": trade.entry_price,
            "mark_price": trade.mark_price, "exit_price": trade.exit_price,
            "exit_reason": trade.exit_reason,
            "exit_reason_display": paper_exit_reason_display(trade, raw),
            "post_exit_recovery_label": raw.get("post_exit_recovery_label"),
            "post_exit_recovery": {
                "status": raw.get("post_exit_recovery_status"),
                "max_net_pnl": safe_float(raw.get("post_exit_max_net_pnl")),
                "max_price": safe_float(raw.get("post_exit_max_price")),
                "max_at": raw.get("post_exit_max_at"),
                "return_to_entry_at": raw.get("post_exit_return_to_entry_at"),
                "market_source": raw.get("post_exit_recovery_market_source")
            }, "gross_pnl": round(float(trade.gross_pnl or 0.0), 8),
            "estimated_costs": round(float(trade.estimated_costs or 0.0), 8),
            "net_pnl": round(float(trade.net_pnl or 0.0), 8),
            "funding_cost_actual": safe_float(raw.get("funding_cost_actual")) if raw.get("funding_history_verified") else None,
            "funding_cost_last_verified": safe_float(raw.get("funding_cost_actual")) if not raw.get("funding_history_verified") else None,
            "funding_estimate_current": safe_float(raw.get("funding_estimate_current")),
            "funding_estimate_at_exit": safe_float(raw.get("funding_estimate_at_exit")),
            "funding_estimate_settlement_count": safe_float(raw.get("funding_estimate_settlement_count")),
            "funding_estimate_rate_abs": safe_float(raw.get("funding_estimate_rate_abs")),
            "funding_history_rows": safe_float(raw.get("funding_history_rows")),
            "funding_history_replay_verified": bool(raw.get("funding_history_replay_verified")),
            "funding_history_replay_rows": safe_float(raw.get("funding_history_replay_rows")),
            "funding_history_replay_expected_rows": safe_float(raw.get("funding_history_replay_expected_rows")),
            "funding_history_replay_source": raw.get("funding_history_replay_source"),
            "closed_trade_candle_audit": raw.get("closed_trade_candle_audit", {}),
            "post_exit_audit_windows": raw.get("post_exit_audit_windows", {}),
            "new_exit_engine_audit": raw.get("new_exit_engine_audit_v1", {}),
            "funding_verified": bool(raw.get("funding_history_verified")),
            "funding_audit_status": raw.get("funding_audit_status", "LEGACY_FUNDING_UNVERIFIED"),
            "funding_source": (
                raw.get("funding_source") if raw.get("funding_history_verified")
                else "CONSERVATIVE_ESTIMATE_PENDING_BINANCE_HISTORY"
                if raw.get("funding_cost_actual") is not None else raw.get("funding_source")
            ),
            "funding_source_last_verified": raw.get("funding_source") if not raw.get("funding_history_verified") else None,
            "funding_verification_error": raw.get("funding_verification_error"),
            "margin_used": trade.margin_used, "leverage": trade.leverage,
            "opened_at": trade.opened_at.isoformat(),
            "closed_at": trade.closed_at.isoformat() if trade.closed_at else None,
            "mfe_net_pnl": safe_float(raw.get("mfe_net_pnl")),
            "mae_net_pnl": safe_float(raw.get("mae_net_pnl")),
            "mfe_pct": safe_float(raw.get("mfe_pct")), "mae_pct": safe_float(raw.get("mae_pct")),
            "last_binance_price_at": raw.get("last_binance_price_at"),
            "last_leader_check_at": raw.get("last_leader_check_at"),
            "data_gap_seconds": safe_float(raw.get("last_data_gap_seconds")),
            "max_data_gap_seconds": round(max_gap, 3),
            "data_quality_status": quality_status,
            "monitoring_status": raw.get("monitoring_status", "NOT_RECORDED"),
            "monitoring_resumed_at": raw.get("monitoring_resumed_at"),
            "meaningful_profit_hits": raw.get("meaningful_profit_hits", {}),
            "funding_history_rows": raw.get("funding_history_rows")
        },
        "trader": {"id": trade.trader_id, "name": trader.name if trader else trade.trader_name},
        "campaign": campaign, "leader": leader, "leader_path": leader_path,
        "recovery": recovery, "potential_ladder": ladder,
        "path": [{
            "at": point.observed_at.isoformat(), "paper_price": point.paper_price,
            "leader_mark": point.leader_mark, "leader_pnl": point.leader_unrealized_pnl,
            "leader_adverse_pct": point.leader_adverse_pct,
            "net_pnl": round(float(point.net_pnl or 0.0), 8),
            "pnl_pct": point.pnl_pct, "source": point.source,
            "gap": point.data_gap_seconds
        } for point in path],
        "events": [{
            "at": event.observed_at.isoformat(), "type": event.event_type,
            "value": event.value, "detail": event.detail
        } for event in events],
        "candles": candles, "candle_interval": candle_interval, "candle_error": candle_error,
        "data_quality": {
            "status": quality_status, "max_gap_seconds": round(max_gap, 3),
            "note": (
                "INFORMATION_CUTOFF: historical PnL is preserved, but the complete path is not considered verified."
                if quality_status == "INFORMATION_CUTOFF" else
                "A recorded gap over 15 seconds makes this path interrupted; it is not a clean continuously-observed failure."
                if quality_status == "DATA_INTERRUPTED_REVIEW" else
                "Path quality is based on persisted observations. Unrecorded historical intervals cannot be reconstructed."
            )
        }
    }
@app.post("/api/paper/settings")
async def update_paper_settings(request: Request):
    body = await request.json()
    cfg = paper_config()
    for key in ["capital_usdt", "risk_pct", "max_margin_pct", "max_total_margin_pct", "max_notional_usdt", "max_positions_per_symbol_direction", "research_unconstrained", "shadow_max_total_margin_pct", "shadow_max_positions_per_symbol_direction", "target_pct", "stop_pct", "taker_fee_pct", "slippage_pct", "max_leverage", "min_adverse_pct", "recovery_max_hold_hours", "momentum_max_hold_hours", "structure_lookback_minutes", "structure_break_buffer_pct", "structure_break_confirmations"]:
        if key in body:
            val = safe_float(body[key])
            if val is None or val < 0:
                raise HTTPException(400, f"Invalid value for {key}")
            cfg[key] = val
    if "auto_enabled" in body:
        cfg["auto_enabled"] = bool(body["auto_enabled"])
    cfg["capital_usdt"] = max(0.0, min(float(cfg["capital_usdt"]), 100000000))
    cfg["risk_pct"] = min(float(cfg["risk_pct"]), 5.0)
    cfg["max_margin_pct"] = min(max(float(cfg["max_margin_pct"]), 1.0), 10.0)
    cfg["max_total_margin_pct"] = min(max(float(cfg["max_total_margin_pct"]), 5.0), 40.0)
    cfg["max_notional_usdt"] = max(5.0, min(float(cfg.get("max_notional_usdt", 50.0)), 50.0))
    cfg["max_positions_per_symbol_direction"] = 1
    # Do not permit the research flag to bypass finite-wallet paper execution.
    cfg["research_unconstrained"] = False
    cfg["shadow_max_total_margin_pct"] = min(max(float(cfg.get("shadow_max_total_margin_pct", 40.0)), 5.0), 100.0)
    cfg["shadow_max_positions_per_symbol_direction"] = min(
        max(int(float(cfg.get("shadow_max_positions_per_symbol_direction", 1))), 1), 5
    )
    cfg["target_pct"] = min(float(cfg["target_pct"]), 20.0)
    cfg["stop_pct"] = min(max(float(cfg["stop_pct"]), 0.05), 20.0)
    cfg["recovery_max_hold_hours"] = min(max(float(cfg.get("recovery_max_hold_hours", 72.0)), 1.0), 720.0)
    cfg["momentum_max_hold_hours"] = min(max(float(cfg.get("momentum_max_hold_hours", 24.0)), 1.0), 240.0)
    cfg["structure_lookback_minutes"] = min(max(int(cfg.get("structure_lookback_minutes", 60)), 15), 240)
    cfg["structure_break_buffer_pct"] = min(max(float(cfg.get("structure_break_buffer_pct", 0.20)), 0.05), 2.0)
    cfg["structure_break_confirmations"] = min(max(int(cfg.get("structure_break_confirmations", 3)), 2), 10)
    cfg["taker_fee_pct"] = min(float(cfg["taker_fee_pct"]), 2.0)
    cfg["slippage_pct"] = min(float(cfg["slippage_pct"]), 2.0)
    cfg["max_leverage"] = min(max(float(cfg["max_leverage"]), 1.0), 3.0)
    cfg["min_adverse_pct"] = min(max(float(cfg["min_adverse_pct"]), 0.05), 20.0)
    set_setting("paper_config", cfg)
    set_setting("paper_config_version", 2)
    return {"saved": True, "config": cfg}

@app.get("/api/health")
async def api_health():
    """Cheap liveness/readiness probe. Never runs full DB telemetry."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar_one()
        return {"status": "ok", "database": "connected", "engine": engine.dialect.name}
    except Exception as exc:
        raise HTTPException(503, f"Database unavailable: {str(exc)[:200]}")

@app.get("/api/db/synthetic")
async def api_db_synthetic():
    """Application-level DB read/write/read probe used to distinguish DB failure from UI/runtime failure."""
    started = time.perf_counter()
    try:
        def _probe():
            with engine.begin() as conn:
                if engine.dialect.name == "postgresql":
                    conn.execute(text("SET LOCAL statement_timeout = '3000ms'"))
                one = conn.execute(text("SELECT 1")).scalar_one()
                db_now = conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
                trader_count = conn.execute(text("SELECT COUNT(*) FROM traders")).scalar_one()
                if engine.dialect.name == "postgresql":
                    conn.execute(text("CREATE TEMP TABLE laes_db_probe (v integer) ON COMMIT DROP"))
                else:
                    conn.execute(text("CREATE TEMP TABLE laes_db_probe (v integer)"))
                conn.execute(text("INSERT INTO laes_db_probe(v) VALUES (731)"))
                probe = conn.execute(text("SELECT v FROM laes_db_probe")).scalar_one()
                conn.execute(text("DROP TABLE laes_db_probe"))
                return one, db_now, trader_count, probe
        one, db_now, trader_count, probe = await asyncio.wait_for(asyncio.to_thread(_probe), timeout=5.0)
        latency_ms = round((time.perf_counter() - started) * 1000, 1)
        return {
            "status": "ok" if one == 1 and probe == 731 else "failed",
            "database": "connected", "engine": engine.dialect.name,
            "read_probe": one == 1, "write_read_delete_probe": probe == 731,
            "trader_count": int(trader_count), "database_time": str(db_now),
            "latency_ms": latency_ms,
            "probe": "SELECT 1 + COUNT(traders) + temp write/read/delete"
        }
    except asyncio.TimeoutError:
        raise HTTPException(504, "Database synthetic probe timed out after 5 seconds")
    except Exception as exc:
        raise HTTPException(503, f"Database synthetic probe failed: {str(exc)[:300]}")

@app.post("/api/client-error")
async def api_client_error(request: Request):
    """Persist browser boot/runtime errors so UI failures are diagnosable from Render logs."""
    try:
        body = await request.json()
        detail = json.dumps({k: body.get(k) for k in ("message", "source", "line", "column", "stack")}, default=str)[:1800]
        with SessionLocal() as db:
            db.add(SourceCheck(source="browser_runtime", checked_at=now(), ok=False, http_status=500, detail=detail))
            db.commit()
        return {"recorded": True}
    except Exception as exc:
        raise HTTPException(503, f"Could not record browser error: {str(exc)[:200]}")

@app.get("/api/system")
async def api_system():
    metrics = db_metrics()
    with SessionLocal() as db:
        counts = {}
        for table_name, model in [
            ("traders", Trader), ("position_snapshots", PositionSnapshot),
            ("position_observations", PositionObservation), ("history_positions", HistoryPosition),
            ("paper_trades", PaperTrade), ("paper_trade_paths", PaperTradePath), ("paper_trade_events", PaperTradeEvent), ("opportunity_decisions", OpportunityDecision), ("source_checks", SourceCheck)
        ]:
            try:
                counts[table_name] = db.scalar(select(func.count()).select_from(model)) or 0
            except Exception:
                counts[table_name] = 0
        stored_trader_count = counts.get("traders", 0)
    return {
        "database": metrics, "counts": counts,
        "monitor": {
            **last_scan, "worker_alive": worker_alive, "interval_seconds": DISCOVERY_INTERVAL,
            "discovery_interval_seconds": DISCOVERY_INTERVAL, "position_refresh_interval_seconds": POSITION_REFRESH_INTERVAL,
            "batch": POSITION_BATCH, "position_batch": POSITION_BATCH,
            "coverage": {
                "stored_discovered_traders": stored_trader_count,
                "rotation_batch": POSITION_BATCH,
                "rotation_scope": ("Qualified Binance public portfolios via Kopyon, ranked by public metrics and refreshed in bounded batches" if KOPYON_ACTIVE else "Qualified Binance public portfolios, ranked by public metrics and refreshed in bounded batches"),
                "open_paper_leader_refresh_seconds": POSITION_REFRESH_INTERVAL,
                "underwater_scan_seconds": 60,
                "fresh_leader_window_seconds": 900
            }
        },
        "data_sources": [
            {"data": "Public trader discovery", "source": "Kopyon v5 Binance Copy Trading API" if KOPYON_ACTIVE else "Binance public Copy Trading page JSON (undocumented website routes)", "request": "bounded public leaderboard pagination", "frequency": f"every {DISCOVERY_INTERVAL} seconds", "batch": TRADER_DISCOVERY_PAGES * 20},
            {"data": "Leader current positions", "source": "Kopyon v5 openPositions" if KOPYON_ACTIVE else "Binance public Copy Trading leader detail page JSON", "request": "one public portfolio id per request", "frequency": f"bounded rotating checks every {POSITION_REFRESH_INTERVAL} seconds; priority for open paper trades", "coverage": "only public positions disclosed by the source; failed/hidden responses are not treated as closed"},
            {"data": "Leader profile and position history", "source": "Kopyon v5 profile, performance and position history" if KOPYON_ACTIVE else "Binance public leader detail and Position History pages", "request": "on-demand; cached for 24 hours", "frequency": "only when a user opens a leader profile"},
            {"data": "Paper opportunity qualification", "source": "LAES DB + Binance market stream", "request": "latest visible leader snapshots + live mark/momentum", "frequency": "every 60 seconds; page refresh is read-only", "coverage": "all latest visible trader snapshots, subject to 15-minute freshness"},
            {"data": "Live paper PnL/exit monitoring", "source": "Binance USD-M markPrice WebSocket", "request": "1s mark-price stream per active symbol", "frequency": "continuous while Render process is awake", "coverage": "open paper symbols + symbols in latest visible leader snapshots"},
            {"data": "Historical candles", "source": "Binance USD-M REST", "request": "klines/markPriceKlines", "frequency": "on demand; cached per symbol", "coverage": "requested trade/symbol only"},
            {"data": "Funding history", "source": "Binance USD-M REST", "request": "funding history", "frequency": "on trade audit/settlement reconciliation; circuit-breaker protected", "coverage": "paper trades needing verification"},
            {"data": "News", "source": "CoinDesk / Cointelegraph / The Block RSS", "request": "RSS feeds", "frequency": "15-minute cache/refresh", "coverage": "general crypto headlines + symbol filter when supported"}
        ],
        "provider": {
            "leader_data_provider": "KOPYON_API_V5" if KOPYON_ACTIVE else "BINANCE_PUBLIC_COPY_TRADING_PAGE",
            "kopyon_base": KOPYON_BASE_URL if KOPYON_ACTIVE else None,
            "kopyon_key_configured": KOPYON_ACTIVE,
            "kopyon_limits": dict(KOPYON_LIMIT_STATUS) if KOPYON_ACTIVE else None,
            "binance_public_copy_trading_base": BINANCE_COPY_ROOT,
            "binance_public_market_data": BINANCE_ROOT,
            "public_page_json_documented": False, "browser_token_required": False
        },
        "price_feed": {
            **PAPER_PRICE_STREAM_STATE,
            "symbol_last_update": dict(PAPER_SYMBOL_LAST_UPDATE),
            "binance_rest": binance_rest_status(),
            "rest_policy": "WebSocket-first; at most one bulk premiumIndex fallback per minute; no per-symbol mark-price polling.",
            "paper_exit_policy": {
                "policy": "MAE_IS_NOT_AN_EXIT; MEANINGFUL_MFE_HARVEST_AND_REENTER_WHILE_EXACT_LEADER_SETUP_REMAINS_VALID",
                "leader_reversal_exit_enabled": bool(paper_config().get("leader_reversal_exit_enabled", True)),
                "recovery_max_hold_hours": paper_config().get("recovery_max_hold_hours", 72.0),
                "momentum_max_hold_hours": paper_config().get("momentum_max_hold_hours", 24.0),
                "structure_lookback_minutes": paper_config().get("structure_lookback_minutes", 60),
                "structure_break_buffer_pct": paper_config().get("structure_break_buffer_pct", 0.20),
                "structure_break_confirmations": paper_config().get("structure_break_confirmations", 3)
            }
        },
        "runtime": {
            "python_env": os.getenv("PYTHON_VERSION", "Render default"),
            "mode": os.getenv("LAES_MODE", "paper"),
            "execution_enabled": False,
            "render_free_sleep_warning": "Render Free web services sleep after 15 minutes without inbound traffic; the in-process monitor pauses while sleeping."
        }
    }

@app.post("/api/sync/now")
async def sync_now():
    global last_manual_sync
    if time.time() - last_manual_sync < 300:
        raise HTTPException(429, "A monitor cycle ran recently. The manual trigger is limited to one call per five minutes; all Binance HTTP requests also share the global weight limiter.")
    return await run_monitor_cycle(force=True)

@app.get("/api/news")
async def api_news(symbol: str = "", refresh: bool = False):
    items = await fetch_news_items(force=refresh)
    fetched_at = datetime.fromtimestamp(news_cache["fetched_at"], timezone.utc).isoformat() if news_cache.get("fetched_at") else None
    return {"source": "CoinDesk / Cointelegraph / The Block RSS", "items": find_news_for_symbol(symbol, items) if symbol else items[:40], "fetched_at": fetched_at, "feed_error": news_cache.get("error"), "warning": "Headline timing is context only; LAES does not claim a headline caused a particular price move."}

IMPORTANT_MARKET_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    "LTCUSDT","BCHUSDT","UNIUSDT","AAVEUSDT","SUIUSDT","TRXUSDT","TONUSDT","NEARUSDT","APTUSDT","ARBUSDT",
    "OPUSDT","FILUSDT","ATOMUSDT","ETCUSDT","XLMUSDT","HBARUSDT","ICPUSDT","INJUSDT","SEIUSDT","TIAUSDT",
    "PEPEUSDT","SHIBUSDT","WIFUSDT","BONKUSDT","FLOKIUSDT","ENAUSDT","WLDUSDT","TAOUSDT","RENDERUSDT","MKRUSDT",
    "CRVUSDT","JUPUSDT","JTOUSDT","PYTHUSDT","RUNEUSDT","FETUSDT","GRTUSDT","SANDUSDT","MANAUSDT","ALGOUSDT",
    "EOSUSDT","NEOUSDT","VETUSDT","QNTUSDT","EGLDUSDT","RAYUSDT","CAKEUSDT","ORDIUSDT","1000SHIBUSDT"
]

@app.get("/api/market/universe")
async def api_market_universe():
    symbols = set(IMPORTANT_MARKET_SYMBOLS)
    with SessionLocal() as db:
        symbols.update(str(x).upper() for x in db.scalars(select(MarketInsight.symbol)).all())
    rows = []
    with SessionLocal() as db:
        caches = {x.symbol: parse_json(x.raw_json) for x in db.scalars(select(MarketInsight)).all()}
    for symbol in sorted(symbols):
        cached = caches.get(symbol, {})
        live_at = PAPER_SYMBOL_LAST_PRICE_AT.get(symbol)
        live_fresh = bool(live_at and time.time() - live_at <= 10)
        price = PAPER_SYMBOL_LAST_PRICE.get(symbol) if live_fresh else safe_float(cached.get("mark_price"))
        funding = PAPER_SYMBOL_FUNDING_RATE.get(symbol) if live_fresh else safe_float(cached.get("funding_rate"))
        momentum = safe_float(cached.get("momentum_5m_pct"))
        history = PAPER_SYMBOL_PRICE_HISTORY.get(symbol, [])
        cutoff = time.time() - 300
        baseline = next((p for stamp,p in history if stamp <= cutoff), None)
        if live_fresh and baseline and baseline > 0 and price and price > 0:
            momentum = (price / baseline - 1) * 100
        rows.append({
            "symbol": symbol, "mark_price": price, "funding_rate": funding,
            "momentum_5m_pct": momentum,
            "quote_volume_24h_usdt": safe_float(cached.get("quote_volume_24h_usdt")),
            "live_fresh": live_fresh, "live_age_seconds": round(time.time()-live_at,1) if live_at else None,
            "status": "LIVE" if live_fresh else ("CACHED" if cached else "NO_DATA")
        })
    return {"source": "Binance USD-M public market data", "count": len(rows), "rows": rows}

@app.get("/api/market/{symbol}")
async def api_market(symbol: str, refresh: bool = False):
    symbol = symbol.upper()
    if not symbol.isalnum():
        raise HTTPException(400, "Invalid symbol")
    try:
        return await market_features(symbol, refresh=refresh)
    except Exception as exc:
        raise HTTPException(502, f"Market analysis unavailable: {str(exc)[:400]}")

@app.get("/api/status")
async def api_status():
    return await api_overview()
