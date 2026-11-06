from __future__ import annotations

import asyncio
import json
import math
import os
import re
import statistics
import time
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import websockets
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import (
    create_engine, String, DateTime, Boolean, Integer, BigInteger, Float, Text,
    ForeignKey, UniqueConstraint, select, desc, func, text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

APP_NAME = "LAES · Leader-Anchored Adverse Excursion Scalping"
KOPYON_KEY = os.getenv("KOPYON_API_KEY", "").strip()
KOPYON_ROOT = os.getenv("KOPYON_BASE_URL", "https://api.kopyon.com/v5").rstrip("/")
if not KOPYON_ROOT.endswith("/binance-copy-trading"):
    KOPYON_ROOT += "/binance-copy-trading"
BINANCE_ROOT = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
DISCOVERY_INTERVAL = max(900, int(os.getenv("LAES_DISCOVERY_INTERVAL_SECONDS", "1800")))
POSITION_BATCH = max(1, min(20, int(os.getenv("LAES_POSITION_BATCH", "20"))))
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
    connect_args={"check_same_thread": False} if SQLITE else {}
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

class PaperTrade(Base):
    __tablename__ = "paper_trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_key: Mapped[str] = mapped_column(String(420), index=True)
    trader_id: Mapped[str] = mapped_column(String(160), index=True)
    trader_name: Mapped[str | None] = mapped_column(String(240))
    symbol: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(12))
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

Base.metadata.create_all(engine)

# Provider timestamps are Unix milliseconds (13 digits); migrate legacy 32-bit columns.
if engine.dialect.name == "postgresql":
    with engine.begin() as migration:
        migration.execute(text("ALTER TABLE history_positions ALTER COLUMN opened_at_ms TYPE BIGINT"))
        migration.execute(text("ALTER TABLE history_positions ALTER COLUMN closed_at_ms TYPE BIGINT"))

last_scan: dict[str, Any] = {"state": "starting", "started_at": None, "finished_at": None, "last_error": None, "traders_found": 0, "positions_checked": 0, "observations_saved": 0}
worker_alive = False
scan_lock = asyncio.Lock()
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
PAPER_CANDLE_CACHE: dict[str, dict[str, Any]] = {}
templates = Jinja2Templates(directory="templates")

def now() -> datetime:
    return datetime.now(timezone.utc)

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

def paper_config() -> dict:
    defaults = {
        "capital_usdt": 1000.0,
        "auto_enabled": True,
        "risk_pct": 0.50,
        "max_margin_pct": 10.0,
        "max_total_margin_pct": 100.0,
        "research_unconstrained": True,
        "shadow_max_total_margin_pct": 40.0,
        "shadow_max_positions_per_symbol_direction": 1,
        "path_sample_seconds": 5.0,
        "meaningful_profit_steps": [0.10, 0.20, 0.30, 0.50, 0.70, 1.00],
        "target_pct": 0.70,
        "stop_pct": 0.45,
        "taker_fee_pct": 0.05,
        "slippage_pct": 0.02,
        "max_leverage": 3.0,
        "min_adverse_pct": 0.30,
        "leader_continuation_enabled": True,
        "leader_continuation_hard_loss_price_pct": 0.60,
        "leader_continuation_max_minutes": 30,
        "leader_reversal_exit_enabled": True,
        "post_exit_recovery_audit_hours": 6.0
    }
    saved = setting("paper_config", {})
    return {**defaults, **(saved if isinstance(saved, dict) else {})}

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

async def request_json(url: str, params: dict | None = None, headers: dict | None = None, timeout: float = 20) -> tuple[Any, int]:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url, params=params or {}, headers=headers or {})
    if response.status_code >= 400:
        message = response.text[:350].replace(KOPYON_KEY, "[redacted]") if KOPYON_KEY else response.text[:350]
        raise RuntimeError(f"HTTP {response.status_code}: {message}")
    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError("Provider returned a non-JSON response") from exc
    # Preserve JSON arrays: Binance /klines returns a list, not an object.
    return payload, response.status_code

async def kopyon_get(route: str, params: dict | None = None) -> tuple[dict, int]:
    if not KOPYON_KEY:
        raise RuntimeError("KOPYON_API_KEY is not configured on this Render service")
    started = time.perf_counter()
    payload, status = await request_json(
        f"{KOPYON_ROOT}/{route}",
        params=params,
        headers={"X-API-KEY": KOPYON_KEY},
        timeout=25
    )
    log_check("kopyon", True, status, int((time.perf_counter() - started) * 1000), f"{route} responded")
    return payload, status

def _binance_rate_limit_cooldown(error: Exception) -> None:
    global BINANCE_REST_BLOCKED_UNTIL, BINANCE_REST_BLOCK_REASON, BINANCE_REST_LAST_LIMIT_LOG_AT
    message = str(error)
    match = re.search(r"HTTP\s+(418|429)", message)
    if not match:
        return
    status = int(match.group(1))
    current = time.time()
    until = None
    ban_match = re.search(r"banned until\s+(\d{13})", message, re.IGNORECASE)
    if ban_match:
        until = int(ban_match.group(1)) / 1000.0
    if until is None or until <= current:
        until = current + (3600 if status == 418 else 60)
    if until > BINANCE_REST_BLOCKED_UNTIL:
        BINANCE_REST_BLOCKED_UNTIL = until
        BINANCE_REST_BLOCK_REASON = f"Binance HTTP {status}; REST calls are paused to prevent another IP ban."
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
        "reason": BINANCE_REST_BLOCK_REASON if remaining else None
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


async def binance_get(path: str, params: dict | None = None) -> tuple[Any, int]:
    if time.time() < BINANCE_REST_BLOCKED_UNTIL:
        remaining = max(0.0, BINANCE_REST_BLOCKED_UNTIL - time.time())
        raise RuntimeError(f"Binance REST circuit breaker active for {remaining:.0f}s; waiting for WebSocket feed.")
    started = time.perf_counter()
    try:
        payload, status = await request_json(f"{BINANCE_ROOT}{path}", params=params, timeout=18)
    except Exception as exc:
        _binance_rate_limit_cooldown(exc)
        raise
    log_check("binance_market_data", True, status, int((time.perf_counter() - started) * 1000), path)
    return payload, status

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
                            detail="Exact leader position absent from two consecutive visible Kopyon snapshots"
                        ))
                paper.raw_json = json.dumps(raw_trade, separators=(",", ":"), default=str)
        db.commit()
    # A hidden or empty response is not evidence of a close; old observations remain intact.
    return saved, hidden

async def discover_traders() -> tuple[int, list[str]]:
    payload, _ = await kopyon_get("traders", {
        "period": "7D",
        "sortBy": "copierPnl",
        "order": "DESC",
        "type": "PUBLIC",
        "page": 1,
        "rows": 20
    })
    data = unwrap_data(payload)
    rows = data.get("traders") or data.get("rows") or data.get("items") or []
    if not isinstance(rows, list):
        rows = []
    saved = persist_traders(rows)
    ids = [str(row.get("traderId") or row.get("portfolioId") or row.get("id") or "") for row in rows]
    return saved, [item for item in ids if item]

async def sync_one_trader_positions(trader_id: str) -> tuple[int, bool]:
    payload, _ = await kopyon_get("openPositions", {"traderId": trader_id})
    return record_position_snapshot(trader_id, payload)

async def sync_trader_profile_if_stale(trader_id: str, max_age_hours: int = 24) -> bool:
    with SessionLocal() as db:
        cache = db.get(TraderProfileCache, trader_id)
        stale = cache is None or (now() - cache.fetched_at).total_seconds() >= max_age_hours * 3600
    if not stale:
        return False
    await load_trader_intelligence(trader_id)
    return True

async def run_monitor_cycle(force: bool = False) -> dict:
    global last_manual_sync
    async with scan_lock:
        last_scan.update({"state": "running", "started_at": now().isoformat(), "last_error": None})
        try:
            if not KOPYON_KEY:
                raise RuntimeError("KOPYON_API_KEY is missing from this service's environment")
            trader_count, discovered_ids = await discover_traders()
            with SessionLocal() as db:
                traders = db.scalars(
                    select(Trader).order_by(desc(Trader.copier_pnl), desc(Trader.last_seen_at)).limit(100)
                ).all()
                ids = [t.trader_id for t in traders]
                cursor = int(setting("position_rotation_cursor", 0) or 0)
                if ids:
                    selected = [ids[(cursor + i) % len(ids)] for i in range(min(POSITION_BATCH, len(ids)))]
                    set_setting("position_rotation_cursor", (cursor + len(selected)) % len(ids))
                else:
                    selected = discovered_ids[:POSITION_BATCH]
            checked = 0
            saved = 0
            profiles_loaded = 0
            for trader_id in selected:
                try:
                    count, hidden = await sync_one_trader_positions(trader_id)
                    checked += 1
                    saved += count
                    log_check("position_snapshot", True, 200, None, f"{trader_id}: hidden={hidden}, observed={count}")
                except Exception as exc:
                    log_check("position_snapshot", False, None, None, f"{trader_id}: {str(exc)[:700]}")
                try:
                    if await sync_trader_profile_if_stale(trader_id, max_age_hours=24):
                        profiles_loaded += 1
                        log_check("trader_profile", True, 200, None, f"{trader_id}: profile/performance/history refreshed")
                except Exception as exc:
                    log_check("trader_profile", False, None, None, f"{trader_id}: {str(exc)[:700]}")
            try:
                await api_opportunities(limit=100)
            except Exception as analysis_exc:
                log_check("paper_analysis", False, None, None, str(analysis_exc))
            await evaluate_open_paper_trades()
            last_scan.update({
                "state": "idle",
                "finished_at": now().isoformat(),
                "last_error": None,
                "traders_found": trader_count,
                "positions_checked": checked,
                "observations_saved": saved,
                "profiles_loaded": profiles_loaded,
                "selected_trader_ids": selected
            })
            last_manual_sync = time.time()
            return dict(last_scan)
        except Exception as exc:
            last_scan.update({"state": "error", "finished_at": now().isoformat(), "last_error": str(exc)[:900]})
            log_check("monitor", False, None, None, str(exc))
            return dict(last_scan)

async def monitor_worker() -> None:
    global worker_alive
    worker_alive = True
    try:
        # First discovery starts automatically at process startup.
        await run_monitor_cycle()
        while True:
            await asyncio.sleep(DISCOVERY_INTERVAL)
            await run_monitor_cycle()
    except asyncio.CancelledError:
        raise
    finally:
        worker_alive = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await run_post_exit_recovery_audit()
    except Exception as exc:
        log_check("post_exit_recovery_audit_startup", False, None, None, str(exc)[:900])
    task = asyncio.create_task(monitor_worker())
    price_task = asyncio.create_task(paper_price_worker())
    app.state.monitor_task = task
    app.state.paper_price_task = price_task
    yield
    task.cancel()
    price_task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    with suppress(asyncio.CancelledError):
        await price_task

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

def overview_counts() -> dict:
    with SessionLocal() as db:
        traders = db.scalar(select(func.count()).select_from(Trader)) or 0
        snapshots = db.scalar(select(func.count()).select_from(PositionSnapshot)) or 0
        observations = db.scalar(select(func.count()).select_from(PositionObservation)) or 0
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).group_by(PositionSnapshot.trader_id).subquery()
        latest_rows = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
        latest_visible_ids = [row.id for row in latest_rows if not row.hidden]
        latest_hidden = sum(1 for row in latest_rows if row.hidden)
        live_rows = db.scalars(select(PositionObservation).where(PositionObservation.snapshot_id.in_(latest_visible_ids))).all() if latest_visible_ids else []
        live_positions = len(live_rows)
        underwater_live = sum(1 for row in live_rows if row.unrealized_pnl is not None and row.unrealized_pnl < 0)
        open_paper = db.scalar(select(func.count()).select_from(PaperTrade).where(PaperTrade.status == "OPEN")) or 0
        closed_paper = db.scalar(select(func.count()).select_from(PaperTrade).where(PaperTrade.status == "CLOSED")) or 0
        latest = db.scalar(select(func.max(PositionSnapshot.observed_at)))
    return {
        "traders": traders, "snapshots": snapshots, "observations": observations,
        "live_positions": live_positions, "underwater_live_positions": underwater_live,
        "latest_trader_snapshots_hidden": latest_hidden, "hidden_snapshots": latest_hidden,
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
        "kopyon_key_configured": bool(KOPYON_KEY),
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
    return {
        **counts,
        "database": db,
        "kopyon": {"configured": bool(KOPYON_KEY), "base": KOPYON_ROOT, "last_status": next(({"ok": c.ok, "detail": c.detail, "at": c.checked_at.isoformat()} for c in checks if c.source == "kopyon"), None)},
        "binance": {"base": BINANCE_ROOT, "role": "Public USDⓈ-M Futures candles and mark/funding data; not individual leader positions."},
        "monitor": {**last_scan, "worker_alive": worker_alive, "interval_seconds": DISCOVERY_INTERVAL, "position_batch_per_cycle": POSITION_BATCH, "quota_design": "One 20-trader leaderboard request plus up to twenty open-position requests per 30-minute cycle; profile/performance/history refreshes are limited to once per trader per 24 hours; market-data calls are separate."},
        "paper": paper_config(),
        "paper_fast_price_monitor": True,
        "execution_enabled": False,
        "runtime_note": "On Render Free, the web service can spin down after 15 minutes without inbound traffic. The in-process monitor runs only while the instance is awake."
    }

@app.get("/api/traders")
async def api_traders(limit: int = 100, search: str = ""):
    limit = max(1, min(limit, 300))
    with SessionLocal() as db:
        query = select(Trader).order_by(desc(Trader.copier_pnl), desc(Trader.last_seen_at))
        rows = db.scalars(query.limit(limit)).all()
        cache = {p.trader_id: p for p in db.scalars(select(TraderProfileCache)).all()}
    result = []
    q = search.lower().strip()
    for t in rows:
        if q and q not in (t.name or "").lower() and q not in t.trader_id.lower():
            continue
        profile_cache = cache.get(t.trader_id)
        profile = unwrap_data(parse_json(profile_cache.raw_profile)) if profile_cache else {}
        src = parse_json(t.raw_json)
        result.append({
            "trader_id": t.trader_id, "name": t.name, "avatar_url": t.avatar_url,
            "portfolio_type": t.portfolio_type, "visibility": t.visibility,
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
        cache = db.get(TraderProfileCache, trader_id)
        history = db.scalars(select(HistoryPosition).where(HistoryPosition.trader_id == trader_id).order_by(desc(HistoryPosition.opened_at_ms)).limit(100)).all()
    if trader is None:
        raise HTTPException(404, "Trader not found in the live discovered universe")
    durations = [((h.closed_at_ms - h.opened_at_ms) / 3600000) for h in history if h.opened_at_ms and h.closed_at_ms and h.closed_at_ms > h.opened_at_ms]
    pnl_values = [h.net_pnl for h in history if h.net_pnl is not None]
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
    if trader is None:
        raise HTTPException(404, "Trader not found")
    if not KOPYON_KEY:
        raise HTTPException(503, "KOPYON_API_KEY is missing from this service's environment")
    results = {}
    try:
        profile_payload, _ = await kopyon_get("traderProfile", {"traderId": trader_id})
        performance_payload, _ = await kopyon_get("performance", {"traderId": trader_id, "period": "30D"})
        history_payload, _ = await kopyon_get("positionHistory", {"traderId": trader_id, "rows": 20, "sortBy": "closed"})
        profile = unwrap_data(profile_payload)
        performance = unwrap_data(performance_payload)
        history_data = unwrap_data(history_payload)
        history_rows = history_data.get("positions") or history_data.get("rows") or history_data.get("items") or []
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
                    opened_at_ms=timestamp_ms(item.get("openedAt") or item.get("openTime")),
                    closed_at_ms=timestamp_ms(item.get("closedAt") or item.get("closeTime")),
                    entry_price=safe_float(item.get("entryPrice")),
                    close_price=safe_float(item.get("closePrice")),
                    roi=safe_float(item.get("roi")),
                    gross_pnl=safe_float(item.get("grossPnl")),
                    net_pnl=safe_float(item.get("netPnl")),
                    raw_json=json.dumps(item, separators=(",", ":"), default=str)
                ))
            db.commit()
        results = {"profile_loaded": True, "history_rows_received": len(history_rows), "history_total": history_data.get("total"), "performance_loaded": True}
    except Exception as exc:
        raise HTTPException(502, f"Could not load trader intelligence: {str(exc)[:700]}")
    return results

@app.get("/api/positions")
async def api_positions(limit: int = 500):
    limit = max(1, min(limit, 1000))
    with SessionLocal() as db:
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).group_by(PositionSnapshot.trader_id).subquery()
        latest_snapshots = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
        visible = {snap.id: snap for snap in latest_snapshots if not snap.hidden}
        observations = db.scalars(select(PositionObservation).where(
            PositionObservation.snapshot_id.in_(list(visible.keys()))
        ).order_by(desc(PositionObservation.id)).limit(limit * 3)).all() if visible else []
    latest = {}
    current_time = now()
    for obs in observations:
        key = f"{obs.trader_id}|{obs.position_key}"
        if key not in latest:
            snap = visible.get(obs.snapshot_id)
            age = max(0.0, (current_time - snap.observed_at).total_seconds()) if snap else None
            latest[key] = {
                "trader_id": obs.trader_id, "symbol": obs.symbol, "side": obs.side, "position_key": obs.position_key,
                "size": obs.size, "entry_price": obs.entry_price, "mark_price": obs.mark_price,
                "unrealized_pnl": obs.unrealized_pnl, "leverage": obs.leverage, "margin_mode": obs.margin_mode,
                "liquidation_price": obs.liquidation_price, "roi": obs.roi,
                "observed_at": snap.observed_at.isoformat() if snap else None,
                "source_fetched_at": snap.source_fetched_at if snap else None,
                "snapshot_age_seconds": round(age, 1) if age is not None else None,
                "freshness": "FRESH" if age is not None and age <= 900 else "STALE",
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

async def market_features(symbol: str, refresh: bool = False) -> dict:
    with SessionLocal() as db:
        cache = db.get(MarketInsight, symbol)
        if cache and not refresh and (now() - cache.fetched_at).total_seconds() < 1800:
            cached = parse_json(cache.raw_json)
            cached_closes = cached.get("closes_1h") if isinstance(cached, dict) else None
            # Re-fetch older cache rows created before list-valued Binance klines were parsed correctly.
            if isinstance(cached_closes, list) and len(cached_closes) >= 25 and cached.get("quote_volume_24h_usdt") is not None and cached.get("spread_pct") is not None and cached.get("min_depth_within_1pct_usdt") is not None and cached.get("volume_ratio_recent_6h_vs_prior_24h") is not None:
                return cached
    premium, _ = await binance_get("/fapi/v1/premiumIndex", {"symbol": symbol})
    ticker24, _ = await binance_get("/fapi/v1/ticker/24hr", {"symbol": symbol})
    book, _ = await binance_get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
    depth, _ = await binance_get("/fapi/v1/depth", {"symbol": symbol, "limit": 20})
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
        "seven_day_return_pct": seven_return, "twenty_four_hour_return_pct": day_return,
        "atr_24h_pct": atr_pct, "rsi_1h": rsi_value(closes),
        "momentum_5m_pct": mom5, "closes_1h": closes[-30:],
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

async def build_opportunity(obs: PositionObservation, snapshot_time: str | None, trader_name: str | None, news: list[dict]) -> dict:
    raw = parse_json(obs.raw_json)
    entry = obs.entry_price
    try:
        market = await market_features(obs.symbol)
    except Exception as exc:
        market = {"symbol": obs.symbol, "error": str(exc)[:220], "source": "Binance market data unavailable"}
    mark = safe_float(market.get("mark_price")) or obs.mark_price
    adverse = None
    if entry and mark and entry > 0:
        adverse = ((entry - mark) / entry * 100) if obs.side == "LONG" else ((mark - entry) / entry * 100)
    underwater = (obs.unrealized_pnl is not None and obs.unrealized_pnl < 0) or (adverse is not None and adverse > 0)
    cfg = paper_config()
    momentum = safe_float(market.get("momentum_5m_pct"))
    confirmation = momentum is not None and ((momentum > 0) if obs.side == "LONG" else (momentum < 0))
    min_adverse = float(cfg["min_adverse_pct"])
    quote_volume = safe_float(market.get("quote_volume_24h_usdt"))
    spread_pct = safe_float(market.get("spread_pct"))
    funding_rate = safe_float(market.get("funding_rate"))
    atr_quality = safe_float(market.get("atr_24h_pct"))
    dead_market_reasons = []
    if quote_volume is None or quote_volume < 5_000_000:
        dead_market_reasons.append("24h quote volume below $5M or unavailable")
    if spread_pct is None or spread_pct > 0.10:
        dead_market_reasons.append("spread above 0.10% or unavailable")
    depth_usdt = safe_float(market.get("min_depth_within_1pct_usdt"))
    if depth_usdt is None or depth_usdt < 10000:
        dead_market_reasons.append("top-20 order-book depth within 1% below $10K or unavailable")
    volume_ratio = safe_float(market.get("volume_ratio_recent_6h_vs_prior_24h"))
    if volume_ratio is None or volume_ratio < 0.30:
        dead_market_reasons.append("recent 6h volume below 30% of prior 24h hourly baseline or unavailable")
    if atr_quality is None or atr_quality < 0.02 or atr_quality > 8.0:
        dead_market_reasons.append("volatility too low or disorderly")
    if funding_rate is not None and ((obs.side == "LONG" and funding_rate > 0.0005) or (obs.side == "SHORT" and funding_rate < -0.0005)):
        dead_market_reasons.append("funding cost is unusually adverse for this side")
    market_quality_ok = not dead_market_reasons
    qualified = bool(underwater and adverse is not None and adverse >= min_adverse and confirmation and market_quality_ok)
    status = "QUALIFIED" if qualified else ("DEAD / COSTLY MARKET" if underwater and not market_quality_ok else ("UNDERWATER WATCH" if underwater else "NOT UNDERWATER"))
    with SessionLocal() as db:
        latest_leader_snapshot = db.scalar(select(PositionSnapshot).where(
            PositionSnapshot.trader_id == obs.trader_id
        ).order_by(desc(PositionSnapshot.id)).limit(1))
    leader_snapshot_current = bool(latest_leader_snapshot and not latest_leader_snapshot.hidden and latest_leader_snapshot.id == obs.snapshot_id)
    snapshot_age_seconds = max(0.0, (now() - latest_leader_snapshot.observed_at).total_seconds()) if latest_leader_snapshot else None
    leader_snapshot_fresh = bool(leader_snapshot_current and snapshot_age_seconds is not None and snapshot_age_seconds <= 900)
    if not leader_snapshot_current:
        qualified = False
        status = "LEADER POSITIONS HIDDEN" if latest_leader_snapshot and latest_leader_snapshot.hidden else "POSITION NOT IN LATEST LEADER SNAPSHOT"
    elif not leader_snapshot_fresh:
        qualified = False
        status = "STALE LEADER SNAPSHOT"
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
    recovery = recovery_statistics(obs.trader_id, obs.position_key, obs.symbol, obs.side, entry)
    hold = holding_time(obs.trader_id, obs.position_key, raw, snapshot_time)
    if qualified and cfg.get("auto_enabled"):
        create_paper_trade(obs, trader_name, mark or entry or 0, cfg, market)
    return {
        "trader_id": obs.trader_id, "trader_name": trader_name or obs.trader_id,
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
        "recovery_warning": "Market trend/news are contextual risk factors, not a deterministic cause or guarantee of recovery."
    }

def create_paper_trade(obs: PositionObservation, trader_name: str | None, mark: float, cfg: dict, market: dict) -> None:
    if mark <= 0 or not obs.entry_price or obs.entry_price <= 0:
        return
    source_key = f"{obs.trader_id}|{obs.position_key}"
    with SessionLocal() as db:
        # Re-entry is allowed only against a fresh, visible Kopyon snapshot containing
        # this exact leader position. Hidden/empty/stale snapshots never authorize entry.
        latest_snapshot = db.scalar(select(PositionSnapshot).where(
            PositionSnapshot.trader_id == obs.trader_id
        ).order_by(desc(PositionSnapshot.id)).limit(1))
        if latest_snapshot is None or latest_snapshot.hidden or latest_snapshot.id != obs.snapshot_id:
            return
        existing = db.scalar(select(PaperTrade.id).where(PaperTrade.source_key == source_key, PaperTrade.status == "OPEN").limit(1))
        if existing:
            return
        prior = db.scalars(select(PaperTrade).where(PaperTrade.source_key == source_key).order_by(desc(PaperTrade.opened_at))).all()
        if len(prior) >= 5:
            return
        if prior:
            previous_raw = parse_json(prior[0].raw_json)
            if previous_raw.get("leader_snapshot_id") == obs.snapshot_id:
                return
            previous_close = prior[0].closed_at
            if previous_close and previous_close.tzinfo is None:
                previous_close = previous_close.replace(tzinfo=timezone.utc)
            if previous_close and (now() - previous_close).total_seconds() < 180:
                return
        opened_trades = db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN")).all()
        used_margin = sum(t.margin_used for t in opened_trades)
        realized = db.scalar(select(func.coalesce(func.sum(PaperTrade.net_pnl), 0)).where(PaperTrade.status == "CLOSED")) or 0
        capital = max(0.0, float(cfg["capital_usdt"]) + float(realized))
        free_capital = max(0.0, capital - used_margin)
        max_margin = capital * float(cfg["max_margin_pct"]) / 100
        max_total_margin = capital * float(cfg["max_total_margin_pct"]) / 100
        remaining_portfolio_margin = max(0.0, max_total_margin - used_margin)
        base_margin = capital * float(cfg["risk_pct"]) / 100 / max(float(cfg["stop_pct"]) / 100, 0.0001)
        # Research mode intentionally takes every qualifying opportunity. It does not
        # silently discard signals because earlier paper trades consumed the wallet.
        # The margin/notional of each individual simulation remains bounded.
        margin = min(base_margin, max_margin)
        if not bool(cfg.get("research_unconstrained", True)):
            margin = min(margin, free_capital, remaining_portfolio_margin)
        leverage = max(1.0, min(float(cfg["max_leverage"]), float(obs.leverage or 1.0)))
        notional = margin * leverage
        notional = min(notional, capital * 0.25)
        if notional < 5 or margin <= 0:
            return
        quantity = notional / mark
        stop_frac = float(cfg["stop_pct"]) / 100
        target_frac = float(cfg["target_pct"]) / 100
        if obs.side == "LONG":
            stop = mark * (1 - stop_frac)
            target = mark * (1 + target_frac)
        else:
            stop = mark * (1 + stop_frac)
            target = mark * (1 - target_frac)
        db.add(PaperTrade(
            source_key=source_key, trader_id=obs.trader_id, trader_name=trader_name,
            symbol=obs.symbol, side=obs.side, status="OPEN",
            entry_price=mark, mark_price=mark, quantity=quantity, notional=notional,
            margin_used=margin, leverage=leverage, stop_price=stop, target_price=target,
            raw_json=json.dumps({
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
        ))
        db.commit()

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

def _leader_for_trade(db, trade: PaperTrade) -> PositionObservation | None:
    prefix = trade.source_key
    if "|" not in prefix:
        return None
    trader_id, position_key = prefix.split("|", 1)
    return db.scalar(select(PositionObservation).where(
        PositionObservation.trader_id == trader_id,
        PositionObservation.position_key == position_key
    ).order_by(desc(PositionObservation.id)).limit(1))

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
    # Estimate funding from the rate captured at entry. When history is not verified,
    # never assume a funding credit; charge only a conservative positive estimate.
    raw = raw or {}
    funding_rate = safe_float(raw.get("funding_rate_at_entry")) or 0.0
    next_funding_ms = safe_float(raw.get("next_funding_time"))
    funding_events = 0
    if at and next_funding_ms and funding_rate:
        elapsed_ms = at.timestamp() * 1000 - next_funding_ms
        if elapsed_ms >= 0:
            funding_events = 1 + int(elapsed_ms // 28800000)
    estimated_funding_cost = max(
        0.0,
        trade.notional * funding_rate * funding_events * directional
    )
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
    Determine leader state without treating missing/stale/hidden data as a close.
    Returns (state, detail):
      OPEN_OBSERVED, CLOSED_CONFIRMED, REVERSED, STALE_OR_UNAVAILABLE
    """
    if "|" not in trade.source_key:
        return "STALE_OR_UNAVAILABLE", "Trade has no leader position key"
    trader_id, position_key = trade.source_key.split("|", 1)
    latest = db.scalar(
        select(PositionSnapshot).where(PositionSnapshot.trader_id == trader_id)
        .order_by(desc(PositionSnapshot.id)).limit(1)
    )
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

    # A visible opposite-side position on the same symbol is a strong reversal
    # signal. We never infer reversal from an empty/hidden snapshot alone.
    symbol = trade.symbol.upper()
    opposite = "SHORT" if trade.side.upper() == "LONG" else "LONG"
    reversed_obs = db.scalar(select(PositionObservation).where(
        PositionObservation.snapshot_id == latest.id,
        PositionObservation.trader_id == trader_id,
        PositionObservation.symbol == symbol,
        PositionObservation.side == opposite
    ).limit(1))
    if reversed_obs is not None:
        return "REVERSED", f"Visible opposite-side leader position {reversed_obs.position_key}"

    raw = parse_json(trade.raw_json)
    missing = int(raw.get("leader_missing_visible_snapshots", 0) or 0)
    if missing >= 2:
        return "CLOSED_CONFIRMED", "Exact leader position absent from two consecutive visible snapshots"
    return "STALE_OR_UNAVAILABLE", "Leader position not present in the latest snapshot but closure is not yet confirmed"


async def run_post_exit_recovery_audit() -> dict:
    """
    One-time, read-only market-path audit for historical clean closed losses.
    It never rewrites realized PnL, exit price, status, or capital. It only stores
    Binance mark-price recovery metadata in raw_json and PaperTradeEvent.
    """
    cfg = paper_config()
    horizon_hours = max(1.0, min(24.0, float(cfg.get("post_exit_recovery_audit_hours", 24.0))))
    audited = recovered = 0
    failures = 0
    with SessionLocal() as db:
        trades = db.scalars(select(PaperTrade).where(
            PaperTrade.status == "CLOSED",
            PaperTrade.net_pnl < 0
        ).order_by(PaperTrade.closed_at)
        ).all()
    for trade in trades:
        raw = parse_json(trade.raw_json)
        if raw.get("post_exit_recovery_audit_version") == 3:
            continue
        # The post-exit market path is independently auditable even when the
        # pre-exit paper path was interrupted or incomplete. Keep the old
        # data-quality label, but do not skip this separate Binance audit.
        if not trade.closed_at or not trade.exit_price or not trade.entry_price:
            continue
        start_ms = int(trade.closed_at.timestamp() * 1000)
        end_ms = start_ms + int(horizon_hours * 3600 * 1000)
        try:
            rows, _ = await binance_get("/fapi/v1/markPriceKlines", {
                "symbol": trade.symbol,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1500
            })
            if not isinstance(rows, list) or not rows:
                raise RuntimeError("No Binance mark-price candles returned")
            directional = 1.0 if trade.side == "LONG" else -1.0
            best = None
            first_entry_cross = None
            first_net_profit = None
            worst = None
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) < 7:
                    continue
                high = safe_float(row[2]); low = safe_float(row[3]); at_ms = int(row[0])
                if high is None or low is None:
                    continue
                favorable = high if trade.side == "LONG" else low
                candidate_gross = directional * (favorable - trade.entry_price) * trade.quantity
                turnover = (trade.entry_price + favorable) * trade.quantity
                estimated_cost = turnover * ((float(cfg["taker_fee_pct"]) + float(cfg["slippage_pct"])) / 100.0)
                candidate_net = candidate_gross - estimated_cost
                if best is None or candidate_net > best["max_net_pnl"]:
                    best = {"max_net_pnl": candidate_net, "max_price": favorable, "at_ms": at_ms, "gross": candidate_gross, "cost": estimated_cost}
                adverse_price = low if trade.side == "LONG" else high
                adverse_gross = directional * (adverse_price - trade.entry_price) * trade.quantity
                adverse_cost = (trade.entry_price + adverse_price) * trade.quantity * ((float(cfg["taker_fee_pct"]) + float(cfg["slippage_pct"])) / 100.0)
                adverse_net = adverse_gross - adverse_cost
                if worst is None or adverse_net < worst["min_net_pnl"]:
                    worst = {"min_net_pnl": adverse_net, "price": adverse_price, "at_ms": at_ms}
                if candidate_net > 0 and first_net_profit is None:
                    first_net_profit = at_ms
                crossed = (high >= trade.entry_price) if trade.side == "LONG" else (low <= trade.entry_price)
                if crossed and first_entry_cross is None:
                    first_entry_cross = at_ms
            if best is None:
                continue
            recovered_to_profit = best["max_net_pnl"] > 0
            recovered_to_entry = first_entry_cross is not None
            raw.update({
                "post_exit_recovery_audit_version": 3,
                "post_exit_recovery_status": "RECOVERED_TO_NET_PROFIT" if recovered_to_profit else ("RECOVERED_TO_ENTRY_ONLY" if recovered_to_entry else "NO_ENTRY_RECOVERY"),
                "post_exit_recovery_market_source": "BINANCE_USDM_MARK_PRICE_1M",
                "post_exit_recovery_horizon_hours": horizon_hours,
                "post_exit_max_net_pnl": round(best["max_net_pnl"], 8),
                "post_exit_max_gross_pnl": round(best["gross"], 8),
                "post_exit_max_price": best["max_price"],
                "post_exit_max_at": datetime.fromtimestamp(best["at_ms"]/1000, timezone.utc).isoformat(),
                "post_exit_return_to_entry_at": datetime.fromtimestamp(first_entry_cross/1000, timezone.utc).isoformat() if first_entry_cross else None,
                "post_exit_minutes_to_entry": round((first_entry_cross - start_ms) / 60000, 2) if first_entry_cross else None,
                "post_exit_first_net_profit_at": datetime.fromtimestamp(first_net_profit/1000, timezone.utc).isoformat() if first_net_profit else None,
                "post_exit_minutes_to_net_profit": round((first_net_profit - start_ms) / 60000, 2) if first_net_profit else None,
                "post_exit_minutes_to_best": round((best["at_ms"] - start_ms) / 60000, 2),
                "post_exit_observed_minutes": round((int(rows[-1][0]) - start_ms) / 60000, 2) if rows else 0,
                "post_exit_worst_net_pnl": round(worst["min_net_pnl"], 8) if worst else None,
                "post_exit_worst_at": datetime.fromtimestamp(worst["at_ms"]/1000, timezone.utc).isoformat() if worst else None,
                "post_exit_recovery_profit_usdt": round(max(0.0, best["max_net_pnl"]), 8),
                "post_exit_old_exit_reason": trade.exit_reason,
                "post_exit_recovery_label": (
                    f"[AFTER EXIT: NET PROFIT +{best['max_net_pnl']:.4f} USDT · +{(first_net_profit-start_ms)/60000:.0f}m | best +{(best['at_ms']-start_ms)/60000:.0f}m | old: {trade.exit_reason}]"
                    if recovered_to_profit else
                    f"[AFTER EXIT: ENTRY RECOVERED +{(first_entry_cross-start_ms)/60000:.0f}m BUT NOT NET PROFIT | old: {trade.exit_reason}]"
                    if recovered_to_entry else
                    f"[AFTER EXIT: NO ENTRY/NET RECOVERY IN {horizon_hours:.0f}H | old: {trade.exit_reason}]"
                )
            })
            with SessionLocal() as db:
                live = db.get(PaperTrade, trade.id)
                if live:
                    live.raw_json = json.dumps(raw, separators=(",", ":"), default=str)
                    exists = db.scalar(select(PaperTradeEvent.id).where(
                        PaperTradeEvent.paper_trade_id == trade.id,
                        PaperTradeEvent.event_type == "POST_EXIT_RECOVERY_AUDIT_V3"
                    ).limit(1))
                    if not exists:
                        db.add(PaperTradeEvent(
                            paper_trade_id=trade.id,
                            event_type="POST_EXIT_RECOVERY_AUDIT_V3",
                            observed_at=live.closed_at or now(),
                            value=best["max_net_pnl"],
                            detail=raw["post_exit_recovery_label"]
                        ))
                    db.commit()
            audited += 1
            recovered += 1 if recovered_to_profit else 0
        except Exception as exc:
            failures += 1
            log_check("post_exit_recovery_audit", False, None, None, f"trade={trade.id} {trade.symbol}: {str(exc)[:500]}")
    return {"audited": audited, "recovered_to_profit": recovered, "failures": failures}


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
            should_sample = last_path is None or (ts-last_path.observed_at).total_seconds() >= float(cfg.get("path_sample_seconds",5))
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
            hit_stop = mark <= trade.stop_price if trade.side == "LONG" else mark >= trade.stop_price
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
                or ((net > 0 or hit_stop or hit_target) and (not funding_verified) and (seconds_since_funding_check is None or seconds_since_funding_check >= 60))
                or (net > 0 and funding_verified and seconds_since_funding_check is not None and seconds_since_funding_check >= 300)
            )
            if check_due:
                try:
                    actual_funding, funding_rows = await _actual_funding_cost(trade, ts)
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
                    db.add(PaperTradeEvent(paper_trade_id=trade.id,event_type="FUNDING_CHECK_PENDING",observed_at=ts,value=net,detail="Binance funding history unavailable; positive exits wait for verification, while a protective stop may still close using a conservative estimate."))
            if net > 0 and funding_verified:
                # No minimum profit threshold: every observed positive net amount,
                # after round-trip costs and verified funding, is realized immediately.
                exit_reason = "PROFIT_HARVEST_NET_POSITIVE"
            elif net <= 0 and not gap_guard and hit_stop:
                # Leader-aware continuation: if the exact leader position is still
                # visibly open, do not let our independent paper stop erase the
                # leader-follow opportunity. The leader itself remains the primary
                # close signal. A hard adverse-price ceiling remains in force so this
                # cannot become an unbounded "hold and hope" rule.
                directional = 1.0 if trade.side == "LONG" else -1.0
                adverse_from_entry_pct = directional * (trade.entry_price - mark) / max(trade.entry_price, 1e-12) * 100.0
                hard_loss_pct = float(cfg.get("leader_continuation_hard_loss_price_pct", 0.60))
                if leader_state == "REVERSED" and bool(cfg.get("leader_reversal_exit_enabled", True)):
                    exit_reason = "LEADER_POSITION_REVERSED"
                elif leader_state == "CLOSED_CONFIRMED":
                    exit_reason = "LEADER_POSITION_CLOSED"
                elif (
                    leader_state == "OPEN_OBSERVED"
                    and bool(cfg.get("leader_continuation_enabled", True))
                    and adverse_from_entry_pct < hard_loss_pct
                ):
                    raw["leader_continuation_active"] = True
                    raw["leader_continuation_started_at"] = raw.get("leader_continuation_started_at") or ts.isoformat()
                    raw["leader_continuation_last_checked_at"] = ts.isoformat()
                    raw["leader_continuation_adverse_pct"] = adverse_from_entry_pct
                    if not raw.get("leader_continuation_hold_logged"):
                        db.add(PaperTradeEvent(
                            paper_trade_id=trade.id,
                            event_type="LEADER_CONTINUATION_HOLD",
                            observed_at=ts,
                            value=net,
                            detail=(
                                f"Stop touched while exact leader position remained visibly OPEN. "
                                f"Paper stop deferred; hard adverse-price ceiling={hard_loss_pct:.2f}%."
                            )
                        ))
                        raw["leader_continuation_hold_logged"] = True
                    exit_reason = None
                else:
                    if leader_state == "OPEN_OBSERVED":
                        raw["leader_continuation_hard_stop_triggered"] = True
                        db.add(PaperTradeEvent(
                            paper_trade_id=trade.id,
                            event_type="LEADER_CONTINUATION_HARD_STOP",
                            observed_at=ts,
                            value=net,
                            detail=f"Leader remained open but adverse price reached {adverse_from_entry_pct:.3f}% >= hard ceiling {hard_loss_pct:.3f}%."
                        ))
                    # A stale/unknown leader state never gets converted into a fake
                    # leader close. The existing protective stop remains the fallback.
                    if funding_verified:
                        exit_reason = "STOP_LOSS"
                    else:
                        exit_reason = "STOP_LOSS_FUNDING_ESTIMATED"
                        raw["funding_audit_status"] = "STOP_EXIT_USING_CONSERVATIVE_ESTIMATE"
                        funding_event_count_at_exit = 0
                        if next_funding_ms and ts.timestamp() * 1000 >= next_funding_ms:
                            funding_event_count_at_exit = 1 + int(
                                (ts.timestamp() * 1000 - next_funding_ms) // 28800000
                            )
                        raw["funding_estimate_at_exit"] = max(
                            0.0,
                            trade.notional
                            * (safe_float(raw.get("funding_rate_at_entry")) or 0.0)
                            * funding_event_count_at_exit
                            * (1 if trade.side == "LONG" else -1)
                        )
                        raw["realized_pnl_is_estimate"] = True
            elif net <= 0 and not gap_guard and hit_target:
                # A price target is not a profit exit if fees/funding still leave
                # net PnL at or below zero. Keep tracking until net turns positive
                # or the protective stop is reached.
                if not raw.get("net_negative_target_touch_logged"):
                    db.add(PaperTradeEvent(
                        paper_trade_id=trade.id,
                        event_type="TARGET_TOUCHED_NET_NOT_POSITIVE",
                        observed_at=ts,
                        value=net,
                        detail="Price target touched, but net PnL after estimated/verified costs was not positive; position remains open."
                    ))
                    raw["net_negative_target_touch_logged"] = True
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
        return sorted({
            str(trade.symbol).upper()
            for trade in db.scalars(select(PaperTrade).where(PaperTrade.status == "OPEN")).all()
            if trade.symbol
        })


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
    - /ws + per-symbol SUBSCRIBE prevents one invalid futures symbol poisoning a combined URL.
    - Price frames are batched and evaluated together, not one blocking DB transaction per tick.
    - New open symbols are subscribed within five seconds.
    - REST backup is one bulk request/minute and is disabled during a Binance 418/429 cooldown.
    """
    reconnect_delay = 2.0
    last_rest_backup_at = 0.0
    last_health_log_at = 0.0
    request_id = 0

    while True:
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
                                    last_price_message_at = time.monotonic()
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
            reconnect_delay = 2.0
            PAPER_PRICE_STREAM_STATE["connected"] = False

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

        # Backup only when the WebSocket is unavailable: a single bulk REST read
        # at most once per minute, and never while Binance has an active 418/429 ban.
        if time.monotonic() - last_rest_backup_at >= 60:
            last_rest_backup_at = time.monotonic()
            current_symbols = await _open_paper_symbols()
            if current_symbols and time.time() >= BINANCE_REST_BLOCKED_UNTIL:
                backup_prices = await _mark_prices(current_symbols)
                if backup_prices:
                    await evaluate_open_paper_trades(backup_prices, "BINANCE_REST_BULK_BACKUP")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(30.0, reconnect_delay * 2)



@app.get("/api/opportunities")
async def api_opportunities(limit: int = 100):
    limit = max(1, min(limit, 200))
    with SessionLocal() as db:
        latest_ids = select(func.max(PositionSnapshot.id).label("id")).group_by(PositionSnapshot.trader_id).subquery()
        latest_snapshots = db.scalars(select(PositionSnapshot).join(latest_ids, PositionSnapshot.id == latest_ids.c.id)).all()
        visible = {snap.id: snap for snap in latest_snapshots if not snap.hidden}
        observations = db.scalars(select(PositionObservation).where(
            PositionObservation.snapshot_id.in_(list(visible.keys()))
        ).order_by(desc(PositionObservation.id)).limit(500)).all() if visible else []
        traders = {t.trader_id: t.name for t in db.scalars(select(Trader)).all()}
    latest = {}
    for obs in observations:
        key = f"{obs.trader_id}|{obs.position_key}"
        if key not in latest:
            latest[key] = obs
    news = await fetch_news_items()
    result = []
    for obs in list(latest.values())[:limit]:
        snapshot = visible.get(obs.snapshot_id)
        result.append(await build_opportunity(obs, snapshot.observed_at.isoformat() if snapshot else None, traders.get(obs.trader_id), news))
    await evaluate_open_paper_trades()
    return sorted(result, key=lambda x: (x["qualified"], x["adverse_pct"] or 0), reverse=True)


@app.get("/api/paper")
async def api_paper():
    cfg = paper_config()
    with SessionLocal() as db:
        trades = db.scalars(select(PaperTrade).order_by(desc(PaperTrade.opened_at)).limit(300)).all()
        # The shadow portfolio replays the complete paper history, separately from
        # the newest 300 rows shown in the visible ledger.
        shadow_trades = db.scalars(
            select(PaperTrade).order_by(PaperTrade.opened_at, PaperTrade.id)
        ).all()
        gap_rows = db.execute(
            select(PaperTradePath.paper_trade_id, func.max(PaperTradePath.data_gap_seconds))
            .group_by(PaperTradePath.paper_trade_id)
        ).all()
        max_gaps = {int(trade_id): float(gap or 0.0) for trade_id, gap in gap_rows}

    open_trades = [trade for trade in trades if trade.status == "OPEN"]
    closed_trades = [trade for trade in trades if trade.status == "CLOSED"]
    realized = sum(float(trade.net_pnl or 0.0) for trade in closed_trades)
    unrealized = sum(float(trade.net_pnl or 0.0) for trade in open_trades)
    deployed_margin = sum(float(trade.margin_used or 0.0) for trade in open_trades)
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
            "id": trade.id, "trader_id": trade.trader_id, "trader_name": trade.trader_name,
            "leader_position_key": (trade.source_key.split("|", 1)[1] if "|" in trade.source_key else trade.source_key),
            "symbol": trade.symbol, "side": trade.side, "status": trade.status,
            "entry_price": trade.entry_price, "mark_price": trade.mark_price, "exit_price": trade.exit_price,
            "quantity": trade.quantity, "notional": trade.notional, "margin_used": trade.margin_used,
            "leverage": trade.leverage, "stop_price": trade.stop_price, "target_price": trade.target_price,
            "gross_pnl": round(float(trade.gross_pnl or 0.0), 8),
            "estimated_costs": round(float(trade.estimated_costs or 0.0), 8),
            "net_pnl": round(float(trade.net_pnl or 0.0), 8),
            "exit_reason": trade.exit_reason,
            "exit_reason_display": raw.get("post_exit_recovery_label") or trade.exit_reason,
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
            "mfe_pct": safe_float(raw.get("mfe_pct")), "mae_pct": safe_float(raw.get("mae_pct")),
            "data_quality_status": raw.get("data_quality_status", "LEGACY_UNCLASSIFIED"),
            "monitoring_status": raw.get("monitoring_status", "NOT_RECORDED"),
            "max_data_gap_seconds": round(max_gap, 3),
            "funding_history_verified": raw.get("funding_history_verified"),
            "funding_audit_status": raw.get("funding_audit_status", "LEGACY_FUNDING_UNVERIFIED"),
            "funding_cost_actual": safe_float(raw.get("funding_cost_actual")),
            "hold_duration_seconds": round(hold_seconds, 2),
            "hold_duration_hours": round(hold_seconds / 3600, 3),
            "post_exit_recovery_minutes_to_entry": safe_float(raw.get("post_exit_minutes_to_entry")),
            "post_exit_recovery_minutes_to_net_profit": safe_float(raw.get("post_exit_minutes_to_net_profit")),
            "post_exit_recovery_minutes_to_best": safe_float(raw.get("post_exit_minutes_to_best")),
            "post_exit_recovery_worst_net_pnl": safe_float(raw.get("post_exit_worst_net_pnl")),
            "post_exit_recovery_observed_minutes": safe_float(raw.get("post_exit_observed_minutes")),
            "opened_at": trade.opened_at.isoformat(),
            "closed_at": trade.closed_at.isoformat() if trade.closed_at else None
        })

    return {
        "config": cfg, "capital": round(float(cfg["capital_usdt"]), 8),
        "realized_pnl": round(realized, 8), "unrealized_pnl": round(unrealized, 8),
        "equity": round(equity, 8), "deployed_margin": round(deployed_margin, 8),
        "margin_cap": round(margin_cap, 8),
        "margin_utilization_pct": round(deployed_margin / max(float(cfg["capital_usdt"]), 0.01) * 100, 4),
        "margin_cap_exceeded": deployed_margin > margin_cap,
        "free_capital": round(max(0.0, equity - deployed_margin), 8),
        "open_count": len(open_trades), "closed_count": len(closed_trades),
        "correlated_exposure_groups": correlated_groups,
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
            "warning": "Positions in the same symbol and direction share market risk. Research-mode parallel PnL is not a deployable capital allocation."
        },
        "trades": trade_rows
    }


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

    campaign = {
        "trade_count": len(campaign_rows),
        "reentry_count": max(0, len(campaign_rows) - 1),
        "open_count": sum(1 for item in campaign_rows if item.status == "OPEN"),
        "realized_net_pnl": round(sum(float(item.net_pnl or 0.0) for item in campaign_rows if item.status == "CLOSED"), 8),
        "total_net_pnl": round(sum(float(item.net_pnl or 0.0) for item in campaign_rows), 8),
        "trade_ids": [item.id for item in campaign_rows],
        "trades": [{
            "id": item.id, "status": item.status, "entry_price": item.entry_price,
            "exit_price": item.exit_price, "net_pnl": round(float(item.net_pnl or 0.0), 8),
            "opened_at": item.opened_at.isoformat(),
            "closed_at": item.closed_at.isoformat() if item.closed_at else None,
            "exit_reason": item.exit_reason
        } for item in campaign_rows]
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
            "our_recovery_profit_usdt": recovery_profit_usdt, "status": leader_status
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
        if cached and time.monotonic() - float(cached.get("fetched_at", 0.0)) < 20:
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

    return {
        "trade": {
            "id": trade.id, "status": trade.status, "trader_id": trade.trader_id,
            "leader_position_key": trade.source_key.split("|", 1)[1] if "|" in trade.source_key else trade.source_key,
            "symbol": trade.symbol, "side": trade.side, "entry_price": trade.entry_price,
            "mark_price": trade.mark_price, "exit_price": trade.exit_price,
            "exit_reason": trade.exit_reason,
            "exit_reason_display": raw.get("post_exit_recovery_label") or trade.exit_reason,
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
            "funding_cost_actual": safe_float(raw.get("funding_cost_actual")),
            "funding_verified": bool(raw.get("funding_history_verified")),
            "funding_audit_status": raw.get("funding_audit_status", "LEGACY_FUNDING_UNVERIFIED"),
            "funding_source": raw.get("funding_source"),
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
    for key in ["capital_usdt", "risk_pct", "max_margin_pct", "max_total_margin_pct", "research_unconstrained", "shadow_max_total_margin_pct", "shadow_max_positions_per_symbol_direction", "target_pct", "stop_pct", "taker_fee_pct", "slippage_pct", "max_leverage", "min_adverse_pct"]:
        if key in body:
            val = safe_float(body[key])
            if val is None or val < 0:
                raise HTTPException(400, f"Invalid value for {key}")
            cfg[key] = val
    if "auto_enabled" in body:
        cfg["auto_enabled"] = bool(body["auto_enabled"])
    cfg["capital_usdt"] = max(0.0, min(float(cfg["capital_usdt"]), 100000000))
    cfg["risk_pct"] = min(float(cfg["risk_pct"]), 5.0)
    cfg["max_margin_pct"] = min(float(cfg["max_margin_pct"]), 100.0)
    cfg["max_total_margin_pct"] = min(max(float(cfg["max_total_margin_pct"]), 5.0), 100.0)
    cfg["shadow_max_total_margin_pct"] = min(max(float(cfg.get("shadow_max_total_margin_pct", 40.0)), 5.0), 100.0)
    cfg["shadow_max_positions_per_symbol_direction"] = min(
        max(int(float(cfg.get("shadow_max_positions_per_symbol_direction", 1))), 1), 5
    )
    cfg["target_pct"] = min(float(cfg["target_pct"]), 20.0)
    cfg["stop_pct"] = min(max(float(cfg["stop_pct"]), 0.05), 20.0)
    cfg["taker_fee_pct"] = min(float(cfg["taker_fee_pct"]), 2.0)
    cfg["slippage_pct"] = min(float(cfg["slippage_pct"]), 2.0)
    cfg["max_leverage"] = min(max(float(cfg["max_leverage"]), 1.0), 5.0)
    cfg["min_adverse_pct"] = min(max(float(cfg["min_adverse_pct"]), 0.05), 20.0)
    set_setting("paper_config", cfg)
    return {"saved": True, "config": cfg}

@app.get("/api/system")
async def api_system():
    metrics = db_metrics()
    with SessionLocal() as db:
        counts = {}
        for table_name, model in [
            ("traders", Trader), ("position_snapshots", PositionSnapshot),
            ("position_observations", PositionObservation), ("history_positions", HistoryPosition),
            ("paper_trades", PaperTrade), ("paper_trade_paths", PaperTradePath), ("paper_trade_events", PaperTradeEvent), ("source_checks", SourceCheck)
        ]:
            try:
                counts[table_name] = db.scalar(select(func.count()).select_from(model)) or 0
            except Exception:
                counts[table_name] = 0
    return {
        "database": metrics, "counts": counts,
        "monitor": {**last_scan, "worker_alive": worker_alive, "interval_seconds": DISCOVERY_INTERVAL, "batch": POSITION_BATCH},
        "provider": {
            "kopyon_key_configured": bool(KOPYON_KEY), "kopyon_base": KOPYON_ROOT,
            "binance_public_market_data": BINANCE_ROOT, "browser_token_required": False
        },
        "price_feed": {
            **PAPER_PRICE_STREAM_STATE,
            "symbol_last_update": dict(PAPER_SYMBOL_LAST_UPDATE),
            "binance_rest": binance_rest_status(),
            "rest_policy": "WebSocket-first; at most one bulk premiumIndex fallback per minute; no per-symbol mark-price polling.",
            "paper_exit_policy": {
                "leader_continuation_enabled": bool(paper_config().get("leader_continuation_enabled", True)),
                "leader_continuation_hard_loss_price_pct": float(paper_config().get("leader_continuation_hard_loss_price_pct", 0.60)),
                "leader_reversal_exit_enabled": bool(paper_config().get("leader_reversal_exit_enabled", True))
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
        raise HTTPException(429, "A monitor cycle ran recently. Automatic cadence is 30 minutes to protect the Kopyon free-tier request budget.")
    return await run_monitor_cycle(force=True)

@app.get("/api/news")
async def api_news(symbol: str = "", refresh: bool = False):
    items = await fetch_news_items(force=refresh)
    fetched_at = datetime.fromtimestamp(news_cache["fetched_at"], timezone.utc).isoformat() if news_cache.get("fetched_at") else None
    return {"source": "CoinDesk / Cointelegraph / The Block RSS", "items": find_news_for_symbol(symbol, items) if symbol else items[:40], "fetched_at": fetched_at, "feed_error": news_cache.get("error"), "warning": "Headline timing is context only; LAES does not claim a headline caused a particular price move."}

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
