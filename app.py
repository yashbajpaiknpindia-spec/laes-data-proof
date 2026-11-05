from __future__ import annotations
import os, json, hmac, hashlib
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, String, DateTime, Boolean, Integer, Float, Text, ForeignKey, UniqueConstraint, select, desc, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker, relationship

APP_NAME = "LAES Data Proof"
KOPYON_BASE = "https://api.kopyon.com/v5/binance-copy-trading"
BINANCE_BASE = "https://fapi.binance.com"
KOPYON_API_KEY = os.getenv("KOPYON_API_KEY", "").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./laes_local.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

class Base(DeclarativeBase): pass

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

Base.metadata.create_all(engine)
app = FastAPI(title=APP_NAME, version="0.1.0", docs_url="/api/docs")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def now(): return datetime.now(timezone.utc)
def safe_float(v):
    try: return None if v is None else float(v)
    except (TypeError, ValueError): return None

def require_admin(token: str | None):
    if not ADMIN_TOKEN:
        raise HTTPException(503, "ADMIN_TOKEN is not configured; sync actions are disabled")
    if not token or not hmac.compare_digest(token, ADMIN_TOKEN):
        raise HTTPException(401, "Invalid admin token")

def unwrap_data(payload: Any) -> dict:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict): return payload["data"]
    return payload if isinstance(payload, dict) else {}

async def kopyon_get(route: str, params: dict | None = None):
    if not KOPYON_API_KEY: raise HTTPException(503, "KOPYON_API_KEY is not configured")
    headers = {"X-API-KEY": KOPYON_API_KEY}
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(f"{KOPYON_BASE}/{route}", params=params or {}, headers=headers)
    if r.status_code >= 400:
        body = r.text[:600]
        raise HTTPException(502, f"Kopyon {route} returned HTTP {r.status_code}: {body}")
    try: return r.json(), r.status_code
    except Exception: raise HTTPException(502, f"Kopyon {route} returned non-JSON data")

async def binance_get(path: str, params: dict | None = None):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(f"{BINANCE_BASE}{path}", params=params or {})
    if r.status_code >= 400: raise HTTPException(502, f"Binance market-data endpoint returned HTTP {r.status_code}")
    return r.json(), r.status_code

def log_check(source, ok, status=None, latency=None, detail=""):
    with SessionLocal() as db:
        db.add(SourceCheck(source=source, ok=ok, http_status=status, latency_ms=latency, detail=detail[:1000]))
        db.commit()

def persist_traders(rows: list[dict]):
    count = 0
    with SessionLocal() as db:
        for item in rows:
            tid = str(item.get("traderId") or item.get("portfolioId") or item.get("id") or "").strip()
            if not tid: continue
            t = db.get(Trader, tid)
            if t is None:
                t = Trader(trader_id=tid)
                db.add(t)
            t.name = item.get("name") or item.get("nickname") or t.name
            t.avatar_url = item.get("avatarUrl") or t.avatar_url
            t.portfolio_type = item.get("type") or item.get("portfolioType") or t.portfolio_type
            # Visibility here is portfolio classification, not proof that current positions are visible.
            if str(t.portfolio_type or "").upper() == "PRIVATE": t.visibility = "private_portfolio"
            elif str(t.portfolio_type or "").upper() == "PUBLIC": t.visibility = "public_portfolio"
            t.roi = safe_float(item.get("roi")); t.pnl = safe_float(item.get("pnl")); t.aum = safe_float(item.get("aum"))
            t.max_drawdown = safe_float(item.get("maxDrawdown")); t.win_rate = safe_float(item.get("winRate"))
            t.copier_pnl = safe_float(item.get("copierPnl")); t.sharpe_ratio = safe_float(item.get("sharpeRatio"))
            t.raw_json = json.dumps(item, separators=(",", ":"), default=str)
            t.last_seen_at = now(); count += 1
        db.commit()
    return count

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "app_name": APP_NAME})

@app.get("/healthz")
async def healthz():
    db_ok = False
    try:
        with SessionLocal() as db: db.execute(select(func.count()).select_from(Trader)).scalar_one()
        db_ok = True
    except Exception: pass
    return {"status": "ok" if db_ok else "degraded", "database": "connected" if db_ok else "unavailable", "kopyon_key_configured": bool(KOPYON_API_KEY), "admin_sync_enabled": bool(ADMIN_TOKEN), "trading_enabled": False, "timestamp": now().isoformat()}

@app.get("/api/status")
async def status():
    with SessionLocal() as db:
        traders = db.scalar(select(func.count()).select_from(Trader)) or 0
        snaps = db.scalar(select(func.count()).select_from(PositionSnapshot)) or 0
        observations = db.scalar(select(func.count()).select_from(PositionObservation)) or 0
        visible = db.scalar(select(func.count()).select_from(PositionSnapshot).where(PositionSnapshot.hidden == False)) or 0
        hidden = db.scalar(select(func.count()).select_from(PositionSnapshot).where(PositionSnapshot.hidden == True)) or 0
        checks = db.scalars(select(SourceCheck).order_by(desc(SourceCheck.checked_at)).limit(8)).all()
    return {"traders": traders, "position_snapshots": snaps, "position_observations": observations, "visible_snapshots": visible, "hidden_snapshots": hidden, "kopyon_key_configured": bool(KOPYON_API_KEY), "direct_binance_role": "official public market data only; no documented public individual lead-position endpoint", "trading_enabled": False, "sources": [dict(source=c.source, ok=c.ok, checked_at=c.checked_at.isoformat(), status=c.http_status, latency_ms=c.latency_ms, detail=c.detail) for c in checks]}

@app.post("/api/sources/check")
async def check_sources(x_admin_token: str | None = Header(default=None)):
    require_admin(x_admin_token)
    import time
    results = {}
    if KOPYON_API_KEY:
        start = time.perf_counter()
        try:
            payload, code = await kopyon_get("traders", {"period":"7D", "sortBy":"copierPnl", "order":"DESC", "type":"PUBLIC", "page":1, "rows":1})
            ms = int((time.perf_counter()-start)*1000); data=unwrap_data(payload)
            results["kopyon"]={"ok":True,"http_status":code,"latency_ms":ms,"response_keys":list(data.keys()),"sample_count":len(data.get("traders", data.get("rows", [])) or [])}
            log_check("kopyon", True, code, ms, "Trader endpoint responded")
        except Exception as e:
            results["kopyon"]={"ok":False,"detail":str(e)}; log_check("kopyon", False, None, None, str(e))
    else:
        results["kopyon"]={"ok":False,"detail":"KOPYON_API_KEY missing; configure it in Render environment"}
    start=time.perf_counter()
    try:
        _, code = await binance_get("/fapi/v1/ping")
        ms=int((time.perf_counter()-start)*1000)
        results["binance_market_data"]={"ok":True,"http_status":code,"latency_ms":ms,"role":"Official market data, not trader identity/position data"}
        log_check("binance_market_data", True, code, ms, "USDⓈ-M Futures public ping succeeded")
    except Exception as e:
        results["binance_market_data"]={"ok":False,"detail":str(e)}; log_check("binance_market_data", False, None, None, str(e))
    results["binance_copy_trader_positions"]={"ok":False,"detail":"No documented public Binance API endpoint for arbitrary individual lead-trader positions was identified. Use publicly disclosed portfolio data via Kopyon; do not infer or bypass private positions."}
    return results

@app.post("/api/sync/traders")
async def sync_traders(request: Request, x_admin_token: str | None = Header(default=None), period: str="30D", sort_by: str="copierPnl", portfolio_type: str="PUBLIC", pages: int=1):
    require_admin(x_admin_token)
    if period not in {"7D","30D","90D","180D","1Y"}: raise HTTPException(400,"Invalid period")
    if portfolio_type not in {"PUBLIC","PRIVATE","SIGNAL","ALL"}: raise HTTPException(400,"Invalid portfolio_type")
    pages=max(1,min(pages,5)); total=0; page_results=[]
    for page in range(1,pages+1):
        payload, code = await kopyon_get("traders", {"period":period,"sortBy":sort_by,"order":"DESC","type":portfolio_type,"page":page,"rows":20})
        data=unwrap_data(payload)
        rows=data.get("traders") or data.get("rows") or data.get("items") or []
        if not isinstance(rows,list): rows=[]
        n=persist_traders(rows); total+=n; page_results.append({"page":page,"received":len(rows),"saved":n,"hasMore":data.get("hasMore")})
        if data.get("hasMore") is False or not rows: break
    log_check("kopyon_traders", True, 200, None, f"Saved {total} trader rows")
    return {"saved":total,"pages":page_results,"portfolio_type":portfolio_type,"period":period}

@app.post("/api/sync/trader/{trader_id}/positions")
async def sync_positions(trader_id: str, x_admin_token: str | None = Header(default=None)):
    require_admin(x_admin_token)
    payload, code = await kopyon_get("openPositions", {"traderId":trader_id})
    data=unwrap_data(payload); positions=data.get("positions") or []
    hidden=bool(data.get("hidden", False))
    fetched_at=data.get("fetchedAt") or data.get("fetched_at")
    with SessionLocal() as db:
        trader=db.get(Trader,trader_id)
        if trader is None:
            trader=Trader(trader_id=trader_id, name=trader_id); db.add(trader)
        trader.visibility = "hidden_positions" if hidden else ("public_positions" if positions else "visible_empty_or_unreported")
        snap=PositionSnapshot(trader_id=trader_id, source_fetched_at=str(fetched_at) if fetched_at else None, hidden=hidden, position_count=len(positions), raw_json=json.dumps(payload, separators=(",",":"), default=str))
        db.add(snap); db.flush()
        saved=0
        if not hidden:
            for p in positions:
                symbol=str(p.get("symbol") or "UNKNOWN")
                side=str(p.get("side") or "UNKNOWN").upper()
                pid=str(p.get("positionId") or "")
                pkey=f"{pid}|{symbol}|{side}" if pid else f"{symbol}|{side}"
                db.add(PositionObservation(snapshot_id=snap.id,trader_id=trader_id,position_key=pkey,symbol=symbol,side=side,size=safe_float(p.get("size")),entry_price=safe_float(p.get("entryPrice")),mark_price=safe_float(p.get("markPrice")),unrealized_pnl=safe_float(p.get("unrealizedPnl")),leverage=safe_float(p.get("leverage")),margin_mode=p.get("marginMode"),liquidation_price=safe_float(p.get("liquidationPrice")),roi=safe_float(p.get("roi")),raw_json=json.dumps(p,separators=(",",":"),default=str)))
                saved+=1
        # Critical: hidden=true does NOT create close events and does NOT erase previous visible observations.
        db.commit()
    log_check("kopyon_open_positions", True, code, None, f"trader={trader_id}; hidden={hidden}; positions={len(positions)}")
    return {"trader_id":trader_id,"hidden":hidden,"reported_position_count":len(positions),"saved_observations":saved,"source_fetched_at":fetched_at,"interpretation":"Positions are private/unavailable; previous observations retained" if hidden else "Snapshot stored; empty array is not automatically interpreted as a closed position" if not positions else "Visible position snapshot stored"}

@app.get("/api/traders")
async def get_traders(limit: int=100):
    limit=max(1,min(limit,500))
    with SessionLocal() as db:
        rows=db.scalars(select(Trader).order_by(desc(Trader.last_seen_at)).limit(limit)).all()
        return [{"trader_id":t.trader_id,"name":t.name,"portfolio_type":t.portfolio_type,"visibility":t.visibility,"roi":t.roi,"pnl":t.pnl,"aum":t.aum,"max_drawdown":t.max_drawdown,"win_rate":t.win_rate,"copier_pnl":t.copier_pnl,"sharpe_ratio":t.sharpe_ratio,"last_seen_at":t.last_seen_at.isoformat()} for t in rows]

@app.get("/api/positions")
async def get_positions(limit: int=200):
    limit=max(1,min(limit,1000))
    with SessionLocal() as db:
        rows=db.execute(select(PositionObservation, PositionSnapshot.observed_at).join(PositionSnapshot, PositionObservation.snapshot_id==PositionSnapshot.id).order_by(desc(PositionSnapshot.observed_at)).limit(limit)).all()
        return [{"trader_id":p.trader_id,"symbol":p.symbol,"side":p.side,"size":p.size,"entry_price":p.entry_price,"mark_price":p.mark_price,"unrealized_pnl":p.unrealized_pnl,"leverage":p.leverage,"margin_mode":p.margin_mode,"liquidation_price":p.liquidation_price,"roi":p.roi,"observed_at":ts.isoformat()} for p,ts in rows]

@app.get("/api/binance/market/{symbol}")
async def binance_market(symbol: str):
    symbol=symbol.upper()
    if not symbol.isalnum(): raise HTTPException(400,"Invalid symbol")
    data,_=await binance_get("/fapi/v1/premiumIndex",{"symbol":symbol})
    return {"source":"Binance USDⓈ-M Futures public market data","symbol":data.get("symbol"),"mark_price":data.get("markPrice"),"index_price":data.get("indexPrice"),"last_funding_rate":data.get("lastFundingRate"),"next_funding_time":data.get("nextFundingTime"),"time":data.get("time")}

@app.get("/api/binance/klines/{symbol}")
async def binance_klines(symbol: str, interval: str="1m", limit: int=100):
    if interval not in {"1m","3m","5m","15m","30m","1h","2h","4h","1d"}: raise HTTPException(400,"Unsupported interval")
    limit=max(1,min(limit,500))
    data,_=await binance_get("/fapi/v1/klines",{"symbol":symbol.upper(),"interval":interval,"limit":limit})
    return {"source":"Binance USDⓈ-M Futures public market data","symbol":symbol.upper(),"interval":interval,"candles":data}

@app.get("/api/observations/{trader_id}")
async def trader_observations(trader_id: str, limit: int=50):
    limit=max(1,min(limit,200))
    with SessionLocal() as db:
        snaps=db.scalars(select(PositionSnapshot).where(PositionSnapshot.trader_id==trader_id).order_by(desc(PositionSnapshot.observed_at)).limit(limit)).all()
        return [{"observed_at":s.observed_at.isoformat(),"source_fetched_at":s.source_fetched_at,"hidden":s.hidden,"position_count":s.position_count,"raw":json.loads(s.raw_json)} for s in snaps]
