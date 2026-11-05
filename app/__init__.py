from __future__ import annotations
import asyncio, json, math, importlib.util, sys
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select, desc, func, text
ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location("laes_legacy", ROOT/"app.py")
legacy=importlib.util.module_from_spec(spec); sys.modules["laes_legacy"]=legacy; spec.loader.exec_module(legacy)
app=legacy.app
@asynccontextmanager
async def noop(_app): yield
app.router.lifespan_context=noop
def now(): return datetime.now(timezone.utc)
def js(v,d=None):
    try:return json.loads(v or "")
    except:return {} if d is None else d
def rawmeta(t):
    x=js(t.raw_json,{})
    if not isinstance(x,dict):x={}
    return x
def move(side,e,p):
    if not e or not p or e<=0:return None
    return ((p-e)/e*100) if side=="LONG" else ((e-p)/e*100)
def behavior(tid,symbol=None,side=None):
    with legacy.SessionLocal() as db:
        q=select(legacy.HistoryPosition).where(legacy.HistoryPosition.trader_id==tid)
        if symbol:q=q.where(legacy.HistoryPosition.symbol==symbol)
        if side:q=q.where(legacy.HistoryPosition.side==side)
        hs=db.scalars(q).all()
    ds=[(h.closed_at_ms-h.opened_at_ms)/3600000 for h in hs if h.opened_at_ms and h.closed_at_ms and h.closed_at_ms>h.opened_at_ms]
    if not ds:return {"sample":0,"avg_hold_hours":None,"median_hold_hours":None,"p90_hold_hours":None}
    s=sorted(ds)
    return {"sample":len(s),"avg_hold_hours":round(sum(s)/len(s),2),"median_hold_hours":round(s[len(s)//2],2),"p90_hold_hours":round(s[min(len(s)-1,math.ceil(.9*len(s))-1)],2)}
def path_stats(tid,symbol,side,target=.7):
    with legacy.SessionLocal() as db:
        hs=db.scalars(select(legacy.HistoryPosition).where(legacy.HistoryPosition.trader_id==tid,legacy.HistoryPosition.symbol==symbol,legacy.HistoryPosition.side==side)).all()
        obs=db.scalars(select(legacy.PositionObservation).where(legacy.PositionObservation.trader_id==tid,legacy.PositionObservation.symbol==symbol,legacy.PositionObservation.side==side)).all()
        snaps={s.id:s for s in db.scalars(select(legacy.PositionSnapshot).where(legacy.PositionSnapshot.id.in_([o.snapshot_id for o in obs]) if obs else text("1=0"))).all()}
    paths=[]
    for h in hs:
        if not h.position_id:continue
        pp=[o for o in obs if o.position_key.startswith(h.position_id+"|") and o.snapshot_id in snaps]
        if len(pp)<2:continue
        pp.sort(key=lambda o:snaps[o.snapshot_id].observed_at)
        adv=[o for o in pp if o.unrealized_pnl is not None and o.unrealized_pnl<0]
        if not adv:continue
        first=adv[0];t0=snaps[first.snapshot_id].observed_at;bounce=target_time=None;dd=0
        for o in pp[pp.index(first):]:
            m=move(side,h.entry_price,o.mark_price)
            if m is not None:
                dd=max(dd,m);dt=(snaps[o.snapshot_id].observed_at-t0).total_seconds()/60
                if m>=0 and bounce is None:bounce=dt
                if m>=target and target_time is None:target_time=dt
        paths.append((h.net_pnl is not None and h.net_pnl>0,bounce,target_time,dd))
    n=len(paths);vals=lambda i:[x[i] for x in paths if x[i] is not None];med=lambda a:sorted(a)[len(a)//2] if a else None
    if not n:return {"sample_size":0,"recovery_probability":None,"bounce_median_minutes":None,"target_median_minutes":None,"max_dd_p95_pct":None}
    d=sorted(x[3] for x in paths)
    return {"sample_size":n,"recovery_probability":round(sum(x[0] for x in paths)/n*100,1),"bounce_median_minutes":round(med(vals(1)),1) if vals(1) else None,"target_median_minutes":round(med(vals(2)),1) if vals(2) else None,"max_dd_p95_pct":round(d[min(len(d)-1,max(0,math.ceil(.95*len(d))-1))],3)}
def persist_state(**kw):
    s=legacy.setting("last_monitor_state",{});s.update(kw);legacy.set_setting("last_monitor_state",s);return s
async def eval_paper():
    with legacy.SessionLocal() as db:ts=db.scalars(select(legacy.PaperTrade).where(legacy.PaperTrade.status=="OPEN").limit(300)).all()
    prices={}
    for sym in sorted({t.symbol for t in ts}):
        try:p,_=await legacy.binance_get("/fapi/v1/premiumIndex",{"symbol":sym});prices[sym]=legacy.safe_float(p.get("markPrice"))
        except Exception as e:legacy.log_check("paper_mark_price",False,None,None,f"{sym}: {str(e)[:160]}")
    cfg=legacy.paper_config()
    with legacy.SessionLocal() as db:
        ts=db.scalars(select(legacy.PaperTrade).where(legacy.PaperTrade.status=="OPEN")).all()
        for t in ts:
            mark=prices.get(t.symbol)
            if not mark:continue
            meta=rawmeta(t);i=meta.setdefault("intelligence",{});t.mark_price=mark
            t.gross_pnl=(1 if t.side=="LONG" else -1)*(mark-t.entry_price)*t.quantity
            t.estimated_costs=t.notional*(float(cfg.get("taker_fee_pct",.05))+float(cfg.get("slippage_pct",.02)))/100*2;t.net_pnl=t.gross_pnl-t.estimated_costs
            i["last_check_at"]=now().isoformat();i["paper_age_minutes"]=(now()-t.opened_at).total_seconds()/60
            lk=i.get("leader_position_key")
            if lk:
                q=select(legacy.PositionObservation,legacy.PositionSnapshot).join(legacy.PositionSnapshot,legacy.PositionSnapshot.id==legacy.PositionObservation.snapshot_id).where(legacy.PositionObservation.trader_id==t.trader_id,legacy.PositionObservation.position_key==lk).order_by(desc(legacy.PositionSnapshot.observed_at)).limit(1)
                row=db.execute(q).first()
                if row:
                    o,s=row;i["leader_last_checked_at"]=s.observed_at.isoformat();i["leader_mark_now"]=o.mark_price;i["leader_unrealized_pnl_now"]=o.unrealized_pnl;i["leader_adverse_now"]=move(o.side,o.entry_price,o.mark_price);i["leader_status"]="OPEN_OBSERVED"
                else:i["leader_status"]="NOT_CURRENTLY_OBSERVED"
            age=i.get("paper_age_minutes",0);expected=i.get("expected_bounce_minutes");reasons=[]
            if i.get("leader_last_checked_at"):
                try:
                    lag=(now()-datetime.fromisoformat(i["leader_last_checked_at"].replace("Z","+00:00"))).total_seconds()/60
                    if lag>45:reasons.append("leader link stale")
                except:pass
            pnlpct=move(t.side,t.entry_price,mark) or 0
            if expected and age>float(expected)*2.5:reasons.append("bounce overdue vs model")
            if expected and age>max(30,float(expected)) and pnlpct<.10:reasons.append("no meaningful rebound by expected window")
            i["dead_trade_reasons"]=reasons
            stop=mark<=t.stop_price if t.side=="LONG" else mark>=t.stop_price;target=mark>=t.target_price if t.side=="LONG" else mark<=t.target_price
            soft=(pnlpct>=.20 and expected and age>float(expected));timeout=bool(reasons and pnlpct<.10 and age>max(45,float(expected or 45)*1.5))
            if stop:t.status="CLOSED";t.exit_price=mark;t.exit_reason="STOP_LOSS";t.closed_at=now()
            elif target:t.status="CLOSED";t.exit_price=mark;t.exit_reason="TARGET";t.closed_at=now()
            elif soft:t.status="CLOSED";t.exit_price=mark;t.exit_reason="SOFT_PROFIT_TIME_DECAY";t.closed_at=now()
            elif timeout:t.status="CLOSED";t.exit_price=mark;t.exit_reason="DEAD_TRADE_TIMEOUT";t.closed_at=now()
            t.raw_json=json.dumps(meta,default=str,separators=(",",":"))
        db.commit()
def create_trade(o,name,mark,market,intel):
    cfg=legacy.paper_config();key=f"{o.trader_id}|{o.position_key}"
    with legacy.SessionLocal() as db:
        if db.scalar(select(legacy.PaperTrade.id).where(legacy.PaperTrade.source_key==key,legacy.PaperTrade.status=="OPEN").limit(1)):return
        openx=db.scalars(select(legacy.PaperTrade).where(legacy.PaperTrade.status=="OPEN")).all()
        realized=float(db.scalar(select(func.coalesce(func.sum(legacy.PaperTrade.net_pnl),0)).where(legacy.PaperTrade.status=="CLOSED")) or 0);cap=max(0,float(cfg["capital_usdt"])+realized);used=sum(x.margin_used for x in openx);remain=max(0,cap*float(cfg.get("max_total_margin_pct",40))/100-used)
        stop=max(.0005,float(cfg.get("stop_pct",.45))/100);margin=min(cap*float(cfg.get("risk_pct",.5))/100/stop,cap*float(cfg.get("max_margin_pct",10))/100,remain);lev=max(1,min(float(cfg.get("max_leverage",3)),float(o.leverage or 1)));notional=min(margin*lev,cap*.25)
        if margin<=0 or notional<5:return
        target=float(cfg.get("target_pct",.7))/100;sp=mark*(1-stop) if o.side=="LONG" else mark*(1+stop);tp=mark*(1+target) if o.side=="LONG" else mark*(1-target)
        meta={"intelligence":{"leader_position_key":o.position_key,"leader_entry":o.entry_price,"leader_mark_at_entry":o.mark_price,"leader_adverse_at_entry":intel.get("adverse_pct"),"leader_last_checked_at":intel.get("observed_at"),"entered_at":now().isoformat(),"expected_bounce_minutes":intel.get("bounce_model",{}).get("expected_bounce_minutes"),"expected_target_minutes":intel.get("bounce_model",{}).get("expected_target_minutes"),"expected_profit_probability_pct":intel.get("bounce_model",{}).get("expected_profit_probability_pct"),"max_expected_dd_pct":intel.get("bounce_model",{}).get("max_expected_dd_pct"),"preferred_profit_pct":float(cfg.get("target_pct",.7)),"last_check_at":now().isoformat()}}
        db.add(legacy.PaperTrade(source_key=key,trader_id=o.trader_id,trader_name=name,symbol=o.symbol,side=o.side,status="OPEN",entry_price=mark,mark_price=mark,quantity=notional/mark,notional=notional,margin_used=margin,leverage=lev,stop_price=sp,target_price=tp,raw_json=json.dumps(meta)));db.commit()
async def opportunities(limit=100):
    with legacy.SessionLocal() as db:
        obs=db.scalars(select(legacy.PositionObservation).order_by(desc(legacy.PositionObservation.id)).limit(700)).all();names={t.trader_id:t.name for t in db.scalars(select(legacy.Trader)).all()}
        snaps={s.id:s for s in db.scalars(select(legacy.PositionSnapshot).where(legacy.PositionSnapshot.id.in_([o.snapshot_id for o in obs]))).all()} if obs else {}
    latest={}
    for o in obs:
        k=f"{o.trader_id}|{o.position_key}"
        if k not in latest:latest[k]=o
    out=[]
    for o in list(latest.values())[:limit]:
        s=snaps.get(o.snapshot_id)
        try:m=await legacy.market_features(o.symbol)
        except:m={"symbol":o.symbol}
        mark=legacy.safe_float(m.get("mark_price")) or o.mark_price;adv=move(o.side,o.entry_price,mark);under=(o.unrealized_pnl is not None and o.unrealized_pnl<0) or (adv is not None and adv>0)
        mom=legacy.safe_float(m.get("momentum_5m_pct"));confirm=mom is not None and ((mom>0) if o.side=="LONG" else (mom<0));age=(now()-s.observed_at).total_seconds()/60 if s else 999999;atr=legacy.safe_float(m.get("atr_24h_pct"));dead=[]
        if age>45:dead.append("leader observation stale")
        if atr is not None and atr>8:dead.append("extreme volatility")
        if under and not confirm:dead.append("bounce confirmation absent")
        cfg=legacy.paper_config();qualified=bool(under and adv is not None and adv>=float(cfg.get("min_adverse_pct",.3)) and confirm and not dead)
        opened=legacy.ms_datetime(js(o.raw_json,{}).get("openedAt"));hold=(now()-opened).total_seconds()/3600 if opened else (now()-s.observed_at).total_seconds()/3600 if s else None
        b=behavior(o.trader_id,o.symbol,o.side);h=path_stats(o.trader_id,o.symbol,o.side,float(cfg.get("target_pct",.7)));expected=h["bounce_median_minutes"] or ((b["median_hold_hours"]*60) if b["median_hold_hours"] else None);unusual=(hold>2*b["median_hold_hours"] or hold<.25*b["median_hold_hours"]) if hold is not None and b["median_hold_hours"] else None
        x={"trader_id":o.trader_id,"trader_name":names.get(o.trader_id,o.trader_id),"position_key":o.position_key,"symbol":o.symbol,"side":o.side,"entry_price":o.entry_price,"mark_price":mark,"adverse_pct":adv,"unrealized_pnl":o.unrealized_pnl,"roi":o.roi,"leverage":o.leverage,"margin_mode":o.margin_mode,"liquidation_price":o.liquidation_price,"observed_at":s.observed_at.isoformat() if s else None,"observation_age_minutes":round(age,1),"underwater":under,"qualified":qualified,"status":"QUALIFIED" if qualified else "DEAD-TRADE WATCH" if under and dead else "UNDERWATER WATCH" if under else "NOT UNDERWATER","dead_trade_reasons":dead,"momentum_5m_pct":mom,"market":m,"recovery":{"probability":h["recovery_probability"],"sample_size":h["sample_size"]},"bounce_model":{"expected_bounce_minutes":round(expected,1) if expected else None,"expected_target_minutes":h["target_median_minutes"],"expected_profit_probability_pct":h["recovery_probability"],"preferred_profit_pct":float(cfg.get("target_pct",.7)),"max_expected_dd_pct":h["max_dd_p95_pct"],"basis":"LAES-observed matched paths; hold-time fallback when path sample unavailable"},"holding":{"current_hours":round(hold,2) if hold is not None else None,"typical_median_hours":b["median_hold_hours"],"typical_average_hours":b["avg_hold_hours"],"typical_p90_hours":b["p90_hold_hours"],"unusual":unusual,"completed_positions_sample":b["sample"]},"leader_link":{"trader_id":o.trader_id,"position_key":o.position_key,"last_observed_at":s.observed_at.isoformat() if s else None},"likely_causes":[]}
        if qualified and cfg.get("auto_enabled"):create_trade(o,names.get(o.trader_id),mark,m,x)
        out.append(x)
    await eval_paper();return sorted(out,key=lambda z:(z["qualified"],z["recovery"]["probability"] or -1,z["adverse_pct"] or 0),reverse=True)
async def paper():
    await eval_paper();cfg=legacy.paper_config()
    with legacy.SessionLocal() as db:ts=db.scalars(select(legacy.PaperTrade).order_by(desc(legacy.PaperTrade.opened_at)).limit(300)).all()
    op=[x for x in ts if x.status=="OPEN"];cl=[x for x in ts if x.status=="CLOSED"];real=sum(x.net_pnl for x in cl);unreal=sum(x.net_pnl for x in op);dep=sum(x.margin_used for x in op);eq=float(cfg["capital_usdt"])+real+unreal;rows=[]
    for t in ts:
        i=rawmeta(t).get("intelligence",{})
        rows.append({"id":t.id,"trader_id":t.trader_id,"trader_name":t.trader_name,"symbol":t.symbol,"side":t.side,"status":t.status,"entry_price":t.entry_price,"mark_price":t.mark_price,"exit_price":t.exit_price,"quantity":t.quantity,"notional":t.notional,"margin_used":t.margin_used,"leverage":t.leverage,"stop_price":t.stop_price,"target_price":t.target_price,"gross_pnl":t.gross_pnl,"estimated_costs":t.estimated_costs,"net_pnl":t.net_pnl,"exit_reason":t.exit_reason,"opened_at":t.opened_at.isoformat(),"closed_at":t.closed_at.isoformat() if t.closed_at else None,**i})
    return {"config":cfg,"capital":cfg["capital_usdt"],"realized_pnl":round(real,4),"unrealized_pnl":round(unreal,4),"equity":round(eq,4),"deployed_margin":round(dep,4),"margin_cap":round(float(cfg["capital_usdt"])*float(cfg.get("max_total_margin_pct",40))/100,4),"margin_utilization_pct":round(dep/max(float(cfg["capital_usdt"]),.01)*100,2),"margin_cap_exceeded":dep>float(cfg["capital_usdt"])*float(cfg.get("max_total_margin_pct",40))/100,"free_capital":round(max(0,eq-dep),4),"open_count":len(op),"closed_count":len(cl),"target_is_soft_preference":True,"trades":rows}
async def overview():
    d=await legacy.api_overview();d["monitor"]["last_persisted_check"]=legacy.setting("last_monitor_state",{});d["paper"]=await paper();return d
async def sync_now():
    r=await legacy.run_monitor_cycle(force=True);persist_state(state=r.get("state"),started_at=r.get("started_at"),finished_at=r.get("finished_at"),traders_found=r.get("traders_found"),positions_checked=r.get("positions_checked"),observations_saved=r.get("observations_saved"),persisted_at=now().isoformat());await paper();return r
async def worker():
    legacy.worker_alive=True
    while True:
        try:await paper();persist_state(last_paper_check_at=now().isoformat())
        except Exception as e:legacy.log_check("enhanced_paper_worker",False,None,None,str(e)[:700])
        await asyncio.sleep(60)
@asynccontextmanager
async def lifespan(_app):
    persist_state(service_boot_at=now().isoformat());task=asyncio.create_task(worker());_app.state.worker=task
    try:yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):await task
app.router.lifespan_context=lifespan
async def r_opp():return JSONResponse(await opportunities())
async def r_paper():return JSONResponse(await paper())
async def r_over():return JSONResponse(await overview())
async def r_sync():return JSONResponse(await sync_now())
for r in app.routes:
    if getattr(r,"path",None)=="/api/opportunities":r.endpoint=r_opp
    elif getattr(r,"path",None)=="/api/paper":r.endpoint=r_paper
    elif getattr(r,"path",None)=="/api/overview":r.endpoint=r_over
    elif getattr(r,"path",None)=="/api/sync/now":r.endpoint=r_sync
@app.get("/api/paper/intelligence")
async def paper_intelligence():return await paper()
@app.get("/api/monitor/persistence")
async def monitor_persistence():return legacy.setting("last_monitor_state",{})
@app.middleware("http")
async def inject_intelligence(request,call_next):
    resp=await call_next(request)
    if request.url.path=="/" and "text/html" in resp.headers.get("content-type",""):
        body=b""
        async for chunk in resp.body_iterator:body+=chunk
        extra=b'<script src="/static/enhanced.js?v=1"></script>'
        body=body.replace(b"</body>",extra+b"</body>")
        return Response(body,status_code=resp.status_code,headers=dict(resp.headers),media_type="text/html")
    return resp
