// LAES client diagnostics: never let one missing DOM hook prevent the entire data boot sequence.
window.addEventListener("error", e => {
  try { fetch("/api/client-error",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({message:e.message,source:e.filename,line:e.lineno,column:e.colno,stack:e.error?.stack||""})}); } catch {}
});
window.addEventListener("unhandledrejection", e => {
  try { fetch("/api/client-error",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({message:String(e.reason?.message||e.reason||"Unhandled promise rejection"),stack:e.reason?.stack||""})}); } catch {}
});
const bind=(id,event,fn)=>{const el=$(id);if(!el){console.warn("LAES missing DOM hook:",id);return false;}el.addEventListener(event,fn);return true;};
const click=(id,fn)=>{const el=$(id);if(!el){console.warn("LAES missing DOM hook:",id);return false;}el.onclick=fn;return true;};
const $=id=>document.getElementById(id);
const state={page:"overview",paperTab:null,closedFilter:"all",paperData:null,researchReplay:null,momentumData:null};
let paperDetailTimer=null;
let paperLoadInFlight=false;
const pendingGetRequests=new Map();
const fmt=(v,d=2)=>v===null||v===undefined||v===""||Number.isNaN(Number(v))?"—":Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
const money=v=>v===null||v===undefined?"—":fmt(v,Math.abs(Number(v))<0.01?8:4)+" USDT";
const pct=v=>v===null||v===undefined?"—":fmt(v,2)+"%";
const date=v=>{if(!v)return"—";const d=new Date(v);return Number.isNaN(d.getTime())?"—":d.toLocaleString();};
const duration=h=>h===null||h===undefined?"Unavailable":h<1?Math.round(h*60)+"m":h<24?h.toFixed(1)+"h":(h/24).toFixed(1)+"d";
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
async function api(url,options={}){
 const method=(options.method||"GET").toUpperCase(),key=method==="GET"&&!options.body?String(url):null;
 if(key&&pendingGetRequests.has(key))return pendingGetRequests.get(key);
 const request=(async()=>{const r=await fetch(url,{headers:{"Content-Type":"application/json"},...options});let d={};try{d=await r.json()}catch{d={detail:await r.text()}}if(!r.ok)throw new Error(d.detail||"HTTP "+r.status);return d;})();
 if(!key)return request;
 pendingGetRequests.set(key,request);
 try{return await request;}finally{if(pendingGetRequests.get(key)===request)pendingGetRequests.delete(key);}
}
function notice(s,kind){const n=$("notice");n.textContent=s;n.className="notice "+(kind||"");n.classList.remove("hidden");clearTimeout(notice.timer);notice.timer=setTimeout(()=>n.classList.add("hidden"),9000);}
function setPage(name, options={}) {
 const page=document.getElementById("page-"+name);
 if(!page){console.warn("LAES unknown page:",name);return false;}
 state.page=name;
 document.querySelectorAll(".page").forEach(x=>x.classList.toggle("active",x===page));
 document.querySelectorAll(".nav-item").forEach(x=>x.classList.toggle("active",x.dataset.page===name));
 const titles={overview:"Overview",traders:"Trader universe",positions:"Live positions",opportunities:"Opportunity scanner",paper:"Paper trading desk",market:"Market & news",database:"Database & system",momentum:"Momentum paper"};
 $("pageTitle").textContent=titles[name]||"Overview";
 if(options.scroll!==false)window.scrollTo({top:0,behavior:"auto"});
 if(name==="overview")refreshOverview();
 else if(name==="traders")loadTraders();
 else if(name==="positions")loadPositions();
 else if(name==="opportunities"){loadOpportunities();loadPipelineDecisions();}
 else if(name==="paper"){loadPaper();}
 else if(name==="momentum"){loadMomentumPaper();}
 else if(name==="market"){loadMarket(false);loadMarketUniverse();}
 else if(name==="database")loadSystem();
 return true;
}
document.addEventListener("click",e=>{
 const target=e.target instanceof Element?e.target:e.target?.parentElement;
 const filter=target?.closest("[data-closed-filter]");
 if(filter){e.preventDefault();setClosedTradeFilter(filter.dataset.closedFilter);return;}
 const paperTab=target?.closest("[data-paper-tab]");
 if(paperTab){e.preventDefault();switchPaperTab(paperTab.dataset.paperTab);return;}
 const nav=target?.closest(".nav-item[data-page]");
 if(nav){e.preventDefault();setPage(nav.dataset.page);return;}
 const goto=target?.closest("[data-goto]");
 if(goto){e.preventDefault();const pageName=goto.dataset.goto;if(pageName)setPage(pageName);return;}
});
function source(id,dot,ok,label){$(id).textContent=label;$(id).className="connection-state "+(ok===true?"ok":ok===false?"bad":"");$(dot).className="source-dot "+(ok===true?"ok":ok===false?"bad":"");}
async function refreshOverview(){
 try{
 const d=await api("/api/overview");state.overview=d;const m=d.monitor||{};
 $("mTraders").textContent=fmt(d.traders,0);$("mPositions").textContent=fmt(d.live_positions??0,0);$("mUnderwater").textContent=fmt(d.underwater_live_positions??0,0);$("mPaperOpen").textContent=fmt(d.open_paper_trades,0);
 $("mPositionsFoot").textContent=fmt(d.stale_visible_trader_snapshots??0,0)+" stale leader snapshots excluded · 15m freshness window";
 $("navTraderCount").textContent=fmt(d.traders,0);$("navPaperCount").textContent=fmt(d.open_paper_trades,0);$("navPositionCount").textContent=fmt(d.live_positions??0,0);$("topLeaderCount").textContent=fmt(d.live_positions??0,0);
 $("sideStatus").textContent=m.worker_alive?"Automatic monitor running":"Monitor starting / sleeping";$("sidePulse").className="pulse "+(m.worker_alive?"on":"");
 $("monitorBadge").textContent=m.state||"INITIALIZING";$("monitorBadge").className="pill "+(m.state==="idle"?"ok":m.state==="error"?"warn":"");
 $("lastScan").textContent=m.finished_at?date(m.finished_at):"Waiting for first scan";$("nextScan").textContent="Discovery every "+Math.round((m.discovery_interval_seconds||1800)/60)+" min · positions every "+Math.round((m.position_refresh_interval_seconds||300)/60)+" min · "+(m.position_batch||20)+" leaders per batch";
 $("stepDiscover").textContent=m.last_error||(m.finished_at?fmt(m.candidates_found??0,0)+" leaders checked · "+fmt(m.traders_found,0)+" qualified"+(m.gate_failures&&Object.keys(m.gate_failures).length?" · failed gates: "+Object.entries(m.gate_failures).map(([k,v])=>k+" "+v).join(", "):""):"Waiting for public Binance listing");$("stepDiscoverState").textContent=m.state==="error"?"ERROR":m.finished_at?"DONE":"WAIT";
 {const ss=d.scanner_summary||{};$("stepPositions").textContent=m.finished_at?"Of "+fmt(ss.qualified_leaders??0,0)+" qualified leaders: "+fmt(ss.leaders_position_checked??0,0)+" positions-checked · "+fmt(ss.leaders_with_live_positions??0,0)+" have live positions ("+fmt(ss.live_positions??0,0)+" positions)"+(ss.leaders_hidden_excluded?" · "+ss.leaders_hidden_excluded+" hidden excluded":""):"Rotation begins after leaderboard fetch";state.scannerSummary=ss;}$("stepPositionsState").textContent=m.state==="error"?"CHECK":m.finished_at?"DONE":"WAIT";
 $("stepPaper").textContent=d.paper?.auto_enabled?"Auto paper entries enabled when scanner confirms":"Paper entry automation paused in settings";$("stepPaperState").textContent=d.paper?.auto_enabled?"ACTIVE":"PAUSED";
 const copyStatus=d.binance_copy_trading?.last_status,copyOk=copyStatus?.ok===true;
 source("healthCopyTrading","copyTradingDot",copyStatus?copyOk:null,copyOk?"PUBLIC PAGE OK":copyStatus?"SOURCE DEGRADED":"WAITING FOR FIRST CHECK");$("copyTradingState").textContent=copyOk?"Public page OK":copyStatus?"Degraded":"Waiting";
 const pf=d.price_feed||{},restBlocked=!!pf.binance_rest?.blocked,priceAge=pf.last_price_at?Date.now()-new Date(pf.last_price_at).getTime():Infinity,feedOk=!!pf.connected&&priceAge<10000;
 const feedLabel=feedOk?(restBlocked?"WEBSOCKET LIVE · REST PAUSED":"WEBSOCKET LIVE"):(restBlocked?"REST PAUSED · WS RECOVERING":(pf.status==="idle_no_open_trades"?"IDLE · NO LEADER POSITIONS TO TRACK":"FEED STARTING"));
 source("healthBinance","binanceDot",feedOk,feedLabel);$("binanceState").textContent=feedLabel;
 {const kc=d.kopyon_calls||{};$("kopyonCallsChip").hidden=!kc.active;if(kc.active){$("kopyonCalls").textContent=fmt(kc.total,0)+" total · "+fmt(kc.last_24h,0)+" /24h · "+fmt(kc.last_30d,0)+" /30d"+(kc.limit_month?" · "+fmt(kc.remaining_month??0,0)+" of "+fmt(kc.limit_month,0)+" left":"")+(kc.failed?" · "+fmt(kc.failed,0)+" failed":"");}}
 const dbOk=d.database?.connection==="connected";$("healthDatabase").textContent=dbOk?(d.database.persistent?"POSTGRES":"SQLITE / EPHEMERAL"):"ERROR";$("healthDatabase").className="connection-state "+(dbOk?(d.database.persistent?"ok":""):"bad");
 $("healthDatabaseDesc").textContent=dbOk?(d.database.persistent?"Persistent PostgreSQL":"SQLite: data can be lost on spin-down/redeploy"):"Database unavailable";
 $("dbUsed").textContent=d.database?.used_mb!==null&&d.database?.used_mb!==undefined?fmt(d.database.used_mb,2)+" MB":"Not reported";$("dbCapacityNote").textContent=d.database?.capacity_note||"";
 $("dbBar").style.width=d.database?.configured_capacity_gb&&d.database?.used_mb!==null?Math.min(100,d.database.used_mb/(d.database.configured_capacity_gb*1024)*100)+"%":"0%";
 if(m.last_error)notice("Monitor error: "+m.last_error,"error");
 if(d.price_feed?.binance_rest?.blocked)notice("Binance REST is paused by the global rate-limit circuit breaker; live monitoring remains WebSocket-first.","error");
 if(!d.database?.persistent)notice("The running service is not connected to persistent PostgreSQL. Check DATABASE_URL on laes-data-proof-python.","error");
 if(state.page==="overview"){
  try{const p=await api("/api/paper");$("mPaperEquity").textContent=money(p.equity);$("mPaperPnl").innerHTML='Realized <b class="'+(Number(p.realized_pnl)>=0?"positive":"negative")+'">'+money(p.realized_pnl)+'</b> · Unrealized <b class="'+(Number(p.unrealized_pnl)>=0?"positive":"negative")+'">'+money(p.unrealized_pnl)+'</b>';}catch{}
 try{const o=await api("/api/opportunities?limit=100&strategy=RECOVERY");const q=o.filter(x=>x.qualified);$("mOpportunities").textContent=fmt(q.length,0);$("navOppCount").textContent=fmt(q.length,0);renderMini(o.slice(0,5));}catch(e){$("overviewOpps").innerHTML='<div class="empty-state">'+esc(e.message)+'</div>';}
 }
 }catch(e){notice("Dashboard status unavailable: "+e.message,"error");}
}
function renderMini(items){
 if(!items.length){$("overviewOpps").innerHTML='<div class="empty-state">No qualifying or underwater positions observed yet. Only live provider snapshots appear here.</div>';return;}
 $("overviewOpps").innerHTML=items.map(o=>'<div class="mini-opp"><div><b>'+esc(o.symbol)+' · '+esc(o.side)+' · '+esc(o.trader_name)+'</b><small>'+esc(o.status)+' · adverse '+pct(o.adverse_pct)+' · observed '+date(o.observed_at)+'</small></div><strong class="'+(Number(o.unrealized_pnl)>=0?"positive":"negative")+'">'+money(o.unrealized_pnl)+'</strong><span class="status-tag '+(o.qualified?"qualified":"")+'">'+(o.qualified?"PAPER READY":"WATCH")+'</span></div>').join("");
}
async function loadTraders(){
 $("traderGrid").innerHTML='<div class="empty-state">Loading discovered public trader records…</div>';
 try{const q=encodeURIComponent($("traderSearch").value||"");const rows=await api("/api/traders?limit=250&search="+q);$("traderListCount").textContent=rows.length+" qualified leaders · "+rows.filter(t=>t.live_positions>0).length+" with live positions";
 if(!rows.length){$("traderGrid").innerHTML='<div class="empty-state">No qualified public Binance leaders with a verified Positions response yet. Check source health or use Scan now.</div>';return;}
 $("traderGrid").innerHTML=rows.map(t=>{
 const vis=t.visibility||"unknown";const av=t.avatar_url?'<img class="avatar" src="'+esc(t.avatar_url)+'" alt="">':'<div class="avatar avatar-fallback">'+esc((t.name||"L").slice(0,1).toUpperCase())+'</div>';
 return '<article class="trader-card"><div class="trader-head">'+av+'<div class="trader-title"><b>'+esc(t.name||"Unnamed leader")+'</b><small>'+esc(t.trader_id)+'</small></div><span class="visibility '+(vis.includes("hidden")?"hidden":"")+'">'+(t.live_positions>0?esc(t.live_positions+" live position"+(t.live_positions>1?"s":"")):"No open positions")+'</span></div><div class="trader-stats"><div class="trader-stat"><small>30D ROI</small><b class="'+((t.roi||0)>=0?"positive":"negative")+'">'+pct((t.roi||0)*100)+'</b></div><div class="trader-stat"><small>MAX DD</small><b class="negative">'+pct((t.max_drawdown||0)*100)+'</b></div><div class="trader-stat"><small>COPIER PNL</small><b class="'+((t.copier_pnl||0)>=0?"positive":"negative")+'">'+money(t.copier_pnl)+'</b></div></div><div class="trader-profile-blurb">'+(t.bio?esc(t.bio):"Open profile to load details and disclosed history")+'</div><div class="trader-meta"><span>Win '+pct((t.win_rate||0)*100)+'</span><span>AUM '+money(t.aum)+'</span><span>Sharpe '+fmt(t.sharpe_ratio)+'</span><span>Copiers '+fmt(t.copiers,0)+'</span><span>Share '+(t.profit_share===null||t.profit_share===undefined?"—":pct(t.profit_share*100))+'</span></div><button class="btn btn-subtle" onclick="openTrader(\''+encodeURIComponent(t.trader_id)+'\')">Open forensic profile →</button> <a class="btn btn-subtle" href="'+esc(t.binance_url)+'" target="_blank" rel="noopener noreferrer">View on Binance ↗</a></article>';
 }).join("");
 }catch(e){$("traderGrid").innerHTML='<div class="empty-state">'+esc(e.message)+'</div>';}
}
bind("traderSearch","input",()=>{clearTimeout(loadTraders.timer);loadTraders.timer=setTimeout(loadTraders,250);});click("reloadTraders",loadTraders);
async function openTrader(enc){
 clearInterval(paperDetailTimer);paperDetailTimer=null;
 const id=decodeURIComponent(enc);$("dialogName").textContent="Loading trader…";$("dialogSub").textContent=id;$("dialogBody").innerHTML='<div class="empty-state">Loading public profile and position history from Binance…</div>';
 if(!$("traderDialog").open)$("traderDialog").showModal();
 try{
 const d=await api("/api/traders/"+encodeURIComponent(id)),t=d.trader,p=d.profile||{},hs=d.history_summary||{};$("dialogName").textContent=t.name||id;$("dialogSub").textContent=id+" · "+(t.portfolio_type||"Portfolio")+" · visibility: "+t.visibility;
 const metrics=[["ROI",pct((t.roi||0)*100)],["Max drawdown",pct((t.max_drawdown||0)*100)],["Copier PnL",money(t.copier_pnl)],["Sharpe",fmt(t.sharpe_ratio)],["AUM",money(t.aum)],["Profit share",pct((p.profitShare||0)*100)],["Copiers",fmt(p.copiers??t.raw?.copiers,0)],["Margin balance",p.sourceDetails?.marginBalance?money(p.sourceDetails.marginBalance):"Not reported"]];
 let html=p.bio?'<p class="profile-bio">'+esc(p.bio)+'</p>':"";
 html+='<div class="profile-grid">'+metrics.map(m=>{const isPnl=m[0]==="Copier PnL";const amount=parseFloat(String(m[1]).replace(/[^0-9+-.]/g,""));return '<div class="profile-metric"><small>'+esc(m[0])+'</small><b class="'+(isPnl?(amount>=0?"positive":"negative"):"")+'">'+esc(m[1])+'</b></div>';}).join("")+'</div>';
 html+='<div class="analysis-box"><h4>TRADER USUAL HOLDING BEHAVIOUR</h4><div class="analysis-line"><span>Completed history rows</span><b>'+fmt(hs.records,0)+'</b></div><div class="analysis-line"><span>Median holding time</span><b>'+duration(hs.median_hold_hours)+'</b></div><div class="analysis-line"><span>Average holding time</span><b>'+duration(hs.avg_hold_hours)+'</b></div><div class="analysis-line"><span>Closed-history win rate</span><b>'+pct(hs.win_rate_closed_history===null?null:hs.win_rate_closed_history*100)+'</b></div><div class="analysis-line"><span>Average reported PnL</span><b>'+money(hs.avg_reported_pnl)+'</b></div></div>';
 html+='<div class="panel-heading" style="margin-top:15px"><div><h3>Closed position history</h3><p>Fetched from Binance public Position History; reported PnL may not include all fees/funding, and history rows do not reveal historical MAE/MFE.</p></div></div><div class="profile-history"><table><thead><tr><th>SYMBOL</th><th>SIDE</th><th>OPENED</th><th>CLOSED</th><th>ROI</th><th>REPORTED PNL</th></tr></thead><tbody>';
 html+=d.history?.length?d.history.map(h=>'<tr><td>'+esc(h.symbol)+'</td><td>'+esc(h.side)+'</td><td>'+date(h.opened_at)+'</td><td>'+date(h.closed_at)+'</td><td>'+pct((h.roi||0)*100)+'</td><td class="'+((h.net_pnl||0)>=0?"positive":"negative")+'">'+money(h.gross_pnl)+'</td></tr>').join(""):'<tr><td colspan="6" class="empty-cell">No closed history returned.</td></tr>';
 $("dialogBody").innerHTML=html+'</tbody></table></div>';
 }catch(e){$("dialogBody").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}
}
window.openTrader=openTrader;click("closeDialog",()=>{clearInterval(paperDetailTimer);paperDetailTimer=null;$("traderDialog").close();});
async function loadPositions(){
 $("positionTable").innerHTML='<tr><td colspan="9" class="empty-cell">Loading real position observations…</td></tr>';
 try{const [rows,traders]=await Promise.all([api("/api/positions?limit=500"),api("/api/traders?limit=300")]);const names=Object.fromEntries(traders.map(t=>[t.trader_id,t.name||t.trader_id]));
 if(!rows.length){$("positionTable").innerHTML='<tr><td colspan="9" class="empty-cell">No public position observations have been saved. Hidden positions remain unavailable.</td></tr>';return;}
 $("positionTable").innerHTML=rows.map(p=>{const a=p.entry_price&&p.mark_price?(p.side==="LONG"?(p.entry_price-p.mark_price):(p.mark_price-p.entry_price))/p.entry_price*100:null;const age=p.snapshot_age_seconds===null||p.snapshot_age_seconds===undefined?"":(p.snapshot_age_seconds<3600?Math.round(p.snapshot_age_seconds/60)+"m old":(p.snapshot_age_seconds/3600).toFixed(1)+"h old");return '<tr><td>'+esc(names[p.trader_id]||p.trader_id)+'<br><small>'+esc(p.trader_id)+'</small></td><td><b>'+esc(p.symbol)+'</b></td><td class="'+(p.side==="LONG"?"side-long":"side-short")+'">'+esc(p.side)+'</td><td>'+fmt(p.entry_price,6)+'</td><td>'+fmt(p.mark_price,6)+'</td><td class="'+((p.unrealized_pnl||0)<0?"negative":"positive")+'">'+money(p.unrealized_pnl)+'</td><td class="'+((a||0)>0?"negative":"positive")+'">'+pct(a)+'</td><td>'+(p.leverage?fmt(p.leverage,1)+"×":"—")+' '+esc(p.margin_mode||"")+'</td><td><span class="status-tag '+(p.freshness==="FRESH"?"qualified":"neutral")+'">'+esc(p.freshness||"UNKNOWN")+'</span><br>'+date(p.observed_at)+'<br><small>'+esc(age)+'</small></td></tr>';}).join("");
 }catch(e){$("positionTable").innerHTML='<tr><td colspan="9" class="empty-cell">'+esc(e.message)+'</td></tr>';}
}
click("reloadPositions",loadPositions);
async function loadPipelineDecisions(){
 const body=$("opportunityDecisionTable");if(!body)return;
 body.innerHTML='<tr><td colspan="6" class="empty-cell">Loading persisted qualification decisions…</td></tr>';
 try{
  const strategy=$("opportunityStrategy")?.value||"RECOVERY";const rows=await api("/api/opportunity-decisions?limit=150&strategy="+encodeURIComponent(strategy));
  body.innerHTML=rows.length?rows.map(r=>{
   const checks=Object.entries(r.checks||{}).map(([k,v])=>k+": "+(v===null?"unknown":typeof v==="object"?JSON.stringify(v):String(v))).join("\n");
   const reasons=(r.reasons||[]).join(" · ")||"No rejection reason; candidate passed the recorded gates.";
   return '<tr><td>'+date(r.observed_at)+'</td><td>'+esc(r.strategy)+'</td><td><b>'+esc(r.symbol)+'</b> '+esc(r.side)+'</td><td><span class="status-tag '+(r.qualified?"qualified":"neutral")+'">'+(r.qualified?"QUALIFIED":"NOT QUALIFIED")+'</span><br><small>'+esc(r.status)+'</small></td><td>'+esc(r.execution_status)+(r.paper_trade_id?' · #'+r.paper_trade_id:'')+'</td><td><details><summary>'+esc(reasons)+'</summary><pre style="white-space:pre-wrap;max-width:520px">'+esc(checks)+'</pre></details></td></tr>';
  }).join(""):'<tr><td colspan="6" class="empty-cell">No pipeline decisions stored yet. They will appear as real leader snapshots are scanned.</td></tr>';
 }catch(e){body.innerHTML='<tr><td colspan="6" class="empty-cell">'+esc(e.message)+'</td></tr>';}
}
click("reloadPipelineDecisions",loadPipelineDecisions);

async function loadOpportunities(){
 const strategy=$("opportunityStrategy")?.value||"RECOVERY";
 $("opportunityList").innerHTML='<div class="empty-state">Loading '+esc(strategy)+' candidates…</div>';
 try{
  const items=await api("/api/opportunities?limit=100&strategy="+encodeURIComponent(strategy));
  const qual=items.filter(o=>o.qualified);{const ss=state.scannerSummary||(state.overview&&state.overview.scanner_summary)||{};$("scannerSummary").textContent="From "+fmt(ss.qualified_leaders??0,0)+" qualified leaders: "+fmt(ss.leaders_position_checked??0,0)+" positions checked · "+fmt(ss.leaders_with_live_positions??0,0)+" have live positions ("+fmt(ss.live_positions??0,0)+" positions) · "+fmt(qual.length,0)+" opportunities qualified"+(ss.leaders_hidden_excluded?" · "+ss.leaders_hidden_excluded+" hidden-position leaders excluded":"");}$("navOppCount").textContent=fmt(qual.length,0);$("topOppCount").textContent=fmt(items.length,0);
  if(!items.length){$("opportunityList").innerHTML='<div class="empty-state">No observed positions yet for this strategy. The scanner does not create hypothetical opportunities.</div>';return;}
  $("opportunityList").innerHTML=items.map(o=>{
   const isMomentum=o.strategy==="MOMENTUM",rec=o.recovery||{},hold=o.holding||{};
   const causes=[...(o.likely_causes||[]),...((o.rejection_reasons&&o.rejection_reasons.length)?o.rejection_reasons:(o.dead_market_reasons||[])).map(reason=>({label:'ENTRY BLOCKED',effect:reason,risk:'adverse'}))];
   let evidence;
   if(isMomentum){evidence='<div class="analysis-line"><span>Leader profitable</span><b>'+(o.leader_profitable?'Yes':'No')+'</b></div><div class="analysis-line"><span>Profit from entry</span><b>'+pct(o.leader_profit_pct)+'</b></div><div class="analysis-line"><span>Directional 5m confirmation</span><b>'+(o.momentum_confirmed?'Confirmed':'Not confirmed')+'</b></div><div class="sample-warning">'+esc(o.entry_model||'Momentum uses a separate profitable-leader rule.')+'</div>';}
   else if(rec.probability===null||rec.probability===undefined){evidence='<div class="recovery-prob">N/A</div><div class="sample-warning">'+esc(rec.label||"Insufficient history")+' · sample '+fmt(rec.sample_size,0)+'</div>';}
   else{evidence='<div class="recovery-prob">'+fmt(rec.probability,1)+'%</div><div class="analysis-line"><span>Wilson 95% interval</span><b>'+fmt(rec.ci_low,1)+'% – '+fmt(rec.ci_high,1)+'%</b></div><div class="analysis-line"><span>Comparable paths</span><b>'+fmt(rec.sample_size,0)+'</b></div><div class="analysis-line"><span>Trader typical median hold</span><b>'+duration(hold.typical_median_hours)+'</b></div><div class="sample-warning">'+esc(rec.note||"")+'</div>';}
   const causeHtml=causes.length?causes.map(c=>'<div class="cause '+esc(c.risk||"")+'">'+esc(c.label)+' · '+esc(c.effect)+(c.url?' · <a href="'+esc(c.url)+'" target="_blank" rel="noreferrer">source</a>':"")+'</div>').join(""):'<div class="cause">No related headline or market cause flagged.</div>';
   const pnlClass=Number(o.unrealized_pnl||0)>=0?'positive':'negative';
   const moveLabel=isMomentum?'PROFIT FROM ENTRY':'ADVERSE FROM ENTRY';
   const moveValue=isMomentum?o.leader_profit_pct:o.adverse_pct;
   return '<article class="opportunity-card"><div class="opp-head"><div class="opp-title"><div><h3>'+esc(o.symbol)+' <span class="'+(o.side==="LONG"?"side-long":"side-short")+'">'+esc(o.side)+'</span></h3><small>'+esc(o.strategy)+' · '+esc(o.trader_name)+' · '+esc(o.trader_id)+' · seen '+date(o.observed_at)+'</small></div></div><span class="status-tag '+(o.qualified?"qualified":(o.underwater||o.leader_profitable)?"":"neutral")+'">'+esc(o.status)+'</span></div><div class="opp-stats"><div class="opp-stat"><small>LEADER PNL</small><b class="'+pnlClass+'">'+money(o.unrealized_pnl)+'</b></div><div class="opp-stat"><small>'+moveLabel+'</small><b class="'+(isMomentum?"positive":"negative")+'">'+pct(moveValue)+'</b></div><div class="opp-stat"><small>ENTRY / CURRENT</small><b>'+fmt(o.entry_price,5)+' / '+fmt(o.mark_price,5)+'</b></div><div class="opp-stat"><small>HOLD TIME</small><b>'+duration(hold.current_hours)+'</b></div><div class="opp-stat"><small>5M MOMENTUM</small><b class="'+((o.momentum_5m_pct||0)>=0?"positive":"negative")+'">'+pct(o.momentum_5m_pct)+'</b></div>'+(isMomentum?'':'<div class="opp-stat"><small>RECOVERY SCORE</small><b>'+(o.recovery_score!=null?fmt(o.recovery_score,0)+'/100'+(o.est_recovery_minutes!=null?' · ~'+fmt(o.est_recovery_minutes,0)+' min':''):'—')+'</b></div><div class="opp-stat"><small>REVERSAL (1M / BOUNCE)</small><b class="'+(o.reversal_confirmed?"positive":"")+'">'+(o.reversal_confirmed?'✓ ':'')+pct(o.momentum_1m_pct)+' / '+pct(o.bounce_off_5m_extreme_pct)+'</b></div>')+'</div><div class="opp-bottom"><div class="analysis-box"><h4>'+(isMomentum?'MOMENTUM EVIDENCE':'RECOVERY EVIDENCE')+'</h4>'+evidence+'</div><div class="analysis-box"><h4>MARKET / EVENT CONTEXT</h4><div class="cause-list">'+causeHtml+'</div><div class="sample-warning">'+esc(o.recovery_warning||"")+'</div></div></div></article>';
  }).join("");
 }catch(e){$("opportunityList").innerHTML='<div class="empty-state">'+esc(e.message)+'</div>';}
}
click("refreshOpps",loadOpportunities);
bind("opportunityStrategy","change",()=>{loadOpportunities();loadPipelineDecisions();});
async function loadMomentumPaper(){
 try{
  const d=await api("/api/paper?strategy=MOMENTUM"); state.momentumData=d;
  const open=d.trades.filter(t=>t.status==="OPEN"),closed=d.trades.filter(t=>t.status==="CLOSED"),walletOpen=open.filter(t=>t.wallet_epoch_eligible);
  $("momentumRealized").textContent=money(d.realized_pnl);
  $("momentumUnrealized").textContent=money(d.unrealized_pnl);
  $("momentumCounts").textContent=walletOpen.length+" wallet-open / "+closed.length+" closed in ledger";
  $("navMomentumCount").textContent=walletOpen.length;
  const audit=d.recovery_audit||{};
  $("momentumAudit").textContent=fmt(audit.complete||0,0)+" / "+fmt((audit.complete||0)+(audit.pending||0),0);
  const pnl=v=>'<span class="'+(Number(v||0)>=0?"positive":"negative")+'">'+money(v)+'</span>';
  $("momentumTrades").innerHTML=d.trades.length?d.trades.map(t=>{
    const timing=t.entry_timing_improvement_pct===null||t.entry_timing_improvement_pct===undefined?"Pending":fmt(t.entry_timing_improvement_pct,3)+"% hindsight";
    return '<tr class="clickable-row" onclick="openPaperTrade('+t.id+')"><td><span class="status-tag '+(t.status==="OPEN"?"positive":"neutral")+'">'+esc(t.status)+'</span>'+(t.wallet_epoch_eligible?'':'<br><small>LEGACY · outside $200 wallet</small>')+'</td><td><b>'+esc(t.trader_name||t.trader_id)+'</b><br><small>'+esc(t.leader_position_key||"")+'</small></td><td><b>'+esc(t.symbol)+'</b><br>'+esc(t.side)+'</td><td><b>'+fmt(t.entry_price,8)+'</b><br><small>'+date(t.opened_at)+'</small>'+(t.status==="CLOSED"?'<br><b>'+fmt(t.exit_price,8)+'</b><br><small>'+date(t.closed_at)+'</small>':'')+'</td><td>'+pnl(t.net_pnl)+'<br><small>MFE '+pnl(t.mfe_net_pnl)+' · MAE '+pnl(t.mae_net_pnl)+'</small></td><td>'+esc(timing)+'<br><small>'+esc(t.entry_timing_verdict||"Not audited")+'</small></td><td>'+esc(t.exit_reason_display||"Open")+'<br><small>'+esc(t.post_exit_recovery_label||"Recovery audit pending")+'</small></td><td><button class="btn btn-subtle" type="button" onclick="event.stopPropagation();openPaperTrade('+t.id+')">Forensics</button></td></tr>';
  }).join(""):'<tr><td colspan="8" class="empty-cell">No profitable-leader momentum paper trades have qualified yet.</td></tr>';
  const audited=closed.filter(t=>t.entry_timing_improvement_pct!==null&&t.entry_timing_improvement_pct!==undefined);
  const material=audited.filter(t=>Number(t.entry_timing_improvement_pct)>0.05);
  $("momentumEntryAudit").innerHTML='<div class="analysis-line"><span>Closed momentum trades</span><b>'+fmt(closed.length,0)+'</b></div><div class="analysis-line"><span>Trades with verified pre-entry candle window</span><b>'+fmt(audited.length,0)+'</b></div><div class="analysis-line"><span>Material hindsight better-entry signals (&gt;0.05%)</span><b>'+fmt(material.length,0)+'</b></div><div class="sample-warning">This is hindsight-only. It cannot prove the scanner could have known the future low/high or obtained that exact fill.</div>';
 }catch(e){notice("Momentum paper ledger: "+e.message,"error");}
}
async function loadPaper(){
 if(paperLoadInFlight)return;paperLoadInFlight=true;
 try{
  const d=await api("/api/paper");state.paperData=d;
  const open=d.trades.filter(t=>t.status==="OPEN"),closed=d.trades.filter(t=>t.status==="CLOSED"),walletOpen=open.filter(t=>t.wallet_epoch_eligible);
  $("paperCapital").textContent=money(d.capital);$("paperMargin").textContent=money(d.deployed_margin)+" / "+money(d.margin_cap);
  $("paperRealized").textContent=money(d.realized_pnl);$("paperUnrealized").textContent=money(d.unrealized_pnl);$("paperFree").textContent=money(d.free_capital);$("paperEquity").textContent=money(d.equity);
  $("paperRealized").classList.toggle("positive",Number(d.realized_pnl)>=0);$("paperRealized").classList.toggle("negative",Number(d.realized_pnl)<0);
  $("paperUnrealized").classList.toggle("positive",Number(d.unrealized_pnl)>=0);$("paperUnrealized").classList.toggle("negative",Number(d.unrealized_pnl)<0);
  $("topOpenCount").textContent=walletOpen.length;$("paperOpenTableCount").textContent=open.length;["topClosedCount","paperClosedTableCount"].forEach(id=>$(id).textContent=closed.length);$("navPaperCount").textContent=walletOpen.length;
  const audit=d.recovery_audit||{};
  if($("recoveryAuditStatus")){
   $("recoveryAuditStatus").textContent=audit.blocked?"Audit paused · Binance REST cooldown "+duration((audit.remaining_seconds||0)/3600):
    fmt(audit.complete||0,0)+" candle audits complete · "+fmt(audit.pending||0,0)+" pending · "+fmt(audit.engine_replay_complete||0,0)+" exit-engine replays stored / "+fmt(audit.engine_replay_pending||0,0)+" pending";
  }
  renderPaperTables(d);
  $("paperRiskClusters").innerHTML=(d.risk_clusters||[]).map(x=>'<div class="analysis-line"><span>'+esc(x.symbol)+' '+esc(x.side)+'</span><b>'+fmt(x.count,0)+' positions · '+money(x.net_pnl)+'</b></div>').join("")||'<div class="empty-state">No overlapping symbol/direction cluster currently needs attention.</div>';
  $("paperShadowPortfolio").innerHTML=renderShadowPortfolio(d.shadow_portfolio);
 }catch(e){notice("Paper ledger: "+e.message,"error")}finally{paperLoadInFlight=false}
}
function renderPaperTables(d){
 if(!d)return;
 const open=d.trades.filter(t=>t.status==="OPEN"),closed=d.trades.filter(t=>t.status==="CLOSED");
 function pnl(v){return '<span class="'+(Number(v||0)>=0?"positive":"negative")+'">'+money(v)+'</span>';}
 function leaderCheck(t){
  const status=t.leader_status||"NOT_CHECKED";
  const labels={OPEN_OBSERVED:"OPEN · exact key observed",CLOSED_CONFIRMED:"CLOSED · confirmed",REVERSED:"REVERSED",STALE_OR_UNAVAILABLE:"STALE / unavailable",HIDDEN_OR_UNAVAILABLE:"HIDDEN / unavailable",NOT_CHECKED:"No stored check"};
  const cls=status==="OPEN_OBSERVED"?"positive":status==="REVERSED"||status==="CLOSED_CONFIRMED"?"negative":"";
  const at=t.leader_check_at||t.leader_state_checked_at||t.last_leader_check_at;
  return '<b class="'+cls+'">'+esc(labels[status]||status)+'</b><br><small>'+esc(at?date(at):"Timestamp not stored")+'</small><br><small>'+esc(t.leader_state_detail||t.leader_check_source||"")+'</small>';
 }
 function row(t,isClosed){
  const rec=t.post_exit_recovery_label||(t.post_exit_recovery_audit_status==="COMPLETE"?"Audit complete":"Recovery audit pending");
  const pnlCell=pnl(t.net_pnl);
  return '<tr class="clickable-row" onclick="openPaperTrade('+t.id+')"><td><span class="status-tag '+(isClosed?"neutral":"positive")+'">'+esc(t.status)+'</span>'+(t.wallet_epoch_eligible?'':'<br><small>LEGACY · outside $200 wallet</small>')+'</td><td><b>'+esc(t.trader_name||t.trader_id)+'</b><br><small>'+esc(t.leader_position_key||"unlinked")+'</small></td><td><b>'+esc(t.symbol)+'</b></td><td>'+esc(t.side)+'</td><td><b>'+fmt(t.entry_price,8)+'</b><br><small>Entry: '+date(t.opened_at)+'</small>'+(isClosed?"<br><b>Exit: "+fmt(t.exit_price,8)+'</b><br><small>Exit time: '+date(t.closed_at)+'</small>':"<br><small>Live mark: "+fmt(t.mark_price,8)+"</small>")+'</td><td>'+pnlCell+'<br><small>MFE '+pnl(t.mfe_net_pnl)+' · MAE '+pnl(t.mae_net_pnl)+'</small></td><td>'+duration((new Date(isClosed?t.closed_at:Date.now())-new Date(t.opened_at))/3600000)+'</td>'+(isClosed?'<td>'+esc(t.exit_reason_display||t.exit_reason||"—")+'</td><td><b>'+esc(rec)+'</b><br><small>'+esc(t.post_exit_recovery_audit_status||"PENDING")+' · exit replay '+esc((t.new_exit_engine_audit||{}).replay_status||"PENDING")+'</small></td>':'<td>'+leaderCheck(t)+'</td><td>'+esc(t.data_quality_status||"—")+'</td>')+'<td><button class="btn btn-subtle" type="button" onclick="event.stopPropagation();openPaperTrade('+t.id+')">Forensics</button></td></tr>';
 }
 $("paperOpenTrades").innerHTML=open.length?open.map(t=>row(t,false)).join(""):'<tr><td colspan="10" class="empty-cell">No open paper positions.</td></tr>';
 const profitable=closed.filter(t=>Number(t.net_pnl||0)>0),losses=closed.filter(t=>Number(t.net_pnl||0)<0);
 $("closedAllCount").textContent=closed.length;$("closedProfitCount").textContent=profitable.length;$("closedLossCount").textContent=losses.length;
 const chosen=state.closedFilter==="profitable"?profitable:state.closedFilter==="loss"?losses:closed;
 $("paperClosedTrades").innerHTML=chosen.length?chosen.map(t=>row(t,true)).join(""):'<tr><td colspan="10" class="empty-cell">No trades in this closed-trade category.</td></tr>';
 const latest=[...d.trades].sort((a,b)=>new Date(b.opened_at)-new Date(a.opened_at)).slice(0,12);
 $("paperLatestTrades").innerHTML=latest.length?latest.map(t=>row(t,t.status==="CLOSED")).join(""):'<tr><td colspan="10" class="empty-cell">No trades are recorded.</td></tr>';
 document.querySelectorAll("[data-closed-filter]").forEach(x=>x.classList.toggle("active",x.dataset.closedFilter===state.closedFilter));
 const tab=state.paperTab||(open.length===0&&closed.length>0?"closed":"open");state.paperTab=tab;
 document.querySelectorAll(".paper-table-tab,.top-live-btn").forEach(x=>x.classList.toggle("active",x.dataset.paperTab===tab));
 $("paperOpenTable").style.display=tab==="open"?"":"none";
 $("paperClosedTable").style.display=tab==="closed"?"":"none";
 $("paperLatestTable").style.display=tab==="latest"?"":"none";
 $("closedTradeFilters").style.display=tab==="closed"?"":"none";
}
function setClosedTradeFilter(filter){
 if(!["all","profitable","loss"].includes(filter))return;
 state.closedFilter=filter;
 renderPaperTables(state.paperData);
}
function switchPaperTab(tab){
 if(!["open","closed","latest"].includes(tab))return;
 if(state.page!=="paper")setPage("paper",{scroll:false});
 state.paperTab=tab;
 document.querySelectorAll(".paper-table-tab,.top-live-btn").forEach(x=>x.classList.toggle("active",x.dataset.paperTab===tab));
 if(state.paperData)renderPaperTables(state.paperData);
}
async function runRecoveryAudit(){
 const button=$("runRecoveryAudit");if(button){button.disabled=true;button.textContent="Auditing…";}
 try{
  const result=await api("/api/paper/audit/recovery",{method:"POST"});
  const status=result.blocked?"Binance REST cooldown · "+duration((result.remaining_seconds||0)/3600):
   "Candles audited "+fmt(result.audited,0)+" / attempted "+fmt(result.attempted,0)+" · "+fmt(result.pending,0)+" pending · exit-engine rows persisted "+fmt(result.engine_replay_persisted||0,0);
  $("recoveryAuditStatus").textContent=status;
  notice(result.blocked?"Historical market audit is paused by Binance's active REST ban; it was not bypassed.":status,result.failures?"error":"success");
  await loadPaper();
 }catch(e){notice("Recovery audit: "+e.message,"error");}
 finally{if(button){button.disabled=false;button.textContent="Run next historical audit batch";}}
}
async function loadResearchReplay(){
 const target=$("researchReplaySummary");if(!target)return;
 target.innerHTML='<div class="empty-state">Replaying stored leader observations…</div>';
 try{
  const d=await api("/api/paper/research-replay");state.researchReplay=d;
  const x=d.summary||{};
  const val=(label,value)=>'<div class="replay-metric"><small>'+esc(label)+'</small><b class="'+(Number(value||0)>=0?"positive":"negative")+'">'+money(value)+'</b></div>';
  const stat=(label,value)=>'<div class="replay-stat"><small>'+esc(label)+'</small><b>'+fmt(value,0)+'</b></div>';
  target.innerHTML='<div class="replay-comparison">'+
   '<section class="replay-side"><h4>RESEARCH REALIZED ACCORDING TO OLD SYSTEM</h4>'+val("Recorded old-policy realized net",x.old_system_realized_net_pnl)+
   '<div class="replay-stats">'+stat("Closed trades",x.old_system_closed_trades)+stat("Profitable",x.old_system_profitable_trades)+stat("Loss-making",x.old_system_losing_trades)+'</div></section>'+
   '<section class="replay-side"><h4>COUNTERFACTUAL POLICY-EXIT NET ESTIMATE · NOT REALIZED</h4>'+val("Simulated policy exits · estimated net",x.new_policy_exit_net_pnl_estimate)+
   '<div class="replay-stats">'+stat("Closed trades in cohort",x.old_system_closed_trades)+stat("Simulated policy exits",x.new_policy_exits_simulated)+stat("Cost-priced exits",x.new_policy_exits_with_cost_estimate)+stat("Positive estimates",x.new_policy_profitable_exit_estimates)+stat("Negative estimates",x.new_policy_losing_exit_estimates)+stat("Still open / unrealized",x.new_policy_open_unrealized_count)+stat("Awaiting v9 candle audit",x.audit_pending_trades)+stat("Missing entry-to-exit paths",x.missing_verified_candle_trades)+stat("Incomplete paths",x.incomplete_entry_to_exit_candle_trades)+stat("Ambiguous 1m OHLC ordering",x.ambiguous_intrabar_ordering_trades)+stat("No valid candles",x.no_valid_candle_trades)+'</div>'+
   
   '<div class="analysis-line"><span>Open positions · mark-to-last unrealized</span><b class="'+(Number(x.new_policy_open_unrealized_net_estimate)>=0?"positive":"negative")+'">'+money(x.new_policy_open_unrealized_net_estimate)+'</b></div></section></div>'+
   '<div class="profile-history"><table><thead><tr><th>TRADE</th><th>SYMBOL</th><th>OLD NET</th><th>NEW POLICY NET ESTIMATE</th><th>NEW EXIT VS OLD EXIT</th><th>NEW EXIT / STATUS</th></tr></thead><tbody>'+
   (d.trades||[]).slice().sort((a,b)=>b.trade_id-a.trade_id).slice(0,20).map(t=>'<tr><td>#'+t.trade_id+'</td><td>'+esc(t.symbol)+' '+esc(t.side)+'</td><td class="'+(Number(t.old_realized_net_pnl)>=0?"positive":"negative")+'">'+money(t.old_realized_net_pnl)+'</td><td class="'+(Number(t.new_policy_net_pnl_estimate)>=0?"positive":"negative")+'">'+(t.new_policy_net_pnl_estimate===null||t.new_policy_net_pnl_estimate===undefined?"—":money(t.new_policy_net_pnl_estimate))+'</td><td>'+(t.new_exit_offset_from_old_exit_hours===null||t.new_exit_offset_from_old_exit_hours===undefined?"—":fmt(t.new_exit_offset_from_old_exit_hours,2)+"h vs old")+'</td><td>'+esc(t.new_exit_reason||t.replay_status||"—")+'<br><small>'+esc(t.new_exit_at||t.last_observed_at||"")+'</small></td></tr>').join("")+
   '</tbody></table></div>';
  $("researchReplayAssumptions").textContent=[d.source,x.exit_policy,x.sizing_basis,x.execution_caveat,x.cost_assumption,x.funding_assumption].filter(Boolean).join(" · ");
  await loadCapitalReplay();
 }catch(e){target.innerHTML='<div class="notice error">Research replay: '+esc(e.message)+'</div>';}
}
async function loadCapitalReplay(){
 const target=$("capitalReplaySummary");if(!target)return;
 target.innerHTML='<div class="empty-state">Recalculating shared-wallet allocations in chronological order…</div>';
 try{
  const d=await api("/api/paper/capital-replay"),x=d.summary||{};
  const metric=(label,value)=>'<div class="replay-stat"><small>'+esc(label)+'</small><b>'+esc(value===null||value===undefined?"—":String(value))+'</b></div>';
  const pnl=(label,value)=>'<div class="replay-metric"><small>'+esc(label)+'</small><b class="'+(Number(value||0)>=0?"positive":"negative")+'">'+money(value)+'</b></div>';
  target.innerHTML='<div class="replay-comparison"><section class="replay-side"><h4>WALLET OUTCOME · COUNTERFACTUAL</h4>'+pnl("Realized net estimate",x.counterfactual_realized_net_pnl)+pnl("Unrealized at last candle",x.unrealized_net_pnl_at_last_candle)+'<div class="replay-stats">'+metric("Starting wallet",money(x.capital_usdt))+metric("Final equity incl. open marks",money(x.final_equity_including_open_mark_estimate))+metric("Accepted trades",x.accepted_trades)+metric("Still open",x.still_open_unrealized_positions)+metric("Wins / losses",(x.closed_wins||0)+" / "+(x.closed_losses||0))+metric("Profit factor",x.realized_profit_factor??"—")+'</div></section><section class="replay-side"><h4>CAPITAL UTILIZATION</h4><div class="replay-stats">'+metric("Total margin deployed",money(x.total_margin_deployed_across_trades))+metric("Total notional deployed",money(x.total_notional_deployed_across_trades))+metric("Peak simultaneous margin",money(x.peak_simultaneous_margin))+metric("Peak wallet utilization",fmt(x.peak_wallet_utilization_pct,2)+"%")+metric("Realized equity max drawdown",money(x.max_realized_equity_drawdown_usdt)+" · "+fmt(x.max_realized_equity_drawdown_pct,2)+"%")+metric("Margin-hours",fmt(x.margin_hours,2))+metric("Average deployed margin",money(x.average_margin_over_observation_period))+metric("Max concurrent positions",x.maximum_simultaneous_positions)+'</div></section></div><div class="replay-stats">'+metric("Skipped: insufficient shared wallet",x.skipped_insufficient_shared_wallet)+metric("Skipped: same coin / direction overlap",x.skipped_same_symbol_direction_overlap)+metric("Unknown / missing candle audit",x.rejected_or_unknown_candle_audit)+metric("Legacy open positions excluded",x.legacy_open_positions_excluded_from_historical_closed_trade_replay)+metric("Legacy open margin excluded",money(x.legacy_open_recorded_margin_excluded))+'</div><div class="profile-history"><table><thead><tr><th>TRADE</th><th>STRATEGY</th><th>SYMBOL</th><th>MARGIN</th><th>NOTIONAL</th><th>NEW ENGINE NET</th><th>STATUS</th></tr></thead><tbody>'+(d.trades||[]).slice(0,50).map(t=>'<tr><td>#'+t.trade_id+'</td><td>'+esc(t.strategy)+'</td><td>'+esc(t.symbol)+' '+esc(t.side)+'</td><td>'+money(t.margin_used)+'</td><td>'+money(t.notional)+'</td><td class="'+(Number(t.new_engine_net_pnl_estimate)>=0?"positive":"negative")+'">'+money(t.new_engine_net_pnl_estimate)+'</td><td>'+esc(t.pnl_status)+'</td></tr>').join("")+'</tbody></table></div><details class="profile-history"><summary>Skipped entries ('+(d.skipped_trades||[]).length+') and unknown candle audits ('+(d.unknown_or_incomplete_trades||[]).length+')</summary><table><thead><tr><th>TRADE</th><th>STRATEGY</th><th>SYMBOL</th><th>REASON</th></tr></thead><tbody>'+[...(d.skipped_trades||[]),...(d.unknown_or_incomplete_trades||[])].slice(0,200).map(t=>'<tr><td>#'+t.trade_id+'</td><td>'+esc(t.strategy)+'</td><td>'+esc(t.symbol)+' '+esc(t.side||'')+'</td><td>'+esc(t.reason)+'</td></tr>').join('')+'</tbody></table></details><div class="muted-note">'+esc(x.method||"")+' · '+esc(x.fee_slippage_funding||"")+'</div>';
 }catch(e){target.innerHTML='<div class="notice error">Shared-wallet replay: '+esc(e.message)+'</div>';}
}
click("loadCapitalReplay",loadCapitalReplay);

async function loadStrategyReplay(){
 const target=$("strategyReplaySummary");if(!target)return;
 target.innerHTML='<div class="empty-state">Rehydrating audited candles if needed, then replaying revised strategy cycles…</div>';
 const button=$("loadStrategyReplay");if(button){button.disabled=true;button.textContent="Replaying…";}
 try{
  const d=await api("/api/paper/strategy-replay"),x=d.summary||{},r=d.rehydration||{};
  const metric=(label,value)=>'<div class="replay-stat"><small>'+esc(label)+'</small><b>'+esc(value===null||value===undefined?"—":String(value))+'</b></div>';
  const pnl=(label,value)=>'<div class="replay-metric"><small>'+esc(label)+'</small><b class="'+(Number(value||0)>=0?"positive":"negative")+'">'+money(value)+'</b></div>';
  const rows=(d.trades||[]).slice().sort((a,b)=>Number(b.trade_id)-Number(a.trade_id));
  target.innerHTML='<div class="replay-comparison"><section class="replay-side"><h4>REVISED POLICY · COUNTERFACTUAL ONLY</h4>'+pnl("Realized cycle PnL estimate",x.revised_policy_realized_net_estimate)+pnl("Open at last audited candle",x.revised_policy_open_unrealized_net_estimate)+pnl("Total including open estimate",x.revised_policy_total_including_open_estimate)+'<div class="replay-stats">'+metric("Closed trades",x.closed_trade_cohort)+metric("Replayable",x.replayable_trades)+metric("Completed cycles",x.completed_cycles)+metric("Re-entries",x.reentries)+metric("Awaiting candle rehydration",x.pending_candle_rehydration)+metric("Incomplete entry paths",x.incomplete_entry_path)+'</div></section><section class="replay-side"><h4>DATA / REHYDRATION</h4><div class="replay-stats">'+metric("Candle batches fetched",r.audited)+metric("Fetch attempts",r.attempted)+metric("Audit pending",r.pending)+metric("Binance REST blocked",r.blocked?"Yes":"No")+'</div><div class="analysis-line"><span>Recorded old-system realized net (comparison)</span><b>'+money(x.old_system_realized_net_pnl)+'</b></div></section></div><div class="profile-history"><table><thead><tr><th>TRADE</th><th>STRATEGY</th><th>SYMBOL</th><th>CYCLES / RE-ENTRIES</th><th>REALIZED ESTIMATE</th><th>OPEN ESTIMATE</th><th>EXIT CYCLES / STATUS</th></tr></thead><tbody>'+rows.slice(0,60).map(t=>'<tr><td>#'+esc(t.trade_id)+'</td><td>'+esc(t.strategy||"—")+'</td><td>'+esc(t.symbol||"—")+' '+esc(t.side||"")+'</td><td>'+fmt(t.completed_cycles,0)+' / '+fmt(t.reentry_count,0)+'</td><td class="'+(Number(t.revised_policy_realized_net_estimate||0)>=0?"positive":"negative")+'">'+money(t.revised_policy_realized_net_estimate)+'</td><td class="'+(Number(t.open_unrealized_net_estimate||0)>=0?"positive":"negative")+'">'+money(t.open_unrealized_net_estimate)+'</td><td>'+((t.cycles||[]).map(c=>'<b>'+esc(c.exit_reason||"OPEN")+'</b> · '+money(c.net_pnl_estimate)+'<br><small>'+esc(c.exit_at||c.last_observed_at||"—")+'</small>').join('<br>')||esc(t.replay_status||"—"))+'</td></tr>').join("")+'</tbody></table></div>';
  $("strategyReplayAssumptions").textContent=[d.source,x.caveat,"Funding, fees and slippage are estimated according to audit coverage; 1m OHLC wicks are not treated as fills.","Rehydration: "+(r.status||"not needed")].filter(Boolean).join(" · ");
 }catch(e){target.innerHTML='<div class="notice error">Revised strategy replay: '+esc(e.message)+'</div>';}
 finally{if(button){button.disabled=false;button.textContent="Replay existing trades";}}
}
click("loadStrategyReplay",loadStrategyReplay);

function paperChartSvg(d){
 const candles=d.candles||[], path=d.path||[], leaderPath=d.leader_path||[], t=d.trade||{}, l=d.leader||{};
 const ts=v=>{const x=typeof v==="number"?v:Date.parse(v||"");return Number.isFinite(x)?x:null;};
 const times=[];
 candles.forEach(c=>{if(ts(c.open_time)!==null)times.push(ts(c.open_time));if(ts(c.close_time)!==null)times.push(ts(c.close_time));});
 path.forEach(p=>{if(ts(p.at)!==null)times.push(ts(p.at));});
 leaderPath.forEach(p=>{if(ts(p.at)!==null)times.push(ts(p.at));});
 [t.opened_at,t.closed_at,t.last_binance_price_at].forEach(v=>{if(ts(v)!==null)times.push(ts(v));});
 if(!times.length)return '<div class="empty-state">No timestamped market candles or trade path are available for this instrument.</div>';
 const t0=Math.min(...times),t1=Math.max(...times),tspan=Math.max(1,t1-t0);
 const prices=[];
 candles.forEach(c=>[c.high,c.low,c.open,c.close].forEach(v=>{if(v!==null&&v!==undefined&&Number.isFinite(Number(v)))prices.push(Number(v));}));
 path.forEach(p=>{if(p.paper_price!==null&&p.paper_price!==undefined&&Number.isFinite(Number(p.paper_price)))prices.push(Number(p.paper_price));});
 leaderPath.forEach(p=>{if(p.mark_price!==null&&p.mark_price!==undefined&&Number.isFinite(Number(p.mark_price)))prices.push(Number(p.mark_price));});
 [t.entry_price,t.mark_price,t.exit_price,t.stop_price,t.target_price,l.entry_price].forEach(v=>{if(v!==null&&v!==undefined&&Number.isFinite(Number(v)))prices.push(Number(v));});
 if(!prices.length)return '<div class="empty-state">Price history is not available.</div>';
 let pmin=Math.min(...prices),pmax=Math.max(...prices);if(pmax===pmin){pmax+=Math.abs(pmax)*0.001||1;pmin-=Math.abs(pmin)*0.001||1;}
 const x=v=>6+(Math.max(0,Math.min(1,(v-t0)/tspan))*88);
 const y=v=>91-((Number(v)-pmin)/(pmax-pmin))*82;
 const parts=['<svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img" aria-label="Binance futures candles with our trade and leader entry references">'];
 parts.push('<line x1="6" y1="91" x2="94" y2="91" stroke="#516071" stroke-width=".35"/>');
 const candleWidth=Math.max(.12,Math.min(1.1,84/Math.max(candles.length,1)*.62));
 candles.forEach(c=>{
  const ot=ts(c.open_time);if(ot===null)return;
  const xx=x(ot),yo=y(c.open),yc=y(c.close),yh=y(c.high),yl=y(c.low);
  const up=Number(c.close)>=Number(c.open),col=up?"#20b58c":"#e36a74";
  parts.push('<line x1="'+xx+'" y1="'+yh+'" x2="'+xx+'" y2="'+yl+'" stroke="'+col+'" stroke-width=".22"/>');
  parts.push('<rect x="'+(xx-candleWidth/2)+'" y="'+Math.min(yo,yc)+'" width="'+candleWidth+'" height="'+Math.max(.24,Math.abs(yc-yo))+'" fill="'+col+'" opacity=".9"><title>'+new Date(ot).toLocaleString()+' · O '+fmt(c.open,8)+' H '+fmt(c.high,8)+' L '+fmt(c.low,8)+' C '+fmt(c.close,8)+'</title></rect>');
 });
 const drawSeries=(rows,key,color,width)=>{
  let current=[];
  const segments=[];
  rows.forEach(row=>{
   const time=ts(row.at),value=Number(row[key]);
   if(time===null||row[key]===null||row[key]===undefined||!Number.isFinite(value)){
    if(current.length>1)segments.push(current);current=[];return;
   }
   current.push(x(time)+","+y(value));
  });
  if(current.length>1)segments.push(current);
  segments.forEach(seg=>parts.push('<polyline points="'+seg.join(" ")+'" fill="none" stroke="'+color+'" stroke-width="'+width+'" vector-effect="non-scaling-stroke"/>'));
 };
 drawSeries(path,"paper_price","#b49aff",1.8);
 drawSeries(leaderPath,"mark_price","#5bd5ae",1.3);
 const hline=(value,color,label)=>{
  if(value===null||value===undefined||!Number.isFinite(Number(value)))return;
  const yy=y(value);
  parts.push('<line x1="6" y1="'+yy+'" x2="94" y2="'+yy+'" stroke="'+color+'" stroke-dasharray="2 1.5" stroke-width=".65"><title>'+label+' '+fmt(value,8)+'</title></line>');
 };
 hline(t.entry_price,"#b49aff","Our entry");
 hline(l.entry_price,"#5bd5ae","Leader entry / recovery");
 const liveAt=ts(t.last_binance_price_at);
 if(t.mark_price!==null&&t.mark_price!==undefined&&Number.isFinite(Number(t.mark_price))){
  const liveX=liveAt===null?94:x(liveAt),liveY=y(t.mark_price);
  parts.push('<circle cx="'+liveX+'" cy="'+liveY+'" r="1.9" fill="#f4c96b" stroke="#111923" stroke-width=".5"><title>Latest Binance mark '+fmt(t.mark_price,8)+' · '+date(t.last_binance_price_at)+'</title></circle>');
 }
 hline(t.stop_price,"#e36a74","Risk-sizing reference · not exit");
 hline(t.target_price,"#e6bb69","Preferred target");
 (d.campaign?.trades||[]).forEach(camp=>{
  const opened=ts(camp.opened_at);
  if(opened!==null){const xx=x(opened);parts.push('<line x1="'+xx+'" y1="5" x2="'+xx+'" y2="95" stroke="#b49aff" stroke-width=".55" stroke-dasharray="1.5 1.5"><title>Paper entry #'+camp.id+'</title></line>');}
  const closed=ts(camp.closed_at);
  if(closed!==null){const xx=x(closed);parts.push('<line x1="'+xx+'" y1="5" x2="'+xx+'" y2="95" stroke="#e6bb69" stroke-width=".7"><title>Paper exit #'+camp.id+' · '+money(camp.net_pnl)+'</title></line>');}
 });
 if(t.exit_price!==null&&t.exit_price!==undefined){
  const closed=ts(t.closed_at);if(closed!==null)parts.push('<circle cx="'+x(closed)+'" cy="'+y(t.exit_price)+'" r="1.1" fill="#e6bb69"><title>Exit '+fmt(t.exit_price,8)+'</title></circle>');
 }
 parts.push('</svg>');
 return '<div class="sparkline paper-market-chart">'+parts.join("")+'</div>'+
  '<div class="chart-legend"><span><i style="background:#e36a74"></i>Down candle</span><span><i style="background:#20b58c"></i>Up candle</span><span><i style="background:#b49aff"></i>Our sampled path / entry</span><span><i style="background:#5bd5ae"></i>Leader mark / recovery</span><span><i style="background:#e6bb69"></i>Exit markers</span></div>';
}
async function refreshPaperTradeDetail(id){
 try{
  const d=await api('/api/paper/'+id),t=d.trade||{},l=d.leader||{},r=d.recovery||{},lad=d.potential_ladder||[],camp=d.campaign||{};
  let html='<div class="profile-grid">'+[
   ['Leader',d.trader?.name||l.trader_name||t.trader_id||'—'],
   ['Leader ID',t.trader_id||d.trader?.id||'—'],
   ['Exact leader position',t.leader_position_key||'—'],
   ['Wallet epoch',t.wallet_epoch_eligible===false?'LEGACY · excluded from current $200 wallet':'CURRENT $200 WALLET'],
   ['Leader status',l.status||'NOT OBSERVED'],
   ['Our entry',fmt(t.entry_price,8)],['Current / exit mark',fmt(t.mark_price,8)],
   ['Gross PnL',money(t.gross_pnl)],['Round-trip costs',money(t.estimated_costs)],
   ['Funding audit',t.funding_audit_status||'LEGACY UNVERIFIED'],
   ['Replay funding history',t.funding_history_replay_verified?'BINANCE VERIFIED':'ESTIMATED / INCOMPLETE'],
   ['Replay funding rows',fmt(t.funding_history_replay_rows,0)+' / '+fmt(t.funding_history_replay_expected_rows,0)],
   ['Actual funding',t.funding_cost_actual===null||t.funding_cost_actual===undefined?'Not verified':money(t.funding_cost_actual)],
   ['Last verified funding subtotal',t.funding_cost_last_verified===null||t.funding_cost_last_verified===undefined?'—':money(t.funding_cost_last_verified)],
   ['Current conservative funding estimate',t.funding_estimate_current===null||t.funding_estimate_current===undefined?'—':money(t.funding_estimate_current)],
   ['Funding estimate at exit',t.funding_estimate_at_exit===null||t.funding_estimate_at_exit===undefined?'—':money(t.funding_estimate_at_exit)],
   ['Funding source used now',t.funding_source||'Not recorded'],
   ['Last verified funding source',t.funding_source_last_verified||'—'],
   ['Funding history rows last seen',t.funding_history_rows===null||t.funding_history_rows===undefined?'—':fmt(t.funding_history_rows,0)],
   ['Net realized / live PnL',money(t.net_pnl)],['Exit reason',t.exit_reason_display||t.exit_reason||'—'],
   ['Post-exit recovery audit',t.post_exit_recovery_label||t.post_exit_recovery?.status||'Pending / not applicable'],
   ['Exit price',fmt(t.exit_price,8)],['MFE net',money(t.mfe_net_pnl)+' · '+pct(t.mfe_pct)],
   ['MAE net',money(t.mae_net_pnl)+' · '+pct(t.mae_pct)],
   ['Max saved data gap',fmt(t.max_data_gap_seconds,1)+' sec'],
   ['Data classification',t.data_quality_status||'UNCLASSIFIED'],
   ['Current feed state',t.monitoring_status||'NOT RECORDED'],
   ['Feed resumed at',date(t.monitoring_resumed_at)],
   ['Last Binance mark',date(t.last_binance_price_at)]
  ].map(x=>{const pnlMetric=/PnL|MFE|MAE/.test(x[0]);const n=parseFloat(String(x[1]).replace(/[^0-9+-.]/g,""));const cls=pnlMetric?(n>=0?"positive":"negative"):"";return '<div class="profile-metric"><small>'+esc(x[0])+'</small><b class="'+cls+'">'+esc(x[1])+'</b></div>';}).join('')+'</div>';
  const ca=t.closed_trade_candle_audit||{},wins=t.post_exit_audit_windows||{},engineAudit=d.legacy_exit_engine_replay||d.new_system_replay||t.new_exit_engine_audit||{},strategyReplay=d.leader_following_strategy_replay||{};
  const horizonHtml=Object.entries(wins).map(([h,v])=>'<div class="analysis-line"><span>'+esc(h)+' · '+esc(v.status||'PENDING')+'</span><b>Checkpoint '+money(v.checkpoint_net_pnl_estimate)+' · MFE '+money(v.max_close_net_pnl_estimate)+' · MAE '+money(v.min_close_net_pnl_estimate)+'</b></div>').join('');
  html+='<div class="analysis-box"><h4>BINANCE CLOSED-TRADE CANDLE AUDIT</h4><div class="analysis-line"><span>Coverage / candles</span><b>'+esc(ca.status||'PENDING')+' · '+fmt(ca.candle_count,0)+'</b></div><div class="analysis-line"><span>Close-based MFE / MAE</span><b>'+money(ca.max_close_mfe_net_estimate)+' / '+money(ca.min_close_mae_net_estimate)+'</b></div><div class="analysis-line"><span>Intrabar MFE / MAE bounds</span><b>'+money(ca.max_intrabar_mfe_net_estimate)+' / '+money(ca.min_intrabar_mae_net_estimate)+'</b></div><div class="analysis-line"><span>Recorded exit capture of close-MFE</span><b>'+pct(ca.actual_exit_capture_pct_of_close_mfe)+'</b></div><div class="analysis-line"><span>Close-MFE left on table (estimate)</span><b>'+money(ca.close_mfe_left_on_table_estimate)+'</b></div><div class="muted-note">'+esc(ca.caveat||'No verified entry-to-exit candle audit yet.')+'</div>'+horizonHtml+'</div>';
  html+='<div class="analysis-box"><h4>LEGACY SINGLE-CYCLE EXIT-ENGINE REPLAY</h4><div class="analysis-line"><span>Replay status / decision</span><b>'+esc(engineAudit.replay_status||'PENDING')+' · '+esc(engineAudit.new_exit_reason||'—')+'</b></div><div class="analysis-line"><span>Counterfactual net estimate</span><b>'+money(engineAudit.new_policy_net_pnl_estimate??engineAudit.new_unrealized_net_pnl_estimate)+'</b></div><div class="analysis-line"><span>Replay exit time</span><b>'+date(engineAudit.new_exit_at||engineAudit.last_observed_at)+'</b></div><div class="analysis-line"><span>Replay sizing basis</span><b>'+money(engineAudit.replay_base_notional)+' notional / '+money(engineAudit.replay_base_margin)+' margin</b></div></div>';
  const strategyCycles=(strategyReplay.cycles||[]).map(c=>'<div class="analysis-line"><span>Cycle '+fmt(c.cycle,0)+' · '+esc(c.exit_reason||'OPEN')+' · '+date(c.exit_at||c.last_observed_at)+'</span><b>'+money(c.net_pnl_estimate)+'</b></div>').join('');
  html+='<div class="analysis-box"><h4>REVISED LEADER-FOLLOWING STRATEGY REPLAY</h4><div class="analysis-line"><span>Status / cycles / re-entries</span><b>'+esc(strategyReplay.replay_status||'Run revised replay from the Paper Trading desk')+' · '+fmt(strategyReplay.completed_cycles,0)+' / '+fmt(strategyReplay.reentry_count,0)+'</b></div><div class="analysis-line"><span>Realized cycle net estimate</span><b>'+money(strategyReplay.revised_policy_realized_net_estimate)+'</b></div><div class="analysis-line"><span>Open unrealized estimate</span><b>'+money(strategyReplay.open_unrealized_net_estimate)+'</b></div>'+strategyCycles+'</div>';
  html+='<div class="analysis-box"><h4>LINKED LEADER POSITION</h4>'+
   '<div class="analysis-line"><span>Leader name / ID</span><b>'+esc(l.trader_name||d.trader?.name||'—')+' / '+esc(l.trader_id||t.trader_id||'—')+'</b></div>'+
   '<div class="analysis-line"><span>Exact position key</span><b>'+esc(l.position_key||t.leader_position_key||'—')+'</b></div>'+
   '<div class="analysis-line"><span>Leader side / leverage / margin</span><b>'+esc(l.side||t.side||'—')+' · '+fmt(l.leverage,2)+'× / '+esc(l.margin_mode||'Not reported')+'</b></div>'+
   '<div class="analysis-line"><span>Leader entry / recovery price</span><b>'+fmt(l.entry_price,8)+'</b></div>'+
   '<div class="analysis-line"><span>Leader latest mark</span><b>'+fmt(l.mark_price,8)+' · '+date(l.observed_at)+'</b></div>'+
   '<div class="analysis-line"><span>Leader unrealized PnL / adverse move</span><b>'+money(l.unrealized_pnl)+' / '+pct(l.adverse_pct)+'</b></div>'+
   '<div class="analysis-line"><span>Our PnL if leader reaches entry</span><b>'+money(l.our_recovery_profit_usdt)+' · '+pct(l.our_recovery_profit_pct)+'</b></div>'+
   '<div class="analysis-line"><span>Leader reached its own entry after our exit?</span><b>'+(l.leader_reached_entry_after_exit?'YES · '+fmt(l.leader_minutes_to_entry_after_exit,1)+' min after exit · '+date(l.leader_reached_entry_after_exit_at):'NO in observed leader path')+'</b></div>'+
   '<div class="analysis-line"><span>Leader best favorable move after our exit</span><b>'+fmt(l.leader_max_favorable_after_exit,8)+' · '+money(l.leader_max_pnl_after_exit)+'</b></div>'+
   '<div class="analysis-line"><span>Comparable recovery sample</span><b>'+fmt(r.probability,1)+'% · '+fmt(r.sample_size,0)+' samples</b></div></div>';
  html+='<div class="analysis-box"><h4>LIVE FUTURES CANDLE CHART · '+esc(t.symbol||'')+' · '+esc(d.candle_interval||'')+'</h4>'+
   paperChartSvg(d)+
   '<div class="muted-note">Candles are Binance USD-M Futures market data. Purple = our observed price path and entry. Green = public Binance leader mark observations and recovery price. Vertical lines = linked paper entries/exits. Refreshes while this trade detail remains open.'+
   (d.candle_error?' Candle fetch warning: '+esc(d.candle_error):'')+'</div></div>';
  const replay=d.new_system_replay||{};
  html+='<div class="analysis-box research-trade-card"><h4>NEW EXIT POLICY · HISTORICAL RESEARCH REPLAY · NOT AN EXECUTED ORDER</h4>'+
   '<div class="analysis-line"><span>Research realized according to old system</span><b class="'+(Number(t.net_pnl)>=0?'positive':'negative')+'">'+money(t.net_pnl)+'</b></div>'+
   '<div class="analysis-line"><span>Counterfactual policy-exit net estimate · not realized</span><b class="'+(Number(replay.new_policy_net_pnl_estimate)>=0?'positive':'negative')+'">'+(replay.new_policy_net_pnl_estimate===null||replay.new_policy_net_pnl_estimate===undefined?'Not verifiable / funding unpriced':money(replay.new_policy_net_pnl_estimate))+'</b></div>'+
   '<div class="analysis-line"><span>Replay status</span><b>'+esc(replay.replay_status||'No qualifying replay evidence')+'</b></div>'+
   '<div class="analysis-line"><span>New policy exit reason / price quality</span><b>'+esc(replay.new_exit_reason||'Still open / no verified exit')+' · '+esc(replay.new_exit_price_quality||'—')+'</b></div>'+
   '<div class="analysis-line"><span>New exit time / detection time</span><b>'+date(replay.new_exit_at)+' / '+date(replay.new_exit_detection_at)+'</b></div>'+
   '<div class="analysis-line"><span>Hold time after old exit</span><b>'+duration(replay.hold_after_old_exit_hours)+'</b></div>'+
   '<div class="analysis-line"><span>Leader identity confidence</span><b>'+esc(replay.leader_identity||'—')+'</b></div>'+
   '<div class="sample-warning">'+esc(replay.leader_identity_note||'No leader identity note')+'</div>'+
   (replay.checkpoints?.length?'<div class="profile-history"><table><thead><tr><th>CHECKPOINT</th><th>OBSERVED TIME</th><th>LEADER MARK</th><th>RESEARCH NET</th></tr></thead><tbody>'+replay.checkpoints.map(cp=>'<tr><td>'+fmt(cp.target_hours,0)+'h target · '+fmt(cp.actual_elapsed_hours,2)+'h actual</td><td>'+date(cp.observed_at)+'</td><td>'+fmt(cp.mark_price,8)+'</td><td class="'+(Number(cp.net_pnl_estimate)>=0?'positive':'negative')+'">'+(cp.net_pnl_estimate===null||cp.net_pnl_estimate===undefined?'Funding unpriced':money(cp.net_pnl_estimate))+'</td></tr>').join('')+'</tbody></table></div>':'<div class="muted-note">No 3h / 6h / later checkpoint was verifiably sampled while this leader key remained visible.</div>')+
   '</div>';
  html+='<div class="analysis-box"><h4>SAME LEADER POSITION · CAMPAIGN PNL & ENTRY FORENSICS</h4>'+
   '<div class="analysis-line"><span>Entry attempts / re-entries</span><b>'+fmt(camp.trade_count,0)+' / '+fmt(camp.reentry_count,0)+'</b></div>'+
   '<div class="analysis-line"><span>Currently open attempts</span><b>'+fmt(camp.open_count,0)+'</b></div>'+
   '<div class="analysis-line"><span>Realized campaign net PnL</span><b class="'+(Number(camp.realized_net_pnl)>=0?'positive':'negative')+'">'+money(camp.realized_net_pnl)+'</b></div>'+
   '<div class="analysis-line"><span>Total campaign net PnL</span><b class="'+(Number(camp.total_net_pnl)>=0?'positive':'negative')+'">'+money(camp.total_net_pnl)+'</b></div>'+
   '<div class="analysis-line"><span>Positive attempts total</span><b class="positive">'+money(camp.sum_positive_attempt_pnl)+'</b></div>'+
   '<div class="analysis-line"><span>Negative attempts total</span><b class="negative">'+money(camp.sum_negative_attempt_pnl)+'</b></div>'+
   '<div class="profile-history"><table><thead><tr><th>ENTRY #</th><th>ENTRY TIME</th><th>EXIT TIME</th><th>HOLD</th><th>GROSS</th><th>COSTS</th><th>NET</th><th>AFTER-EXIT PRICE PATH · NOT REALIZED</th></tr></thead><tbody>'+
   (camp.trades||[]).map(x=>'<tr><td><b>Entry #'+fmt(x.attempt_number,0)+' · Trade #'+x.id+'</b></td><td>'+date(x.opened_at)+'</td><td>'+date(x.closed_at)+'</td><td>'+duration(x.hold_duration_hours)+'</td><td class="'+(Number(x.gross_pnl)>=0?'positive':'negative')+'">'+money(x.gross_pnl)+'</td><td class="negative">'+money(x.estimated_costs)+'</td><td class="'+(Number(x.net_pnl)>=0?'positive':'negative')+'"><b>'+money(x.net_pnl)+'</b></td><td>'+esc(x.post_exit_status||'Pending')+'<br><small>'+date(x.post_exit_max_at)+'</small></td></tr>').join('')+
   '</tbody></table></div>'+
   '<div class="sample-warning">Campaign total is the sum of these actual paper-ledger attempts. Post-exit price recovery is counterfactual market-path evidence, not realized campaign profit. Linked trade IDs: '+esc((camp.trade_ids||[]).join(', '))+'</div></div>';
  html+='<div class="analysis-box"><h4>SWING & LEADER-RECOVERY PROFIT LADDER</h4><div class="profile-history"><table><thead><tr><th>TARGET / MOVE</th><th>PRICE</th><th>GROSS</th><th>ESTIMATED NET</th></tr></thead><tbody>'+
   (lad.length?lad.map(x=>'<tr><td>'+esc(x.target_type==='INTERMEDIATE_SWING_FROM_LATEST_MARK'?'Swing +':'Leader recovery +')+fmt(x.target_pct_from_reference,2)+'%</td><td>'+fmt(x.price,8)+'</td><td class="'+(x.gross_pnl_usdt>=0?'positive':'negative')+'">'+money(x.gross_pnl_usdt)+'</td><td class="'+(x.estimated_net_pnl_usdt>=0?'positive':'negative')+'">'+money(x.estimated_net_pnl_usdt)+'</td></tr>').join(''):'<tr><td colspan="4">No swing or leader recovery targets available.</td></tr>')+
   '</tbody></table></div></div>';
  const events=(d.events||[]).map(e=>'<div class="analysis-line"><span>'+date(e.at)+' · '+esc(e.type)+'</span><b>'+money(e.value)+' · '+esc(e.detail)+'</b></div>').join('');
  html+='<div class="analysis-box"><h4>PROFIT / DATA-QUALITY EVENT AUDIT</h4>'+(events||'<div class="muted-note">No audit events recorded.</div>')+'</div>';
  html+='<div class="sample-warning">'+esc(d.data_quality?.note||r.note||'Recovery statistics are empirical, not a guarantee. Missing observations cannot be reconstructed.')+'</div>';
  $("dialogBody").innerHTML=html;
 }catch(e){$("dialogBody").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}
}
async function openPaperTrade(id){
 clearInterval(paperDetailTimer);paperDetailTimer=null;
 const modal=$("traderDialog");
 if(modal.open)modal.close();
 $("dialogName").textContent="Paper trade #"+id;
 $("dialogSub").textContent="Exact leader-linked position · live Binance futures chart";
 $("dialogBody").innerHTML='<div class="empty-state">Loading leader observations, futures candles and paper audit path…</div>';
 modal.showModal();
 await refreshPaperTradeDetail(id);
 paperDetailTimer=setInterval(()=>{
  if(!modal.open){clearInterval(paperDetailTimer);paperDetailTimer=null;return;}
  refreshPaperTradeDetail(id);
 },2000);
}
window.openPaperTrade=openPaperTrade;
click("reloadPaper",loadPaper);
click("reloadMomentum",loadMomentumPaper);
click("runRecoveryAudit",runRecoveryAudit);
click("loadResearchReplay",loadResearchReplay);
bind("paperSettings","submit",async e=>{e.preventDefault();const f=new FormData(e.currentTarget),body={};for(const [k,v] of f.entries())body[k]=Number(v);body.auto_enabled=e.currentTarget.elements.auto_enabled.checked;try{await api("/api/paper/settings",{method:"POST",body:JSON.stringify(body)});$("paperSaveStatus").textContent="Saved · paper settings active";await loadPaper();}catch(err){$("paperSaveStatus").textContent=err.message;}});
function spark(values){if(!values||values.length<2)return'<div class="empty-state">Candle series unavailable</div>';const min=Math.min(...values),max=Math.max(...values),span=max-min||1;const pts=values.map((v,i)=>(i/(values.length-1)*100)+","+(95-(v-min)/span*85)).join(" ");return'<svg viewBox="0 0 100 100" preserveAspectRatio="none"><polyline points="'+pts+'" fill="none" stroke="#a995f3" stroke-width="1.8" vector-effect="non-scaling-stroke"/><line x1="0" y1="95" x2="100" y2="95" stroke="#354454" stroke-width=".6"/></svg>';}
async function loadMarket(forceRefresh=true){
 const s=$("marketSymbol").value.trim().toUpperCase();
 if(!s){notice("Enter a futures symbol such as BTCUSDT.","error");return;}
 $("marketResult").innerHTML='<div class="empty-state">Pulling seven days of candles and current market data…</div>';
 $("newsList").innerHTML='<div class="empty-state">Refreshing headline feeds…</div>';
 const results=await Promise.allSettled([
  api("/api/market/"+encodeURIComponent(s)+"?refresh="+(forceRefresh?"true":"false")),
  api("/api/news?symbol="+encodeURIComponent(s)+"&refresh="+(forceRefresh?"true":"false"))
 ]);
 const marketResult=results[0], newsResult=results[1];
 if(marketResult.status==="fulfilled"){
  const m=marketResult.value;
  const items=[["MARK PRICE",fmt(m.mark_price,6)],["7-DAY RETURN",pct(m.seven_day_return_pct)],["24-HOUR RETURN",pct(m.twenty_four_hour_return_pct)],["FUNDING RATE",m.funding_rate===null?"—":pct(m.funding_rate*100)],["24H ATR / PRICE",pct(m.atr_24h_pct)],["1H RSI(14)",fmt(m.rsi_1h,1)],["5M MOMENTUM",pct(m.momentum_5m_pct)],["INDEX PRICE",fmt(m.index_price,6)]];
  $("marketResult").innerHTML='<div class="market-grid">'+items.map(x=>'<div class="market-stat"><small>'+x[0]+'</small><b>'+x[1]+'</b></div>').join("")+'</div><div class="market-chart"><div class="section-kicker">LAST 30 HOURLY CLOSES · SOURCE: BINANCE</div><div class="sparkline">'+spark(m.closes_1h)+'</div><div class="muted-note">Fetched '+date(m.fetched_at)+' · 168 one-hour candles requested for the seven-day window.</div></div>';
 }else{$("marketResult").innerHTML='<div class="notice error">Market data: '+esc(marketResult.reason?.message||"unavailable")+'</div>';}
 if(newsResult.status==="fulfilled"){
  const n=newsResult.value;
  $("newsList").innerHTML='<div class="muted-note">Updated: '+esc(n.fetched_at?date(n.fetched_at):"time unavailable")+' · '+esc(n.feed_error||"All feeds fetched without reported errors")+(n.feed_error?' · partial feed failure':'')+'</div>'+(n.items?.length?n.items.map(i=>'<div class="news-item"><a href="'+esc(i.url)+'" target="_blank" rel="noreferrer">'+esc(i.title)+'</a><small>'+esc(i.source)+' · '+esc(i.published_at||"Publication time unavailable")+'</small></div>').join(""):'<div class="empty-state">No matched headlines returned. This is not proof that no news event occurred.</div>');
 }else{$("newsList").innerHTML='<div class="notice error">News feed: '+esc(newsResult.reason?.message||"unavailable")+'</div>';}
}
click("loadMarket",()=>loadMarket(true));bind("marketSymbol","keydown",e=>{if(e.key==="Enter")loadMarket(true);});click("reloadMarketUniverse",loadMarketUniverse);
async function loadMarketUniverse(){
 const body=$("marketUniverseTable");if(!body)return;
 try{const d=await api("/api/market/universe");body.innerHTML=(d.rows||[]).map(x=>'<tr><td><b>'+esc(x.symbol)+'</b></td><td>'+fmt(x.mark_price,8)+'</td><td>'+fmt(x.momentum_5m_pct,3)+'%</td><td>'+fmt((x.funding_rate||0)*100,4)+'%</td><td>'+money(x.quote_volume_24h_usdt)+'</td><td><span class="status-tag '+(x.live_fresh?"positive":"neutral")+'">'+esc(x.status||"CACHED")+'</span></td></tr>').join("")||'<tr><td colspan="6" class="empty-cell">No market symbols available yet.</td></tr>';}catch(e){body.innerHTML='<tr><td colspan="6" class="empty-cell">'+esc(e.message)+'</td></tr>'}
}
async function loadSystem(){
 $("tableMetrics").innerHTML='<div class="empty-state">Reading database telemetry…</div>';
 try{
  const [systemResult,healthResult,probeResult]=await Promise.allSettled([
   api("/api/system"),api("/api/health"),api("/api/db/synthetic")
  ]);
  if(systemResult.status!=="fulfilled")throw systemResult.reason;
  const d=systemResult.value,db=d.database||{},m=d.monitor||{};
  const health=healthResult.status==="fulfilled"?healthResult.value:null;
  const probe=probeResult.status==="fulfilled"?probeResult.value:null;
  $("dbEngine").textContent=(db.engine||"unknown").toUpperCase();
  $("dbUsedLarge").textContent=db.used_mb===null||db.used_mb===undefined?"Not reported":fmt(db.used_mb,3)+" MB";
  const healthText=health?("API "+(health.status||"unknown")):("API probe failed: "+(healthResult.reason?.message||"unavailable"));
  const probeText=probe?("DB read/write "+(probe.status||"unknown")+" · "+fmt(probe.latency_ms,0)+" ms"):("DB read/write probe failed: "+(probeResult.reason?.message||"unavailable"));
  $("dbConnection").textContent="Connection: "+(db.connection||"unknown")+" · persistent: "+(db.persistent?"yes":"no")+" · "+healthText+" · "+probeText;
  $("dbCapacityLarge").textContent=db.capacity_note||"";
  $("dbBarLarge").style.width=db.configured_capacity_gb&&db.used_mb!==null?Math.min(100,db.used_mb/(db.configured_capacity_gb*1024)*100)+"%":"0%";
  $("workerState").textContent=m.worker_alive?"Running":"Not active";
  $("workerInterval").textContent=Math.round((m.interval_seconds||0)/60)+" minutes";
  $("workerBatch").textContent=(m.batch||0)+" traders / pass";
  $("workerLastState").textContent=m.state||"—";
  $("workerError").textContent=m.last_error||"None";
  $("runtimeCaveat").textContent=d.runtime?.render_free_sleep_warning||"";
  $("tableMetrics").innerHTML=db.tables?.length?db.tables.map(t=>'<div class="table-metric"><b>'+esc(t.name)+'</b><small>'+(t.bytes===null?"Size unavailable":fmt(t.bytes/1024,1)+" KB")+'</small></div>').join(""):'<div class="empty-state">No table-size breakdown returned.</div>';
  $("rowCounts").innerHTML=Object.entries(d.counts||{}).map(([k,v])=>'<div class="row-count"><span>'+esc(k.replaceAll("_"," "))+'</span><b>'+fmt(v,0)+'</b></div>').join("");
 }catch(e){$("tableMetrics").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}
}
click("reloadSystem",loadSystem);
click("scanNow",async()=>{$("scanNow").disabled=true;$("scanNow").textContent="Scanning…";try{const d=await api("/api/sync/now",{method:"POST"});notice("Monitor pass complete · "+fmt(d.traders_found,0)+" leaders updated · "+fmt(d.positions_checked,0)+" portfolios checked · "+fmt(d.observations_saved,0)+" positions saved.","success");await refreshOverview();if(state.page==="market")loadMarketUniverse();if(state.page==="traders")await loadTraders();if(state.page==="positions")await loadPositions();}catch(e){notice(e.message,"error");}finally{$("scanNow").disabled=false;$("scanNow").textContent="↻ Scan now";}});
setInterval(()=>{if(state.page==="market"){loadMarket(true);loadMarketUniverse();}},900000);
async function boot(){
 // The active Overview page does not need the full trader grid; load it on navigation.
 refreshOverview();
 // Paper marks/PnL refresh faster than trader discovery and profile refresh.
 setInterval(()=>{if(state.page==="paper")loadPaper();},3000);
 setInterval(()=>{
  refreshOverview();
  if(state.page==="positions")loadPositions();
  if(state.page==="traders")loadTraders();
  if(state.page==="opportunities")loadOpportunities();
 },60000);
}

window.addEventListener("load",()=>boot().catch(e=>notice("LAES boot failed: "+e.message,"error")));
