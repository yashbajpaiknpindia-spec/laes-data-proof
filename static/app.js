const $=id=>document.getElementById(id);
const state={page:"overview"};
let paperDetailTimer=null;
const fmt=(v,d=2)=>v===null||v===undefined||v===""||Number.isNaN(Number(v))?"—":Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
const money=v=>v===null||v===undefined?"—":fmt(v,Math.abs(Number(v))<0.01?8:4)+" USDT";
const pct=v=>v===null||v===undefined?"—":fmt(v,2)+"%";
const date=v=>{if(!v)return"—";const d=new Date(v);return Number.isNaN(d.getTime())?"—":d.toLocaleString();};
const duration=h=>h===null||h===undefined?"Unavailable":h<1?Math.round(h*60)+"m":h<24?h.toFixed(1)+"h":(h/24).toFixed(1)+"d";
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
async function api(url,options={}){const r=await fetch(url,{headers:{"Content-Type":"application/json"},...options});let d={};try{d=await r.json()}catch{d={detail:await r.text()}}if(!r.ok)throw new Error(d.detail||"HTTP "+r.status);return d;}
function notice(s,kind){const n=$("notice");n.textContent=s;n.className="notice "+(kind||"");n.classList.remove("hidden");clearTimeout(notice.timer);notice.timer=setTimeout(()=>n.classList.add("hidden"),9000);}
function setPage(name){state.page=name;document.querySelectorAll(".page").forEach(x=>x.classList.toggle("active",x.id==="page-"+name));document.querySelectorAll(".nav-item").forEach(x=>x.classList.toggle("active",x.dataset.page===name));const t={overview:"Overview",traders:"Trader universe",positions:"Live positions",opportunities:"Opportunity scanner",paper:"Paper trading desk",market:"Market & news",database:"Database & system"};$("pageTitle").textContent=t[name]||"Overview";if(name==="overview")refreshOverview();if(name==="traders")loadTraders();if(name==="positions")loadPositions();if(name==="opportunities")loadOpportunities();if(name==="paper")loadPaper();if(name==="database")loadSystem();}
document.querySelectorAll(".nav-item").forEach(b=>b.addEventListener("click",()=>setPage(b.dataset.page)));
document.querySelectorAll("[data-goto]").forEach(b=>b.addEventListener("click",()=>setPage(b.dataset.goto)));
function source(id,dot,ok,label){$(id).textContent=label;$(id).className="connection-state "+(ok===true?"ok":ok===false?"bad":"");$(dot).className="source-dot "+(ok===true?"ok":ok===false?"bad":"");}
async function refreshOverview(){
 try{
 const d=await api("/api/overview");state.overview=d;const m=d.monitor||{};
 $("mTraders").textContent=fmt(d.traders,0);$("mPositions").textContent=fmt(d.observations,0);$("mUnderwater").textContent=fmt(d.underwater_observations,0);$("mPaperOpen").textContent=fmt(d.open_paper_trades,0);
 $("navTraderCount").textContent=fmt(d.traders,0);$("navPaperCount").textContent=fmt(d.open_paper_trades,0);$("navPositionCount").textContent=fmt(d.observations,0);
 $("sideStatus").textContent=m.worker_alive?"Automatic monitor running":"Monitor starting / sleeping";$("sidePulse").className="pulse "+(m.worker_alive?"on":"");
 $("monitorBadge").textContent=m.state||"INITIALIZING";$("monitorBadge").className="pill "+(m.state==="idle"?"ok":m.state==="error"?"warn":"");
 $("lastScan").textContent=m.finished_at?date(m.finished_at):"Waiting for first scan";$("nextScan").textContent="Discovery cadence "+Math.round((m.interval_seconds||1800)/60)+" min · up to "+(m.position_batch_per_cycle||5)+" leaders per pass";
 $("stepDiscover").textContent=m.last_error||(m.finished_at?fmt(m.traders_found,0)+" new/updated trader records":"Waiting for Kopyon");$("stepDiscoverState").textContent=m.state==="error"?"ERROR":m.finished_at?"DONE":"WAIT";
 $("stepPositions").textContent=m.finished_at?(m.positions_checked||0)+" leader portfolios checked · "+(m.observations_saved||0)+" position rows stored":"Rotation begins after leaderboard fetch";$("stepPositionsState").textContent=m.state==="error"?"CHECK":m.finished_at?"DONE":"WAIT";
 $("stepPaper").textContent=d.paper?.auto_enabled?"Auto paper entries enabled when scanner confirms":"Paper entry automation paused in settings";$("stepPaperState").textContent=d.paper?.auto_enabled?"ACTIVE":"PAUSED";
 source("healthKopyon","kopyonDot",d.kopyon?.configured,d.kopyon?.configured?"KEY CONFIGURED":"KEY MISSING");$("kopyonState").textContent=d.kopyon?.configured?"Configured":"Missing key";source("healthBinance","binanceDot",true,"PUBLIC DATA");$("binanceState").textContent="Public data";
 const dbOk=d.database?.connection==="connected";$("healthDatabase").textContent=dbOk?(d.database.persistent?"POSTGRES":"SQLITE / EPHEMERAL"):"ERROR";$("healthDatabase").className="connection-state "+(dbOk?(d.database.persistent?"ok":""):"bad");
 $("healthDatabaseDesc").textContent=dbOk?(d.database.persistent?"Persistent PostgreSQL":"SQLite: data can be lost on spin-down/redeploy"):"Database unavailable";
 $("dbUsed").textContent=d.database?.used_mb!==null&&d.database?.used_mb!==undefined?fmt(d.database.used_mb,2)+" MB":"Not reported";$("dbCapacityNote").textContent=d.database?.capacity_note||"";
 $("dbBar").style.width=d.database?.configured_capacity_gb&&d.database?.used_mb!==null?Math.min(100,d.database.used_mb/(d.database.configured_capacity_gb*1024)*100)+"%":"0%";
 if(m.last_error)notice("Monitor error: "+m.last_error,"error");
 if(!d.kopyon?.configured)notice("KOPYON_API_KEY is not visible to this running service. Add it to laes-data-proof-python, not the retired Elixir service.","error");
 if(!d.database?.persistent)notice("The running service is not connected to persistent PostgreSQL. Check DATABASE_URL on laes-data-proof-python.","error");
 if(state.page==="overview"){
  try{const p=await api("/api/paper");$("mPaperEquity").textContent=money(p.equity);$("mPaperPnl").textContent="Realized "+money(p.realized_pnl)+" · Unrealized "+money(p.unrealized_pnl);}catch{}
  try{const o=await api("/api/opportunities?limit=10");const q=o.filter(x=>x.qualified);$("mOpportunities").textContent=fmt(q.length,0);$("navOppCount").textContent=fmt(q.length,0);renderMini(o.slice(0,5));}catch(e){$("overviewOpps").innerHTML='<div class="empty-state">'+esc(e.message)+'</div>';}
 }
 }catch(e){notice("Dashboard status unavailable: "+e.message,"error");}
}
function renderMini(items){if(!items.length){$("overviewOpps").innerHTML='<div class="empty-state">No qualifying or underwater positions observed yet. Only live provider snapshots appear here.</div>';return;}$("overviewOpps").innerHTML=items.map(o=>'<div class="mini-opp"><div><b>'+esc(o.symbol)+' · '+esc(o.side)+' · '+esc(o.trader_name)+'</b><small>'+esc(o.status)+' · adverse '+pct(o.adverse_pct)+' · observed '+date(o.observed_at)+'</small></div><strong>'+money(o.unrealized_pnl)+'</strong><span class="status-tag '+(o.qualified?"qualified":"")+'">'+(o.qualified?"PAPER READY":"WATCH")+'</span></div>').join("");}
async function loadTraders(){
 $("traderGrid").innerHTML='<div class="empty-state">Loading discovered public trader records…</div>';
 try{const q=encodeURIComponent($("traderSearch").value||"");const rows=await api("/api/traders?limit=250&search="+q);$("traderListCount").textContent=rows.length+" live traders stored";
 if(!rows.length){$("traderGrid").innerHTML='<div class="empty-state">No real trader profiles stored yet. Check Kopyon source status or use Scan now.</div>';return;}
 $("traderGrid").innerHTML=rows.map(t=>{
 const vis=t.visibility||"unknown";const av=t.avatar_url?'<img class="avatar" src="'+esc(t.avatar_url)+'" alt="">':'<div class="avatar avatar-fallback">'+esc((t.name||"L").slice(0,1).toUpperCase())+'</div>';
 return '<article class="trader-card"><div class="trader-head">'+av+'<div class="trader-title"><b>'+esc(t.name||"Unnamed leader")+'</b><small>'+esc(t.trader_id)+'</small></div><span class="visibility '+(vis.includes("hidden")?"hidden":"")+'">'+esc(vis.replaceAll("_"," "))+'</span></div><div class="trader-stats"><div class="trader-stat"><small>7D ROI</small><b class="'+((t.roi||0)>=0?"positive":"negative")+'">'+pct((t.roi||0)*100)+'</b></div><div class="trader-stat"><small>MAX DD</small><b class="negative">'+pct((t.max_drawdown||0)*100)+'</b></div><div class="trader-stat"><small>COPIER PNL</small><b class="'+((t.copier_pnl||0)>=0?"positive":"negative")+'">'+money(t.copier_pnl)+'</b></div></div><div class="trader-profile-blurb">'+(t.bio?esc(t.bio):"Profile details refresh automatically")+'</div><div class="trader-meta"><span>Win '+pct((t.win_rate||0)*100)+'</span><span>AUM '+money(t.aum)+'</span><span>Sharpe '+fmt(t.sharpe_ratio)+'</span><span>Copiers '+fmt(t.copiers,0)+'</span><span>Share '+(t.profit_share===null||t.profit_share===undefined?"—":pct(t.profit_share*100))+'</span></div><button class="btn btn-subtle" onclick="openTrader(\''+encodeURIComponent(t.trader_id)+'\')">Open forensic profile →</button></article>';
 }).join("");
 }catch(e){$("traderGrid").innerHTML='<div class="empty-state">'+esc(e.message)+'</div>';}
}
$("traderSearch").addEventListener("input",()=>{clearTimeout(loadTraders.timer);loadTraders.timer=setTimeout(loadTraders,250);});$("reloadTraders").onclick=loadTraders;
async function openTrader(enc){
 clearInterval(paperDetailTimer);paperDetailTimer=null;
 const id=decodeURIComponent(enc);$("dialogName").textContent="Loading trader…";$("dialogSub").textContent=id;$("dialogBody").innerHTML='<div class="empty-state">Loading profile, performance and closed history from Kopyon…</div>';$("traderDialog").showModal();
 try{await api("/api/traders/"+encodeURIComponent(id)+"/intel",{method:"POST"});}catch(e){$("dialogBody").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}
 try{
 const d=await api("/api/traders/"+encodeURIComponent(id)),t=d.trader,p=d.profile||{},hs=d.history_summary||{};$("dialogName").textContent=t.name||id;$("dialogSub").textContent=id+" · "+(t.portfolio_type||"Portfolio")+" · visibility: "+t.visibility;
 const metrics=[["ROI",pct((t.roi||0)*100)],["Max drawdown",pct((t.max_drawdown||0)*100)],["Copier PnL",money(t.copier_pnl)],["Sharpe",fmt(t.sharpe_ratio)],["AUM",money(t.aum)],["Profit share",pct((p.profitShare||0)*100)],["Copiers",fmt(p.copiers??t.raw?.copiers,0)],["Margin balance",p.sourceDetails?.marginBalance?money(p.sourceDetails.marginBalance):"Not reported"]];
 let html=p.bio?'<p class="profile-bio">'+esc(p.bio)+'</p>':"";
 html+='<div class="profile-grid">'+metrics.map(m=>'<div class="profile-metric"><small>'+esc(m[0])+'</small><b>'+esc(m[1])+'</b></div>').join("")+'</div>';
 html+='<div class="analysis-box"><h4>TRADER USUAL HOLDING BEHAVIOUR</h4><div class="analysis-line"><span>Completed history rows</span><b>'+fmt(hs.records,0)+'</b></div><div class="analysis-line"><span>Median holding time</span><b>'+duration(hs.median_hold_hours)+'</b></div><div class="analysis-line"><span>Average holding time</span><b>'+duration(hs.avg_hold_hours)+'</b></div><div class="analysis-line"><span>Closed-history win rate</span><b>'+pct(hs.win_rate_closed_history===null?null:hs.win_rate_closed_history*100)+'</b></div><div class="analysis-line"><span>Average net PnL</span><b>'+money(hs.avg_net_pnl)+'</b></div></div>';
 html+='<div class="panel-heading" style="margin-top:15px"><div><h3>Closed position history</h3><p>Fetched from Kopyon; history rows alone do not reveal historical MAE/MFE.</p></div></div><div class="profile-history"><table><thead><tr><th>SYMBOL</th><th>SIDE</th><th>OPENED</th><th>CLOSED</th><th>ROI</th><th>NET PNL</th></tr></thead><tbody>';
 html+=d.history?.length?d.history.map(h=>'<tr><td>'+esc(h.symbol)+'</td><td>'+esc(h.side)+'</td><td>'+date(h.opened_at)+'</td><td>'+date(h.closed_at)+'</td><td>'+pct((h.roi||0)*100)+'</td><td class="'+((h.net_pnl||0)>=0?"positive":"negative")+'">'+money(h.net_pnl)+'</td></tr>').join(""):'<tr><td colspan="6" class="empty-cell">No closed history returned.</td></tr>';
 $("dialogBody").innerHTML=html+'</tbody></table></div>';
 }catch(e){$("dialogBody").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}
}
window.openTrader=openTrader;$("closeDialog").onclick=()=>{clearInterval(paperDetailTimer);paperDetailTimer=null;$("traderDialog").close();};
async function loadPositions(){
 $("positionTable").innerHTML='<tr><td colspan="9" class="empty-cell">Loading real position observations…</td></tr>';
 try{const [rows,traders]=await Promise.all([api("/api/positions?limit=500"),api("/api/traders?limit=300")]);const names=Object.fromEntries(traders.map(t=>[t.trader_id,t.name||t.trader_id]));
 if(!rows.length){$("positionTable").innerHTML='<tr><td colspan="9" class="empty-cell">No public position observations have been saved. Hidden positions remain unavailable.</td></tr>';return;}
 $("positionTable").innerHTML=rows.map(p=>{const a=p.entry_price&&p.mark_price?(p.side==="LONG"?(p.entry_price-p.mark_price):(p.mark_price-p.entry_price))/p.entry_price*100:null;return '<tr><td>'+esc(names[p.trader_id]||p.trader_id)+'<br><small>'+esc(p.trader_id)+'</small></td><td><b>'+esc(p.symbol)+'</b></td><td class="'+(p.side==="LONG"?"side-long":"side-short")+'">'+esc(p.side)+'</td><td>'+fmt(p.entry_price,6)+'</td><td>'+fmt(p.mark_price,6)+'</td><td class="'+((p.unrealized_pnl||0)<0?"negative":"positive")+'">'+money(p.unrealized_pnl)+'</td><td class="'+((a||0)>0?"negative":"positive")+'">'+pct(a)+'</td><td>'+(p.leverage?fmt(p.leverage,1)+"×":"—")+' '+esc(p.margin_mode||"")+'</td><td>'+date(p.observed_at)+'</td></tr>';}).join("");
 }catch(e){$("positionTable").innerHTML='<tr><td colspan="9" class="empty-cell">'+esc(e.message)+'</td></tr>';}
}
$("reloadPositions").onclick=loadPositions;
async function loadOpportunities(){
 $("opportunityList").innerHTML='<div class="empty-state">Analyzing actual positions against Binance market data…</div>';
 try{const items=await api("/api/opportunities?limit=100");const qual=items.filter(o=>o.qualified);$("navOppCount").textContent=fmt(qual.length,0);
 if(!items.length){$("opportunityList").innerHTML='<div class="empty-state">No observed positions yet. Scanner does not create hypothetical opportunities.</div>';return;}
 $("opportunityList").innerHTML=items.map(o=>{
 const rec=o.recovery||{},hold=o.holding||{},causes=[...(o.likely_causes||[]),...(o.dead_market_reasons||[]).map(reason=>({label:'ENTRY BLOCKED',effect:reason,risk:'adverse'}))];
 let recovery=rec.probability===null?'<div class="recovery-prob">N/A</div><div class="sample-warning">'+esc(rec.label||"Insufficient history")+' · sample '+fmt(rec.sample_size,0)+'</div>':'<div class="recovery-prob">'+fmt(rec.probability,1)+'%</div><div class="analysis-line"><span>Wilson 95% interval</span><b>'+fmt(rec.ci_low,1)+'% – '+fmt(rec.ci_high,1)+'%</b></div><div class="analysis-line"><span>Comparable paths</span><b>'+fmt(rec.sample_size,0)+'</b></div>';
 const causeHtml=causes.length?causes.map(c=>'<div class="cause '+esc(c.risk||"")+'">'+esc(c.label)+' · '+esc(c.effect)+(c.url?' · <a href="'+esc(c.url)+'" target="_blank" rel="noreferrer">source</a>':"")+'</div>').join(""):'<div class="cause">No related headline or market cause flagged.</div>';
 return '<article class="opportunity-card"><div class="opp-head"><div class="opp-title"><div><h3>'+esc(o.symbol)+' <span class="'+(o.side==="LONG"?"side-long":"side-short")+'">'+esc(o.side)+'</span></h3><small>'+esc(o.trader_name)+' · '+esc(o.trader_id)+' · seen '+date(o.observed_at)+'</small></div></div><span class="status-tag '+(o.qualified?"qualified":o.underwater?"":"neutral")+'">'+esc(o.status)+'</span></div><div class="opp-stats"><div class="opp-stat"><small>LEADER LOSS</small><b class="negative">'+money(o.unrealized_pnl)+'</b></div><div class="opp-stat"><small>ADVERSE FROM ENTRY</small><b class="negative">'+pct(o.adverse_pct)+'</b></div><div class="opp-stat"><small>ENTRY / CURRENT</small><b>'+fmt(o.entry_price,5)+' / '+fmt(o.mark_price,5)+'</b></div><div class="opp-stat"><small>HOLD TIME</small><b>'+duration(hold.current_hours)+'</b></div><div class="opp-stat"><small>5M MOMENTUM</small><b class="'+((o.momentum_5m_pct||0)>=0?"positive":"negative")+'">'+pct(o.momentum_5m_pct)+'</b></div></div><div class="opp-bottom"><div class="analysis-box"><h4>RECOVERY EVIDENCE</h4>'+recovery+'<div class="analysis-line"><span>Trader typical median hold</span><b>'+duration(hold.typical_median_hours)+'</b></div><div class="sample-warning">'+esc(rec.note||"")+'</div></div><div class="analysis-box"><h4>MARKET / EVENT CONTEXT</h4><div class="cause-list">'+causeHtml+'</div><div class="sample-warning">'+esc(o.recovery_warning||"")+'</div></div></div></article>';
 }).join("");
 }catch(e){$("opportunityList").innerHTML='<div class="empty-state">'+esc(e.message)+'</div>';}
}
$("refreshOpps").onclick=loadOpportunities;
async function loadPaper(){
 try{
  const d=await api("/api/paper");
  $("paperCapital").textContent=money(d.capital);
  $("paperMargin").textContent=money(d.deployed_margin)+" / "+money(d.margin_cap);
  $("paperMarginFoot").textContent=d.margin_cap_exceeded?"PORTFOLIO MARGIN CAP EXCEEDED · new entries paused":fmt(d.margin_utilization_pct,1)+"% of capital deployed · cap "+fmt(d.config?.max_total_margin_pct,0)+"%";
  $("paperRealized").textContent=money(d.realized_pnl);
  $("paperUnrealized").textContent=money(d.unrealized_pnl);
  $("paperFree").textContent=money(d.free_capital);
  $("paperEquity").textContent=money(d.equity);
  const f=$("paperSettings");
  Object.entries(d.config||{}).forEach(([k,v])=>{const el=f.elements[k];if(el){if(el.type==="checkbox")el.checked=!!v;else el.value=v;}});
  const rs=d.risk_summary||{}, groups=d.correlated_exposure_groups||[];
  $("paperRiskClusters").innerHTML=
   '<div class="profile-grid">'+[
    ["Risk mode",rs.mode||"—"],
    ["Shared-direction clusters",fmt(rs.correlated_groups,0)],
    ["Estimated open stop risk",money(rs.estimated_open_stop_risk_usdt)],
    ["Largest cluster stop risk",money(rs.largest_shared_direction_stop_risk_usdt)]
   ].map(x=>'<div class="profile-metric"><small>'+esc(x[0])+'</small><b>'+esc(x[1])+'</b></div>').join("")+'</div>'+
   '<div class="sample-warning">'+esc(rs.warning||"Correlated exposure is aggregated by futures symbol and direction.")+'</div>'+
   (groups.length?'<div class="profile-history"><table><thead><tr><th>SYMBOL / DIRECTION</th><th>OPEN SIGNALS</th><th>NOTIONAL</th><th>STOP RISK</th><th>LIVE NET PNL</th><th>LEADERS</th></tr></thead><tbody>'+
    groups.map(g=>'<tr><td><b>'+esc(g.symbol)+'</b> <span class="'+(g.side==="LONG"?"side-long":"side-short")+'">'+esc(g.side)+'</span></td><td>'+fmt(g.open_count,0)+'</td><td>'+money(g.open_notional)+'</td><td class="negative">'+money(g.estimated_stop_loss_usdt)+'</td><td class="'+(g.current_net_pnl>=0?"positive":"negative")+'">'+money(g.current_net_pnl)+'</td><td>'+esc((g.leaders||[]).join(", "))+'</td></tr>').join("")+
    '</tbody></table></div>':'<div class="empty-state">No overlapping open symbol-direction clusters.</div>');
  if(!d.trades.length){$("paperTrades").innerHTML='<tr><td colspan="11" class="empty-cell">No real opportunity has qualified. No demo trades are generated.</td></tr>';return;}
  $("paperTrades").innerHTML=d.trades.map(t=>{
   const statusText=t.status==="OPEN"?"OPEN":"CLOSED";
   const quality=t.data_quality_status==="INFORMATION_CUTOFF"?"INFORMATION CUTOFF":
     t.data_quality_status==="DATA_INTERRUPTED_REVIEW"?"DATA INTERRUPTED":
     t.data_quality_status==="PATH_NOT_RECORDED"?"PATH NOT RECORDED":"";
   const funding=t.funding_history_verified===true?"FUNDING VERIFIED":
     t.funding_audit_status==="LEGACY_FUNDING_UNVERIFIED"?"LEGACY FUNDING UNKNOWN":"FUNDING PENDING";
   return '<tr class="clickable-row" onclick="openPaperTrade('+t.id+')">'+
    '<td><span class="status-tag '+(t.status==="OPEN"?"qualified":"neutral")+'">'+statusText+'</span>'+
      (quality?'<br><small class="muted-note">'+esc(quality)+'</small>':'')+'</td>'+
    '<td><b>'+esc(t.trader_name||t.trader_id)+'</b><br><small>'+esc(t.trader_id)+'</small><br><small>Position: '+esc(t.leader_position_key||"unlinked")+'</small></td>'+
    '<td><b>'+esc(t.symbol)+'</b></td>'+
    '<td class="'+(t.side==="LONG"?"side-long":"side-short")+'">'+esc(t.side)+'</td>'+
    '<td>'+fmt(t.entry_price,8)+' / '+fmt(t.mark_price,8)+'</td>'+
    '<td>'+money(t.notional)+'</td>'+
    '<td>'+money(t.margin_used)+' · '+fmt(t.leverage,1)+'×</td>'+
    '<td class="'+(Number(t.net_pnl)>=0?"positive":"negative")+'">'+money(t.net_pnl)+
      '<br><small>MFE '+money(t.mfe_net_pnl)+' · MAE '+money(t.mae_net_pnl)+'</small>'+
      '<br><small>'+esc(funding)+'</small></td>'+
    '<td>'+fmt(t.stop_price,8)+' / '+fmt(t.target_price,8)+'</td>'+
    '<td>'+date(t.opened_at)+'</td>'+
    '<td><button class="btn btn-subtle" type="button" onclick="event.stopPropagation();openPaperTrade('+t.id+')">View leader & chart</button></td>'+
   '</tr>';
  }).join("");
 }catch(e){notice("Paper ledger: "+e.message,"error");}
}
function paperChartSvg(d){
 const candles=d.candles||[], path=d.path||[], leaderPath=d.leader_path||[], t=d.trade||{}, l=d.leader||{};
 const ts=v=>{const x=typeof v==="number"?v:Date.parse(v||"");return Number.isFinite(x)?x:null;};
 const times=[];
 candles.forEach(c=>{if(ts(c.open_time)!==null)times.push(ts(c.open_time));if(ts(c.close_time)!==null)times.push(ts(c.close_time));});
 path.forEach(p=>{if(ts(p.at)!==null)times.push(ts(p.at));});
 leaderPath.forEach(p=>{if(ts(p.at)!==null)times.push(ts(p.at));});
 [t.opened_at,t.closed_at].forEach(v=>{if(ts(v)!==null)times.push(ts(v));});
 if(!times.length)return '<div class="empty-state">No timestamped market candles or trade path are available for this instrument.</div>';
 const t0=Math.min(...times),t1=Math.max(...times),tspan=Math.max(1,t1-t0);
 const prices=[];
 candles.forEach(c=>[c.high,c.low,c.open,c.close].forEach(v=>{if(Number.isFinite(Number(v)))prices.push(Number(v));}));
 path.forEach(p=>{if(Number.isFinite(Number(p.paper_price)))prices.push(Number(p.paper_price));});
 leaderPath.forEach(p=>{if(Number.isFinite(Number(p.mark_price)))prices.push(Number(p.mark_price));});
 [t.entry_price,t.exit_price,t.stop_price,t.target_price,l.entry_price].forEach(v=>{if(Number.isFinite(Number(v)))prices.push(Number(v));});
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
 hline(t.stop_price,"#e36a74","Paper stop");
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
   ['Leader status',l.status||'NOT OBSERVED'],
   ['Our entry',fmt(t.entry_price,8)],['Current / exit mark',fmt(t.mark_price,8)],
   ['Gross PnL',money(t.gross_pnl)],['Round-trip costs',money(t.estimated_costs)],
   ['Funding audit',t.funding_audit_status||'LEGACY UNVERIFIED'],
   ['Actual funding',t.funding_cost_actual===null||t.funding_cost_actual===undefined?'Not verified':money(t.funding_cost_actual)],
   ['Net realized / live PnL',money(t.net_pnl)],['Exit reason',t.exit_reason||'—'],
   ['Exit price',fmt(t.exit_price,8)],['MFE net',money(t.mfe_net_pnl)+' · '+pct(t.mfe_pct)],
   ['MAE net',money(t.mae_net_pnl)+' · '+pct(t.mae_pct)],
   ['Max saved data gap',fmt(t.max_data_gap_seconds,1)+' sec'],
   ['Data classification',t.data_quality_status||'UNCLASSIFIED'],
   ['Last Binance mark',date(t.last_binance_price_at)]
  ].map(x=>'<div class="profile-metric"><small>'+esc(x[0])+'</small><b>'+esc(x[1])+'</b></div>').join('')+'</div>';
  html+='<div class="analysis-box"><h4>LINKED LEADER POSITION</h4>'+
   '<div class="analysis-line"><span>Leader name / ID</span><b>'+esc(l.trader_name||d.trader?.name||'—')+' / '+esc(l.trader_id||t.trader_id||'—')+'</b></div>'+
   '<div class="analysis-line"><span>Exact position key</span><b>'+esc(l.position_key||t.leader_position_key||'—')+'</b></div>'+
   '<div class="analysis-line"><span>Leader side / leverage / margin</span><b>'+esc(l.side||t.side||'—')+' · '+fmt(l.leverage,2)+'× / '+esc(l.margin_mode||'Not reported')+'</b></div>'+
   '<div class="analysis-line"><span>Leader entry / recovery price</span><b>'+fmt(l.entry_price,8)+'</b></div>'+
   '<div class="analysis-line"><span>Leader latest mark</span><b>'+fmt(l.mark_price,8)+' · '+date(l.observed_at)+'</b></div>'+
   '<div class="analysis-line"><span>Leader unrealized PnL / adverse move</span><b>'+money(l.unrealized_pnl)+' / '+pct(l.adverse_pct)+'</b></div>'+
   '<div class="analysis-line"><span>Our PnL if leader reaches entry</span><b>'+money(l.our_recovery_profit_usdt)+' · '+pct(l.our_recovery_profit_pct)+'</b></div>'+
   '<div class="analysis-line"><span>Comparable recovery sample</span><b>'+fmt(r.probability,1)+'% · '+fmt(r.sample_size,0)+' samples</b></div></div>';
  html+='<div class="analysis-box"><h4>LIVE FUTURES CANDLE CHART · '+esc(t.symbol||'')+' · '+esc(d.candle_interval||'')+'</h4>'+
   paperChartSvg(d)+
   '<div class="muted-note">Candles are Binance USD-M Futures market data. Purple = our observed price path and entry. Green = Kopyon leader mark observations and recovery price. Vertical lines = linked paper entries/exits. Refreshes while this trade detail remains open.'+
   (d.candle_error?' Candle fetch warning: '+esc(d.candle_error):'')+'</div></div>';
  html+='<div class="analysis-box"><h4>SAME LEADER POSITION · HARVEST CAMPAIGN</h4>'+
   '<div class="analysis-line"><span>Paper attempts / re-entries</span><b>'+fmt(camp.trade_count,0)+' / '+fmt(camp.reentry_count,0)+'</b></div>'+
   '<div class="analysis-line"><span>Currently open attempts</span><b>'+fmt(camp.open_count,0)+'</b></div>'+
   '<div class="analysis-line"><span>Realized campaign net PnL</span><b>'+money(camp.realized_net_pnl)+'</b></div>'+
   '<div class="analysis-line"><span>Total campaign net PnL</span><b>'+money(camp.total_net_pnl)+'</b></div>'+
   '<div class="sample-warning">Linked trade IDs: '+esc((camp.trade_ids||[]).join(', '))+'</div></div>';
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
 },10000);
}
window.openPaperTrade=openPaperTrade;
$("reloadPaper").onclick=loadPaper;
$("paperSettings").addEventListener("submit",async e=>{e.preventDefault();const f=new FormData(e.currentTarget),body={};for(const [k,v] of f.entries())body[k]=Number(v);body.auto_enabled=e.currentTarget.elements.auto_enabled.checked;try{await api("/api/paper/settings",{method:"POST",body:JSON.stringify(body)});$("paperSaveStatus").textContent="Saved · paper settings active";await loadPaper();}catch(err){$("paperSaveStatus").textContent=err.message;}});
function spark(values){if(!values||values.length<2)return'<div class="empty-state">Candle series unavailable</div>';const min=Math.min(...values),max=Math.max(...values),span=max-min||1;const pts=values.map((v,i)=>(i/(values.length-1)*100)+","+(95-(v-min)/span*85)).join(" ");return'<svg viewBox="0 0 100 100" preserveAspectRatio="none"><polyline points="'+pts+'" fill="none" stroke="#a995f3" stroke-width="1.8" vector-effect="non-scaling-stroke"/><line x1="0" y1="95" x2="100" y2="95" stroke="#354454" stroke-width=".6"/></svg>';}
async function loadMarket(){const s=$("marketSymbol").value.trim().toUpperCase();if(!s){notice("Enter a futures symbol such as BTCUSDT.","error");return;}$("marketResult").innerHTML='<div class="empty-state">Pulling seven days of candles and current market data…</div>';try{const [m,n]=await Promise.all([api("/api/market/"+encodeURIComponent(s)+"?refresh=true"),api("/api/news?symbol="+encodeURIComponent(s))]);
 const items=[["MARK PRICE",fmt(m.mark_price,6)],["7-DAY RETURN",pct(m.seven_day_return_pct)],["24-HOUR RETURN",pct(m.twenty_four_hour_return_pct)],["FUNDING RATE",m.funding_rate===null?"—":pct(m.funding_rate*100)],["24H ATR / PRICE",pct(m.atr_24h_pct)],["1H RSI(14)",fmt(m.rsi_1h,1)],["5M MOMENTUM",pct(m.momentum_5m_pct)],["INDEX PRICE",fmt(m.index_price,6)]];
 $("marketResult").innerHTML='<div class="market-grid">'+items.map(x=>'<div class="market-stat"><small>'+x[0]+'</small><b>'+x[1]+'</b></div>').join("")+'</div><div class="market-chart"><div class="section-kicker">LAST 30 HOURLY CLOSES · SOURCE: BINANCE</div><div class="sparkline">'+spark(m.closes_1h)+'</div><div class="muted-note">Fetched '+date(m.fetched_at)+' · 168 one-hour candles requested for the seven-day window.</div></div>';
 $("newsList").innerHTML=n.items?.length?n.items.map(i=>'<div class="news-item"><a href="'+esc(i.url)+'" target="_blank" rel="noreferrer">'+esc(i.title)+'</a><small>'+esc(i.source)+' · '+esc(i.published_at||"Publication time unavailable")+'</small></div>').join(""):'<div class="empty-state">No matched headlines returned. This is not proof that no news event occurred.</div>';
 }catch(e){$("marketResult").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}}
$("loadMarket").onclick=loadMarket;$("marketSymbol").addEventListener("keydown",e=>{if(e.key==="Enter")loadMarket();});
async function loadSystem(){$("tableMetrics").innerHTML='<div class="empty-state">Reading database telemetry…</div>';try{const d=await api("/api/system"),db=d.database||{},m=d.monitor||{};$("dbEngine").textContent=(db.engine||"unknown").toUpperCase();$("dbUsedLarge").textContent=db.used_mb===null||db.used_mb===undefined?"Not reported":fmt(db.used_mb,3)+" MB";$("dbConnection").textContent="Connection: "+db.connection+" · persistent: "+(db.persistent?"yes":"no");$("dbCapacityLarge").textContent=db.capacity_note||"";$("dbBarLarge").style.width=db.configured_capacity_gb&&db.used_mb!==null?Math.min(100,db.used_mb/(db.configured_capacity_gb*1024)*100)+"%":"0%";$("workerState").textContent=m.worker_alive?"Running":"Not active";$("workerInterval").textContent=Math.round((m.interval_seconds||0)/60)+" minutes";$("workerBatch").textContent=(m.batch||0)+" traders / pass";$("workerLastState").textContent=m.state||"—";$("workerError").textContent=m.last_error||"None";$("runtimeCaveat").textContent=d.runtime?.render_free_sleep_warning||"";$("tableMetrics").innerHTML=db.tables?.length?db.tables.map(t=>'<div class="table-metric"><b>'+esc(t.name)+'</b><small>'+(t.bytes===null?"Size unavailable":fmt(t.bytes/1024,1)+" KB")+'</small></div>').join(""):'<div class="empty-state">No table-size breakdown returned.</div>';$("rowCounts").innerHTML=Object.entries(d.counts||{}).map(([k,v])=>'<div class="row-count"><span>'+esc(k.replaceAll("_"," "))+'</span><b>'+fmt(v,0)+'</b></div>').join("");}catch(e){$("tableMetrics").innerHTML='<div class="notice error">'+esc(e.message)+'</div>';}}
$("reloadSystem").onclick=loadSystem;
$("scanNow").onclick=async()=>{$("scanNow").disabled=true;$("scanNow").textContent="Scanning…";try{const d=await api("/api/sync/now",{method:"POST"});notice("Monitor pass complete · "+fmt(d.traders_found,0)+" leaders updated · "+fmt(d.positions_checked,0)+" portfolios checked · "+fmt(d.observations_saved,0)+" positions saved.","success");await refreshOverview();if(state.page==="traders")await loadTraders();if(state.page==="positions")await loadPositions();}catch(e){notice(e.message,"error");}finally{$("scanNow").disabled=false;$("scanNow").textContent="↻ Scan now";}};
async function boot(){await refreshOverview();try{await loadTraders();}catch{}setInterval(async()=>{await refreshOverview();if(state.page==="paper")await loadPaper();if(state.page==="positions")await loadPositions();if(state.page==="traders")await loadTraders();if(state.page==="opportunities")await loadOpportunities();},60000);}
boot();