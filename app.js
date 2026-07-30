const API = '';
let currentDate = new Date();
let currentGame = null;
let currentGamePk = null;
let activeTab = 'game';
let refreshTimer = null;
let feedStream = null;
let feedStreamWatchdog = null;
let feedRequest = null;
let feedSession = 0;
let streamHasDelivered = false;
let lastFeedVersion = null;
let lastFeedOrder = 0;
let teamsCache = {};
let lastPlayIndex = -1;
let lastAnimatedPitchEventId = null;
let lastPitchContextKey = null;
let lastActiveAtBatIndex = null;
let activePlayAheadOfLinescore = false;
let transientAlertsInitialized = false;
let seenTransientAlertKeys = new Set();
let gameAlertQueue = [];
let gameAlertTimer = null;
let gameAlertExitTimer = null;
let pitchAnimationUntil = 0;
let deferredGameRenderTimer = null;
let deferredGameRenderData = null;
let lastCompletedPlayAtBat = null;
let lastCompletedPlayTime = 0;
const PLAY_RESULT_HOLD_MS = 2500;
const FEED_FALLBACK_MS = 500;
const FEED_FALLBACK_REQUEST_TIMEOUT_MS = 900;
const FEED_COLD_START_TIMEOUT_MS = 5000;
const FEED_STREAM_TIMEOUT_MS = 3500;

const TEAM_COLORS = {
    ARI:{c:'#A71930',id:109},ATL:{c:'#CE1141',id:144},BAL:{c:'#DF4601',id:110},
    BOS:{c:'#BD3039',id:111},CHC:{c:'#0E3386',id:112},CHW:{c:'#27251F',id:145},
    CIN:{c:'#C6011F',id:113},CLE:{c:'#00385D',id:114},COL:{c:'#33006F',id:115},
    DET:{c:'#0C2340',id:116},HOU:{c:'#EB6E1F',id:117},KC:{c:'#004687',id:118},
    LAA:{c:'#BA0021',id:108},LAD:{c:'#005A9C',id:119},MIA:{c:'#00A3E0',id:146},
    MIL:{c:'#FFC52F',id:158},MIN:{c:'#002B5C',id:142},NYM:{c:'#002D72',id:121},
    NYY:{c:'#003087',id:147},OAK:{c:'#003831',id:133},PHI:{c:'#E81828',id:143},
    PIT:{c:'#27251F',id:134},SD:{c:'#2F241D',id:135},SEA:{c:'#0C2C56',id:136},
    SF:{c:'#FD5A1E',id:137},STL:{c:'#C41E3A',id:138},TB:{c:'#092C5C',id:139},
    TEX:{c:'#003278',id:140},TOR:{c:'#134A8E',id:141},WSH:{c:'#AB0003',id:120},
    AZ:{c:'#A71930',id:109},CWS:{c:'#27251F',id:145},ATH:{c:'#003831',id:133},
};
function teamColor(a){return(TEAM_COLORS[a]||{c:'#555'}).c;}
function teamId(a){return(TEAM_COLORS[a]||{}).id;}
function teamLogoUrl(a,id){const t=id||teamId(a);return t?`https://www.mlbstatic.com/team-logos/${t}.svg`:'';}
function playerHeadshotUrl(p,size=96){
    const width=Math.max(80,Math.round(size));
    return p?`https://img.mlbstatic.com/mlb-photos/image/upload/w_${width},q_auto:best/v1/people/${p}/headshot/67/current`:'';
}
function teamLogoImg(a,id,s){
    s=s||24;const u=teamLogoUrl(a,id);
    return u?`<img src="${u}" alt="${a}" width="${s}" height="${s}" style="object-fit:contain;background:transparent" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'"><span class="team-logo" style="background:${teamColor(a)};display:none">${a?.[0]||'?'}</span>`:`<span class="team-logo" style="background:${teamColor(a)}">${a?.[0]||'?'}</span>`;
}

let schedulePollTimer = null;
const SCHEDULE_POLL_MS = 10000;

document.addEventListener('DOMContentLoaded',async()=>{
    await loadTeams();renderDatePicker();loadGames();
    document.getElementById('standingsBtn').addEventListener('click',showStandings);
    startSchedulePoll();
});
document.addEventListener('visibilitychange',()=>{
    if(document.hidden){stopGameFeed();stopSchedulePoll();return;}
    if(currentGamePk)startGameFeed();else startSchedulePoll();
});
function startSchedulePoll(){
    stopSchedulePoll();
    if(currentGamePk)return;
    schedulePollTimer=setInterval(()=>{if(!currentGamePk&&!document.hidden)loadGames(true);},SCHEDULE_POLL_MS);
}
function stopSchedulePoll(){if(schedulePollTimer){clearInterval(schedulePollTimer);schedulePollTimer=null;}}
async function loadTeams(){try{const r=await fetch(`${API}/api/teams`);teamsCache=await r.json();}catch(e){}}

function renderDatePicker(){
    const p=document.getElementById('datePicker');p.innerHTML='';const t=new Date();
    const days=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    for(let i=-3;i<=4;i++){
        const d=new Date(t);d.setDate(t.getDate()+i);
        const sel=d.toDateString()===currentDate.toDateString();
        const b=document.createElement('button');b.className='date-item'+(sel?' active':'');
        b.innerHTML=`<span class="day-name">${days[d.getDay()]}</span><span class="day-num">${d.getDate()}</span>`;
        b.addEventListener('click',()=>{currentDate=d;renderDatePicker();loadGames();});
        p.appendChild(b);
    }
    const a=p.querySelector('.active');if(a)a.scrollIntoView({behavior:'smooth',inline:'center',block:'nearest'});
}
function fmtDate(d){return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');}

async function loadGames(silent=false){
    if(!silent)showLoading();
    try{const r=await fetch(`${API}/api/schedule?date=${fmtDate(currentDate)}`);const d=await r.json();if(!silent)hideLoading();renderGames(d.games);}
    catch(e){if(!silent){hideLoading();document.getElementById('emptyState').style.display='flex';document.getElementById('emptyState').querySelector('p').textContent='Could not load games';document.getElementById('emptyState').querySelector('span').textContent=e.message;}}
}

function renderGames(games){
    const list=document.getElementById('gamesList');list.innerHTML='';document.getElementById('emptyState').style.display='none';
    if(!games||games.length===0){document.getElementById('emptyState').style.display='flex';return;}
    games.forEach(g=>{
        const aw=g.away,hm=g.home,ls=g.linescore,st=g.status;
        const isLive=st.abstract==='Live',isFinal=st.abstract==='Final',isDelayed=st.detailed==='Delayed',isPreview=st.abstract==='Preview';
        let inningText='';
        if(isLive){const innN=ls.inning||'';const halfL=ls.inningState==='Middle'?'Mid':ls.inningState==='End'?'End':ls.isTopInning?'Top':'Bot';inningText=`${halfL} ${innN}`;}
        else if(isFinal){inningText='Final';if(ls.inning>9)inningText+=' / '+ls.inning;}
        else if(isDelayed)inningText='Delayed';
        else if(isPreview){const gt=new Date(g.gameDate);inningText=gt.toLocaleTimeString([],{hour:'numeric',minute:'2-digit'});}
        const awW=isFinal&&aw.score>hm.score,hmW=isFinal&&hm.score>aw.score;
        const c=document.createElement('div');c.className='game-card'+(isLive?' live':'');
        c.innerHTML=`
            <div class="gc-status"><span class="gc-inning ${isLive?'live':''} ${isFinal?'final':''} ${isDelayed?'status-delayed':''}">${inningText}</span></div>
            <div class="gc-team-row"><div class="gc-team-left">${teamLogoImg(aw.abbr,aw.id,24)}<div class="gc-team-info"><span class="team-abbr">${aw.abbr}</span><span class="team-name">${aw.name}</span></div></div><span class="gc-score ${awW?'winner':''} ${isFinal&&!awW?'loser':''}">${isPreview?'':(aw.score??'')}</span></div>
            <div class="gc-team-row"><div class="gc-team-left">${teamLogoImg(hm.abbr,hm.id,24)}<div class="gc-team-info"><span class="team-abbr">${hm.abbr}</span><span class="team-name">${hm.name}</span></div></div><span class="gc-score ${hmW?'winner':''} ${isFinal&&!hmW?'loser':''}">${isPreview?'':(hm.score??'')}</span></div>
            <div class="gc-bottom">${(isLive||isFinal)?`<div class="diamond"><div class="diamond-base first ${g.bases.first?'occupied':''}"></div><div class="diamond-base second ${g.bases.second?'occupied':''}"></div><div class="diamond-base third ${g.bases.third?'occupied':''}"></div></div><div class="bso-dots"><div class="bso-group"><div class="bso-circles">${[1,2,3,4].map(b=>`<div class="bso-circle ${b<=ls.balls?'filled-ball':''}"></div>`).join('')}</div><span class="bso-label">B</span></div><div class="bso-group"><div class="bso-circles">${[1,2,3].map(s=>`<div class="bso-circle ${s<=ls.strikes?'filled-strike':''}"></div>`).join('')}</div><span class="bso-label">S</span></div><div class="bso-group"><div class="bso-circles">${[1,2,3].map(o=>`<div class="bso-circle ${o<=ls.outs?'filled-out':''}"></div>`).join('')}</div><span class="bso-label">O</span></div></div>`:''}
            ${isDelayed?'<span style="font-size:10px;color:var(--live-yellow)">Weather delay</span>':''}
            ${(isPreview&&g.venue)?`<span style="font-size:10px;color:var(--text-muted)">${g.venue}</span>`:''}
        </div>`;
        c.addEventListener('click',()=>openGameCenter(g.gamePk,aw,hm));list.appendChild(c);
    });
}
function showLoading(){document.getElementById('loadingState').style.display='flex';document.getElementById('gamesList').innerHTML='';document.getElementById('emptyState').style.display='none';}
function hideLoading(){document.getElementById('loadingState').style.display='none';}

async function showStandings(){
    switchScreen('app-standings');
    const c=document.getElementById('standingsContent');
    c.innerHTML='<div class="loading-state"><div class="spinner"></div><p>Loading standings...</p></div>';
    try{const r=await fetch(`${API}/api/standings`);const d=await r.json();c.innerHTML='';
    Object.entries(d).forEach(([div,teams])=>{const s=document.createElement('div');s.className='standings-section';
    s.innerHTML=`<h3>${div}</h3><table class="standings-table"><thead><tr><th>Team</th><th>W</th><th>L</th><th>PCT</th><th>GB</th><th>Strk</th></tr></thead><tbody>${teams.map(t=>`<tr><td><span class="standings-team">${teamLogoImg(t.abbr,null,22)} ${t.abbr}</span></td><td>${t.w}</td><td>${t.l}</td><td>${t.pct}</td><td>${t.gb}</td><td>${t.streak}</td></tr>`).join('')}</tbody></table>`;c.appendChild(s);});}
    catch(e){c.innerHTML='<div class="empty-state"><p>Failed to load standings</p></div>';}
}
function showScores(){switchScreen('app-scores');}

function switchScreen(id){
    document.querySelectorAll('.screen').forEach(s=>s.classList.remove('active'));
    document.getElementById(id).classList.add('active');
    if(id!=='app-gamecenter')stopGameFeed();
}

function openGameCenter(pk,away,home){
    stopGameFeed();stopSchedulePoll();
    resetTransientAlerts();
    currentGamePk=pk;currentGame={away,home};activeTab='game';lastFeedData=null;lastFeedVersion=null;lastFeedOrder=0;lastPlayIndex=-1;lastAnimatedPitchEventId=null;lastPitchContextKey=null;lastActiveAtBatIndex=null;activePlayAheadOfLinescore=false;switchScreen('app-gamecenter');
    document.getElementById('gcContent').innerHTML='<div class="loading-state" id="gcLoader"><div class="spinner"></div><p>Loading game...</p></div>';
    renderGCTabs(away,home);
    startGameFeed();
}
function renderGCTabs(away,home){document.getElementById('gcTabs').innerHTML=`<button class="gc-tab active" data-tab="game" onclick="showGCPanel('game')">Feed</button><button class="gc-tab" data-tab="away" onclick="showGCPanel('away')">${away.abbr}</button><button class="gc-tab" data-tab="home" onclick="showGCPanel('home')">${home.abbr}</button>`;}
function toggleGCPanel(tab){
    document.querySelectorAll('.gc-tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===tab));
    document.querySelectorAll('.gc-panel').forEach(p=>p.classList.toggle('active',p.id==='panel-'+tab));
}
function showGCPanel(tab){
    activeTab=tab;
    if(lastFeedData){
        if(tab==='game')renderGameTab(lastFeedData);
        else if(tab==='away')renderTeamTab(lastFeedData,'away');
        else if(tab==='home')renderTeamTab(lastFeedData,'home');
    }
    toggleGCPanel(tab);
}
function closeGameCenter(){currentGamePk=null;switchScreen('app-scores');startSchedulePoll();}

let lastFeedData=null;
function stopGameFeed(){
    feedSession++;
    streamHasDelivered=false;
    delete document.documentElement.dataset.feedTransport;
    delete document.documentElement.dataset.feedVersion;
    if(feedStream){feedStream.close();feedStream=null;}
    if(feedStreamWatchdog!==null){clearTimeout(feedStreamWatchdog);feedStreamWatchdog=null;}
    if(refreshTimer!==null){clearTimeout(refreshTimer);refreshTimer=null;}
    if(feedRequest){feedRequest.controller.abort();feedRequest=null;}
    if(deferredGameRenderTimer!==null){clearTimeout(deferredGameRenderTimer);deferredGameRenderTimer=null;}
    deferredGameRenderData=null;
    pitchAnimationUntil=0;
    clearGameAlertToast(true);
}
function markFeedStreamAlive(session){
    if(session!==feedSession)return;
    streamHasDelivered=true;
    document.documentElement.dataset.feedTransport='stream';
    if(refreshTimer!==null){clearTimeout(refreshTimer);refreshTimer=null;}
    if(feedRequest?.session===session){feedRequest.controller.abort();feedRequest=null;}
    if(feedStreamWatchdog!==null)clearTimeout(feedStreamWatchdog);
    feedStreamWatchdog=setTimeout(()=>{
        if(session!==feedSession)return;
        streamHasDelivered=false;
        scheduleFallbackPoll(session,0);
    },FEED_STREAM_TIMEOUT_MS);
}
function scheduleFallbackPoll(session,delay=FEED_FALLBACK_MS){
    if(document.hidden||session!==feedSession||!currentGamePk||streamHasDelivered||refreshTimer!==null||feedRequest?.session===session)return;
    refreshTimer=setTimeout(async()=>{
        refreshTimer=null;
        const cycleStartedAt=performance.now();
        document.documentElement.dataset.feedTransport='polling';
        const timeoutMs=lastFeedData?FEED_FALLBACK_REQUEST_TIMEOUT_MS:FEED_COLD_START_TIMEOUT_MS;
        await loadGameFeed(session,timeoutMs);
        const elapsed=performance.now()-cycleStartedAt;
        scheduleFallbackPoll(session,Math.max(0,FEED_FALLBACK_MS-elapsed));
    },delay);
}
function startGameFeed(){
    const session=feedSession,pk=currentGamePk;
    if(document.hidden||!pk)return;
    document.documentElement.dataset.feedTransport='connecting';
    if('EventSource'in window){
        const source=new EventSource(`/api/game/${pk}/stream`);
        feedStream=source;
        source.addEventListener('feed',event=>{
            if(session!==feedSession||pk!==currentGamePk)return;
            try{
                const data=JSON.parse(event.data);
                applyGameFeed(data,session);
                markFeedStreamAlive(session);
            }catch(e){console.error('Feed parse error:',e);}
        });
        source.addEventListener('heartbeat',event=>{
            if(session!==feedSession)return;
            let degraded=false;
            try{degraded=Boolean(JSON.parse(event.data||'{}').degraded);}catch(e){}
            if(degraded){
                streamHasDelivered=false;
                document.documentElement.dataset.feedTransport='recovering';
                scheduleFallbackPoll(session,0);
                return;
            }
            markFeedStreamAlive(session);
        });
        source.addEventListener('feed-error',()=>{
            if(session!==feedSession)return;
            streamHasDelivered=false;
            scheduleFallbackPoll(session,0);
        });
        source.onerror=()=>{
            if(session!==feedSession)return;
            streamHasDelivered=false;
            scheduleFallbackPoll(session,0);
        };
    }
    // Start one request immediately as both a fast first paint and an SSE fallback.
    scheduleFallbackPoll(session,0);
}
async function loadGameFeed(session=feedSession,timeoutMs=lastFeedData?FEED_FALLBACK_REQUEST_TIMEOUT_MS:FEED_COLD_START_TIMEOUT_MS){
    if(document.hidden||session!==feedSession||!currentGamePk)return;
    if(feedRequest?.session===session)return;
    if(feedRequest)feedRequest.controller.abort();
    const pk=currentGamePk,controller=new AbortController(),request={session,controller};
    request.timeout=setTimeout(()=>controller.abort(),timeoutMs);
    feedRequest=request;
    try{const r=await fetch(`/api/game/${pk}/feed`,{cache:'no-store',signal:controller.signal,headers:{Accept:'application/json'}});
    if(!r.ok)throw new Error(r.status);
    const d=await r.json();
    if(session!==feedSession||pk!==currentGamePk)return;
    applyGameFeed(d,session);}
    catch(e){if(e.name!=='AbortError')console.error('Feed error:',e);}
    finally{clearTimeout(request.timeout);if(feedRequest===request)feedRequest=null;}
}
function applyGameFeed(d,session=feedSession){
    if(session!==feedSession||!currentGamePk||d.error)return false;
    const feedOrder=Number(d.feedOrder)||0;
    if(feedOrder&&feedOrder<=lastFeedOrder&&d.feedVersion!==lastFeedVersion)return false;
    if(d.feedVersion&&d.feedVersion===lastFeedVersion)return true;
    if(feedOrder)lastFeedOrder=feedOrder;
    lastFeedVersion=d.feedVersion||null;
    if(d.feedVersion)document.documentElement.dataset.feedVersion=d.feedVersion;
    const loader=document.getElementById('gcLoader');
    if(loader)loader.remove();
    lastFeedData=d;
    processTransientAlerts(d);
    renderGCHeader(d);
    if(activeTab==='game')renderGameTabWithoutInterruptingPitch(d);
    else if(activeTab==='away')renderTeamTab(d,'away');
    else if(activeTab==='home')renderTeamTab(d,'home');
    toggleGCPanel(activeTab);
    return true;
}

function renderGCHeader(data){
    const aw=currentGame.away,hm=currentGame.home,ls=data.linescore,st=data.status;
    const isLive=st?.abstractGameState==='Live',isFinal=st?.abstractGameState==='Final';
    const awS=ls.score?.away??(data.boxscore?.away?.batters?calcScore(data.boxscore.away):'?');
    const hmS=ls.score?.home??(data.boxscore?.home?.batters?calcScore(data.boxscore.home):'?');
    let statusHtml='';
    if(isLive){
        const innNum=ls.inning||'';
        const halfLabel=ls.inningState==='Middle'?'Mid':ls.inningState==='End'?'End':ls.isTopInning?'Top':'Bot';
        statusHtml=`<span class="gc-header-status live"><span class="live-dot"></span>${halfLabel} ${innNum}</span><span class="outs" style="color:var(--text-secondary);margin-left:6px">${ls.outs} out${ls.outs!==1?'s':''}</span>`;
    }
    else if(isFinal)statusHtml=`<span class="gc-header-status" style="color:var(--text-muted)">Final</span>`;
    else statusHtml=`<span class="gc-header-status" style="color:var(--text-secondary)">${st?.detailedState||''}</span>`;
    document.getElementById('gcHeader').innerHTML=`
        <div class="gc-header-top"><button class="gc-back-btn" onclick="closeGameCenter()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 12H5"/><path d="M12 19l-7-7 7-7"/></svg>Back</button><div class="gc-header-status-center">${statusHtml}</div><div style="width:60px"></div></div>
        <div class="gc-header-teams">
            <div class="gc-team-header">${teamLogoImg(aw.abbr,aw.id,36)}<div class="team-info"><span class="team-abbr">${aw.abbr}</span><span class="team-name">${aw.name}</span></div></div>
            <span class="gc-header-score ${isFinal&&Number(awS)>=Number(hmS)?'winner':''}">${awS}</span><span class="gc-header-dash">-</span><span class="gc-header-score ${isFinal&&Number(hmS)>=Number(awS)?'winner':''}">${hmS}</span>
            <div class="gc-team-header right">${teamLogoImg(hm.abbr,hm.id,36)}<div class="team-info"><span class="team-abbr">${hm.abbr}</span><span class="team-name">${hm.name}</span></div></div>
        </div>`;
}
function calcScore(tb){let r=0;(tb.batters||[]).forEach(b=>r+=b.r||0);return r||0;}
function uniquePitchEvents(events){
    const seen=new Set();
    return(events||[]).filter(p=>{
        if(!p.isPitch)return false;
        const key=p.eventId||`${p.pitchNumber||''}|${p.description||''}|${p.startSpeed||''}`;
        if(seen.has(key))return false;
        seen.add(key);
        return true;
    });
}
function latestPitchEventId(data){
    const pitches=uniquePitchEvents(data?.currentPlay?.pitches);
    const latest=pitches[pitches.length-1];
    return latest?.eventId?String(latest.eventId):'';
}
function flushDeferredGameRender(){
    if(deferredGameRenderTimer!==null){clearTimeout(deferredGameRenderTimer);deferredGameRenderTimer=null;}
    const data=deferredGameRenderData;
    deferredGameRenderData=null;
    pitchAnimationUntil=0;
    if(data&&activeTab==='game'&&currentGamePk===data.gamePk)renderGameTab(data);
}
function renderGameTabWithoutInterruptingPitch(data){
    const sameAnimatingPitch=performance.now()<pitchAnimationUntil
        &&latestPitchEventId(data)
        &&latestPitchEventId(data)===lastAnimatedPitchEventId;
    if(sameAnimatingPitch){
        deferredGameRenderData=data;
        if(deferredGameRenderTimer!==null)clearTimeout(deferredGameRenderTimer);
        deferredGameRenderTimer=setTimeout(
            flushDeferredGameRender,
            Math.max(16,pitchAnimationUntil-performance.now())
        );
        return;
    }
    if(deferredGameRenderTimer!==null){clearTimeout(deferredGameRenderTimer);deferredGameRenderTimer=null;}
    deferredGameRenderData=null;
    renderGameTab(data);
}

const NON_PLAY_ALERT_TYPES=new Set([
    'batter-timeout','pitch-timer','pitch-timer-violation','pitch-clock-violation',
    'mound','mound-visit','pickoff','stepoff','pitcher-change','pitching-change',
    'pitching-substitution','offensive-substitution','defensive-substitution',
    'defensive-switch','sub','review','replay','challenge','injury','delay',
    'abs-challenge','strikeout-abs-challenge'
]);
const NON_PLAY_ALERT_TEXT=/\b(batter timeout|pitch(?:er)? (?:timer|clock) violation|mound visit|pitching (?:change|substitution)|offensive substitution|defensive (?:substitution|switch)|replay review|manager challenge|umpire review|injury delay|rain delay|abs challenge|automated ball-strike)\b/i;
function normalizeAlertType(value){
    return String(value||'').trim().toLowerCase().replace(/[\s_]+/g,'-');
}
function isNonPlayAlert(type,text=''){
    const normalized=normalizeAlertType(type);
    return NON_PLAY_ALERT_TYPES.has(normalized)||NON_PLAY_ALERT_TEXT.test(String(text||''));
}
function escapeAlertText(value){
    return String(value??'').replace(/[&<>"']/g,char=>({
        '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    })[char]);
}
function alertTitleFor(type){
    const titles={
        'batter-timeout':'Batter Timeout',
        'pitch-timer':'Pitch Timer Violation',
        'pitch-timer-violation':'Pitch Timer Violation',
        'pitch-clock-violation':'Pitch Timer Violation',
        'mound':'Mound Visit',
        'mound-visit':'Mound Visit',
        'pitcher-change':'Pitching Change',
        'pitching-change':'Pitching Change',
        'pitching-substitution':'Pitching Change',
        'offensive-substitution':'Offensive Substitution',
        'defensive-substitution':'Defensive Substitution',
        'defensive-switch':'Defensive Switch',
        'sub':'Substitution',
        'review':'Replay Review',
        'replay':'Replay Review',
        'challenge':'Replay Review',
        'injury':'Injury Update',
        'steal':'Stolen Base',
        'event':'Game Update'
    };
    return titles[type]||type.split('-').filter(Boolean).map(word=>word[0].toUpperCase()+word.slice(1)).join(' ')||'Game Update';
}
function gameAlertKind(type,title,description){
    const value=`${type} ${title} ${description}`.toLowerCase();
    if(/timer|clock|timeout/.test(value))return'timer';
    if(/mound|pickoff|stepoff/.test(value))return'mound';
    if(/change|substitution|switch|\bsub\b/.test(value))return'change';
    if(/review|replay|challenge/.test(value))return'review';
    if(/injury|medical|delay/.test(value))return'medical';
    if(/steal|running/.test(value))return'running';
    return'event';
}
function gameAlertIcon(kind){
    const paths={
        timer:'<circle cx="12" cy="13" r="7.5"/><path d="M12 9v4l2.8 1.7M9.5 3h5M12 3v2"/>',
        mound:'<path d="M4 17c2.4-3.8 4.9-5.7 8-5.7s5.6 1.9 8 5.7"/><circle cx="12" cy="8" r="2.2"/>',
        change:'<path d="M5 8h12l-3-3M19 16H7l3 3"/><path d="m17 5 3 3-3 3M7 13l-3 3 3 3"/>',
        review:'<rect x="3.5" y="5" width="17" height="12" rx="2"/><path d="m10 9 5 2.5-5 2.5zM8 20h8"/>',
        medical:'<path d="M12 20s-7-4.4-7-10a4 4 0 0 1 7-2.6A4 4 0 0 1 19 10c0 5.6-7 10-7 10Z"/><path d="M9 12h6M12 9v6"/>',
        running:'<path d="m8.5 20 2.3-5-2.5-2.2M11 9.2l2.6 2.2 3.4.2M12.5 5.5a1.4 1.4 0 1 0 0 .1M13.6 11.4l-1.2 3.2 4.1 3.6"/>',
        event:'<path d="M12 3 2.8 19h18.4L12 3Z"/><path d="M12 9v4M12 16h.01"/>'
    };
    return`<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${paths[kind]||paths.event}</svg>`;
}
function normalizeTransientAlert(raw,context={}){
    if(!raw||typeof raw!=='object')return null;
    const type=normalizeAlertType(raw.type||raw.eventType||context.type);
    const title=String(raw.title||alertTitleFor(type)).trim();
    const description=String(raw.description||raw.result||'').trim();
    const inning=String(raw.inning||context.inning||'').trim();
    const explicitKey=raw.eventId||raw.key;
    const fallbackKey=[
        context.atBatIndex??'na',raw.eventIndex??context.eventIndex??'na',
        type,title,description,inning
    ].join('|').toLowerCase();
    return{
        key:String(explicitKey||fallbackKey),
        type,
        kind:gameAlertKind(type,title,description),
        title,
        description:description===title?'':description,
        inning
    };
}
function collectHistoricalTransientAlerts(data){
    const alerts=[],collectedKeys=new Set();
    (Array.isArray(data.gameEvents)?data.gameEvents:[]).forEach(raw=>{
        const alert=normalizeTransientAlert(raw);
        if(!alert||collectedKeys.has(alert.key))return;
        collectedKeys.add(alert.key);
        alerts.push(alert);
    });
    return alerts;
}
function collectLiveTransientAlerts(data){
    const alerts=[],collectedKeys=new Set();
    const add=(raw,context)=>{
        const alert=normalizeTransientAlert(raw,context);
        if(!alert||collectedKeys.has(alert.key))return;
        collectedKeys.add(alert.key);
        alerts.push(alert);
    };
    const currentAlerts=Array.isArray(data.currentAlerts)
        ?data.currentAlerts
        :(data.currentAlerts?[data.currentAlerts]:[]);
    currentAlerts.forEach(event=>add(event));
    const play=data.currentPlay;
    if(play&&(data.currentPlayActive===true||play.about?.isComplete!==true)){
        const half=play.about?.halfInning;
        const inning=play.about?.inning;
        const inningLabel=half&&inning?`${half==='top'?'Top':'Bot'} ${inning}`:'';
        (play.pitches||[]).forEach((event,eventIndex)=>{
            if(event.isPitch||!isNonPlayAlert(event.eventType,event.description))return;
            add(event,{atBatIndex:play.atBatIndex,eventIndex,inning:inningLabel});
        });
        const result=play.shortResult||play.result||'';
        if(alerts.length===0&&isNonPlayAlert(play.eventType,result)){
            add({
                eventId:play.eventId||`play:${play.atBatIndex??'na'}:result:${normalizeAlertType(play.eventType)}`,
                eventType:play.eventType,
                description:result
            },{atBatIndex:play.atBatIndex,inning:inningLabel});
        }
    }
    return alerts;
}
function resetTransientAlerts(){
    transientAlertsInitialized=false;
    seenTransientAlertKeys=new Set();
    activeTransientAlert=null;
    activeTransientAlertKey=null;
    clearGameAlertToast(true);
}
function processTransientAlerts(data){
    const historicalAlerts=collectHistoricalTransientAlerts(data);
    const liveAlerts=collectLiveTransientAlerts(data);
    if(!transientAlertsInitialized){
        historicalAlerts.forEach(alert=>seenTransientAlertKeys.add(alert.key));
        liveAlerts.forEach(alert=>seenTransientAlertKeys.add(alert.key));
        transientAlertsInitialized=true;
        return;
    }
    const newLiveAlerts=liveAlerts.filter(alert=>!seenTransientAlertKeys.has(alert.key));
    liveAlerts.forEach(alert=>seenTransientAlertKeys.add(alert.key));
    historicalAlerts.forEach(alert=>seenTransientAlertKeys.add(alert.key));
    newLiveAlerts.forEach(alert=>{
        gameAlertQueue.push(alert);
    });
    showNextGameAlert();
}
function clearGameAlertToast(dropQueue=false){
    if(gameAlertTimer!==null){clearTimeout(gameAlertTimer);gameAlertTimer=null;}
    if(gameAlertExitTimer!==null){clearTimeout(gameAlertExitTimer);gameAlertExitTimer=null;}
    activeTransientAlert=null;
    activeTransientAlertKey=null;
    const region=document.getElementById('gameAlertToastRegion');
    if(region)region.replaceChildren();
    if(dropQueue)gameAlertQueue=[];
}
let activeTransientAlert = null;
let activeTransientAlertKey = null;

function isAlertStillActive(alert){
    if(!lastFeedData)return false;
    const currentPlay=lastFeedData.currentPlay;
    if(!currentPlay||currentPlay.about?.isComplete===true)return false;
    const pitches=currentPlay.pitches||[];
    for(let i=pitches.length-1;i>=0;i--){
        const ev=pitches[i];
        if(ev.isPitch)continue;
        const evType=String(ev.eventType||'').toLowerCase();
        const evDesc=String(ev.description||'').toLowerCase();
        if(alert.kind==='review'||alert.kind==='replay'||alert.kind==='challenge'){
            if(evType.includes('review')||evType.includes('challenge')||evType.includes('replay'))return true;
        }
        if(alert.kind==='batter-timeout'||alert.kind==='pitch-timer'||alert.kind==='pitch-timer-violation'){
            if(evType.includes('timeout')||evType.includes('timer')||evType.includes('clock')||evDesc.includes('timeout')||evDesc.includes('timer'))return true;
        }
        if(alert.kind==='mound'){
            if(evType.includes('mound')||evDesc.includes('mound'))return true;
        }
    }
    return false;
}
function showNextGameAlert(){
    if(gameAlertTimer!==null||gameAlertExitTimer!==null||document.hidden||!currentGamePk||!gameAlertQueue.length)return;
    const region=document.getElementById('gameAlertToastRegion');
    if(!region)return;
    const alert=gameAlertQueue.shift();
    activeTransientAlert=alert;
    activeTransientAlertKey=alert.key;
    region.innerHTML=`<div class="game-alert-toast game-alert-toast--${alert.kind}" role="status">
        <div class="game-alert-toast-icon">${gameAlertIcon(alert.kind)}</div>
        <div class="game-alert-toast-copy">
            <div class="game-alert-toast-heading"><strong>${escapeAlertText(alert.title)}</strong>${alert.inning?`<span>${escapeAlertText(alert.inning)}</span>`:''}</div>
            ${alert.description?`<p>${escapeAlertText(alert.description)}</p>`:''}
        </div>
    </div>`;
    const toast=region.firstElementChild;
    requestAnimationFrame(()=>toast?.classList.add('is-visible'));
    const dismissAfter=(ms)=>{
        gameAlertTimer=setTimeout(()=>{
            gameAlertTimer=null;
            if(activeTransientAlertKey===alert.key&&isAlertStillActive(alert)){
                dismissAfter(1500);
                return;
            }
            activeTransientAlert=null;
            activeTransientAlertKey=null;
            toast?.classList.remove('is-visible');
            toast?.classList.add('is-leaving');
            gameAlertExitTimer=setTimeout(()=>{
                gameAlertExitTimer=null;
                if(region.firstElementChild===toast)region.replaceChildren();
                showNextGameAlert();
            },260);
        },ms);
    };
    dismissAfter(2500);
}

// ── GAME TAB ──────────────────────────────────────────────────
function renderGameTab(data){
    const container=document.getElementById('gcContent');
    document.getElementById('panel-away')?.remove();document.getElementById('panel-home')?.remove();
    let panel=document.getElementById('panel-game');
    if(!panel){panel=document.createElement('div');panel.id='panel-game';panel.className='gc-panel active';container.appendChild(panel);}

    const ls=data.linescore,isLive=data.status?.abstractGameState==='Live',isFinal=data.status?.abstractGameState==='Final';
    const aw=currentGame.away,hm=currentGame.home,offense=ls.offense||{},defense=ls.defense||{};
    const currentPlay=data.currentPlay;
    const betweenInnings=ls.inningState==='Middle'||ls.inningState==='End';
    const playBatter=currentPlay?.matchup?.batter;
    const lineBatter=data.currentBatter?.id?data.currentBatter:offense.batter;
    const sameBatter=!lineBatter?.id||!playBatter?.id||lineBatter.id===playBatter.id;
    const liveHalf=String(ls.inningHalf||(ls.isTopInning?'Top':'Bottom')).toLowerCase();
    const playHalf=String(currentPlay?.about?.halfInning||'').toLowerCase();
    const sameHalf=!playHalf||!liveHalf||playHalf===liveHalf;
    const sameInning=!currentPlay?.about?.inning||!ls.inning||currentPlay.about.inning===ls.inning;
    const candidatePitches=uniquePitchEvents(currentPlay?.pitches);
    const playAtBatIndex=currentPlay?.atBatIndex==null?NaN:Number(currentPlay.atBatIndex);
    const advancesAtBat=Number.isInteger(playAtBatIndex)&&(
        lastActiveAtBatIndex===null||playAtBatIndex>lastActiveAtBatIndex
    );
    if(betweenInnings)activePlayAheadOfLinescore=false;
    const keepsAcceptedLead=Number.isInteger(playAtBatIndex)&&
        playAtBatIndex===lastActiveAtBatIndex&&activePlayAheadOfLinescore;
    const playIsCurrent=Boolean(
        currentPlay&&currentPlay.about?.isComplete!==true&&!betweenInnings&&sameHalf&&sameInning&&
        (data.currentPlayActive===true||sameBatter||
            (candidatePitches.length>0&&(advancesAtBat||keepsAcceptedLead)))
    );
    if(playIsCurrent&&Number.isInteger(playAtBatIndex)){
        lastActiveAtBatIndex=lastActiveAtBatIndex===null
            ?playAtBatIndex
            :Math.max(lastActiveAtBatIndex,playAtBatIndex);
        activePlayAheadOfLinescore=!sameBatter;
    }else if(!sameBatter&&Number.isInteger(playAtBatIndex)&&playAtBatIndex===lastActiveAtBatIndex){
        activePlayAheadOfLinescore=false;
    }
    const activePlay=playIsCurrent?currentPlay:null;
    const batterObj=activePlay?.matchup?.batter?.id
        ?activePlay.matchup.batter
        :(lineBatter?.id?lineBatter:{});
    const matchPitcher=activePlay?.matchup?.pitcher;
    const pitcherObj=matchPitcher?.id
        ?matchPitcher
        :data.currentPitcher?.id
            ?data.currentPitcher
            :(defense.pitcher?.id?defense.pitcher:{});
    const pitches=activePlay?candidatePitches:[];
    const lastPitch=pitches.length>0?pitches[pitches.length-1]:null;
    const activeHalf=String(activePlay?.about?.halfInning||'').toLowerCase();
    const offSide=activeHalf==='top'?'away':activeHalf==='bottom'?'home':offense.team?.id===aw.id?'away':'home';
    const offBox=data.boxscore?.[offSide];
    const defSide=offSide==='away'?'home':'away',defBox=data.boxscore?.[defSide];
    const findB=id=>offBox?.batters?.find(b=>b.id===id),findP=id=>defBox?.pitchers?.find(p=>p.id===id);
    const bStats=findB(batterObj?.id);
    const pStats=findP(pitcherObj.id);
    const pitcherPlayByKey=new Map();
    (data.plays||[]).forEach((play,index)=>{
        const key=play.atBatIndex!=null?`ab:${play.atBatIndex}`:`row:${index}`;
        pitcherPlayByKey.set(key,play);
    });
    if(activePlay){
        const key=activePlay.atBatIndex!=null?`ab:${activePlay.atBatIndex}`:'active';
        pitcherPlayByKey.set(key,activePlay);
    }
    const derivedPitchTotal=[...pitcherPlayByKey.values()].reduce((total,play)=>{
        if(!pitcherObj.id||play.matchup?.pitcher?.id!==pitcherObj.id)return total;
        return total+uniquePitchEvents(play.pitches).length;
    },0);
    const boxPitchTotal=Number(pStats?.pitches);
    const pitcherPitchTotal=Number.isFinite(boxPitchTotal)
        ?Math.max(boxPitchTotal,derivedPitchTotal)
        :null;
    const odStats=findB(offense.onDeck?.id),ihStats=findB(offense.inHole?.id);
    const bSide=offense.batSide?.code||'',pSide=offense.pitchHand?.code||'';
    const avg=s=>{if(!s)return'.000';if(s.seasonAvg)return s.seasonAvg;if(!s.seasonAB||s.seasonAB===0)return'.000';return'.'+String(Math.round(s.seasonH/s.seasonAB*1000)).padStart(3,'0');};
    const seasonLine=s=>{if(!s)return'0/0';return s.seasonH!=null&&s.seasonAB!=null?s.seasonH+'/'+s.seasonAB:s.ab!=null?s.h+'/'+s.ab:'0/0';};
    const pitchContextKey=`${ls.inning||''}:${liveHalf||''}:${activePlay?.atBatIndex??'pending'}:${batterObj?.id||''}:${pitcherObj.id||''}`;
    if(pitchContextKey!==lastPitchContextKey){
        lastPitchContextKey=pitchContextKey;
        lastAnimatedPitchEventId=null;
    }
    let html='';

    // Situation bar
    if(isLive&&offense.batter){
        html+=`<div class="sit-bar"><div class="sit-bar-left">`;
        if(offense.onDeck)html+=`<div class="sit-row"><span class="sit-label">On deck · ${(offense.battingOrder||0)%9+1}</span><div class="sit-player"><img src="${playerHeadshotUrl(offense.onDeck.id)}" alt="" class="sit-avatar" onerror="this.style.display='none'"><div class="sit-player-info"><span class="sit-player-name">${offense.onDeck.fullName}</span><span class="sit-player-stats">${seasonLine(odStats)} · ${avg(odStats)}</span></div></div></div>`;
        if(offense.inHole)html+=`<div class="sit-row"><span class="sit-label">In the hole · ${(offense.battingOrder||0)%9+2}</span><div class="sit-player"><img src="${playerHeadshotUrl(offense.inHole.id)}" alt="" class="sit-avatar" onerror="this.style.display='none'"><div class="sit-player-info"><span class="sit-player-name">${offense.inHole.fullName}</span><span class="sit-player-stats">${seasonLine(ihStats)} · ${avg(ihStats)}</span></div></div></div>`;
        html+=`</div><div class="diamond-wrap"><div class="diamond-large">`;
        html+=`<div class="base base-pos first ${offense.first?'occupied':''}">${offense.first?`<img src="${playerHeadshotUrl(offense.first.id)}" alt="" class="base-avatar">`:''}</div>`;
        html+=`<div class="base base-pos second ${offense.second?'occupied':''}">${offense.second?`<img src="${playerHeadshotUrl(offense.second.id)}" alt="" class="base-avatar">`:''}</div>`;
        html+=`<div class="base base-pos third ${offense.third?'occupied':''}">${offense.third?`<img src="${playerHeadshotUrl(offense.third.id)}" alt="" class="base-avatar">`:''}</div>`;
        html+=`</div></div></div>`;
    }

    // Last pitch card
    if(isLive&&!betweenInnings&&batterObj?.id){
        let displayPitches=pitches;
        let displayCountBalls=lastPitch?.count?.balls;
        let displayCountStrikes=lastPitch?.count?.strikes;
        let displayResult=null;
        let displayBatter=batterObj;
        let displayPitcher=pitcherObj;
        if(!playIsCurrent&&displayPitches.length===0){
            const lastCompletedPlay=[...(data.plays||[])].reverse().find(p=>p.about?.isComplete&&p.pitches?.length>0);
            if(lastCompletedPlay){
                displayPitches=uniquePitchEvents(lastCompletedPlay.pitches);
                const lastP=displayPitches[displayPitches.length-1];
                displayCountBalls=lastP?.count?.balls;
                displayCountStrikes=lastP?.count?.strikes;
                const now=performance.now();
                const timeSinceComplete=now-lastCompletedPlayTime;
                if(lastCompletedPlay.atBatIndex===lastCompletedPlayAtBat&&timeSinceComplete<PLAY_RESULT_HOLD_MS){
                    displayResult=lastCompletedPlay.shortResult||lastCompletedPlay.result||'';
                    if(lastCompletedPlay.matchup?.batter?.id)displayBatter=lastCompletedPlay.matchup.batter;
                    if(lastCompletedPlay.matchup?.pitcher?.id)displayPitcher=lastCompletedPlay.matchup.pitcher;
                }else if(lastCompletedPlay.atBatIndex!==lastCompletedPlayAtBat){
                    lastCompletedPlayAtBat=lastCompletedPlay.atBatIndex;
                    lastCompletedPlayTime=now;
                    displayResult=lastCompletedPlay.shortResult||lastCompletedPlay.result||'';
                    if(lastCompletedPlay.matchup?.batter?.id)displayBatter=lastCompletedPlay.matchup.batter;
                    if(lastCompletedPlay.matchup?.pitcher?.id)displayPitcher=lastCompletedPlay.matchup.pitcher;
                }
            }
        }
        if(playIsCurrent){
            lastCompletedPlayAtBat=null;
            lastCompletedPlayTime=0;
        }
        const cb=displayCountBalls??(playIsCurrent?ls.balls:0);
        const cs=displayCountStrikes??(playIsCurrent?ls.strikes:0);
        const ct=displayResult||(`${cb} ball${cb!==1?'s':''}, ${cs} strike${cs!==1?'s':''}`);
        const visiblePitches=displayPitches.filter(p=>(p.px!=null&&p.pz!=null)||(p.x!=null&&p.y!=null));
        const pitchDots=visiblePitches.map((p,pi)=>{
            const pcls=getPitchClass(p);
            const px=p.px!=null?mapPitchMiniX(p.px):mapPitchMiniX(((p.x||125)-80)/90*1.7-0.85);
            const py=p.pz!=null?mapPitchMiniY(p.pz,p.szTop,p.szBottom):mapPitchMiniY(3.0-((p.y||150)-85)/130*3.0+1.0);
            const isLatest=pi===visiblePitches.length-1;
            const eventId=p.eventId||`${pitchContextKey}:${p.pitchNumber||pi}:${pi}`;
            const flightX=(75-px).toFixed(2),flightY=(-80).toFixed(2);
            return`<div class="mini-sz-dot ${pcls}${isLatest?' pitch-dot-new':''}" data-x="${px}" data-y="${py}" data-eid="${escapeAlertText(eventId)}" style="left:${px}px;top:${py}px;--pitch-flight-x:${flightX}px;--pitch-flight-y:${flightY}px;">${p.pitchNumber||''}</div>`;
        }).join('');
        const pitchList=displayPitches.slice().reverse().map((p,pi)=>{
            const pcls=getPitchClass(p);
            const pbg=getPitchBg(pcls);
            const pvelo=p.startSpeed?Math.round(p.startSpeed)+' mph':'';
            const ptype=p.type||'';
            return`<div class="fpc-pitch-row"><span class="fpc-badge fpc-badge-sm" style="${pbg}">${p.pitchNumber}</span><div class="fpc-pitch-info"><span class="fpc-pitch-call">${p.call||p.description||''}</span><span class="fpc-pitch-detail">${ptype}${pvelo?' · '+pvelo:''}</span></div></div>`;
        }).join('');
        const batterName=displayBatter.fullName||'';
        const pitcherName=displayPitcher.fullName||'';
        const bStatsDisplay=findB(displayBatter?.id);
        const pStatsDisplay=findP(displayPitcher?.id);
        const batterMeta=`${seasonLine(bStatsDisplay)}, ${avg(bStatsDisplay)}${bSide?` (${bSide})`:''}`;
        const pitcherMeta=`${pStatsDisplay?pStatsDisplay.ip:'—'} IP, ${pStatsDisplay?pStatsDisplay.k:'—'} K, ${pitcherPitchTotal??'—'} P${pSide?` (${pSide})`:''}`;
        html+=`<div class="feed-pitch-card${displayResult?' play-result-hold':''}">
            <div class="fpc-top">
                <div class="fpc-avatar-stack">
                    <img src="${playerHeadshotUrl(displayBatter.id,160)}" alt="" class="fpc-avatar fpc-avatar-batter" onerror="this.style.display='none'">
                    ${displayPitcher.id?`<img src="${playerHeadshotUrl(displayPitcher.id,128)}" alt="" class="fpc-avatar fpc-avatar-pitcher" onerror="this.style.display='none'">`:''}
                </div>
                <div class="fpc-info">
                    <div class="fpc-count">${ct}</div>
                    <div class="fpc-batter"><span class="fpc-player-name">${batterName}</span><span class="fpc-player-meta">&middot; ${batterMeta}</span></div>
                    <div class="fpc-pitcher"><span class="fpc-player-name">${pitcherName}</span><span class="fpc-player-meta">&middot; ${pitcherMeta}</span></div>
                </div>
            </div>
            <div class="fpc-body">
            <div class="fpc-zone-wrap"><div class="fpc-pitch-list">${pitchList}</div><div class="mini-sz-zone"><div class="mini-sz-grid">${Array(9).fill('').map(()=>'<div class="mini-sz-cell"></div>').join('')}</div>${pitchDots}</div></div></div></div>`;
    }

    // Play-by-play
    const playByKey=new Map();
    (data.plays||[]).forEach((play,index)=>{
        const key=play.atBatIndex!=null?`ab:${play.atBatIndex}`:`row:${index}`;
        playByKey.set(key,play);
    });
    const plays=[...playByKey.values()];
    const feedPlays=plays.filter(play=>{
        const result=play.shortResult||play.result||'';
        return play.about?.isComplete===true
            &&(!play.resultType||play.resultType==='atBat')
            &&Boolean(result)
            &&!isNonPlayAlert(play.eventType,result);
    });
    const recent=feedPlays.slice(-20).reverse();
    const currentPlayCount=feedPlays.length;
    if(currentPlayCount>0){
        const newPlaysCount=lastPlayIndex>=0?Math.max(0,currentPlayCount-lastPlayIndex):1;
        html+=`<div class="play-feed-section">`;
        recent.forEach((p,idx)=>{
            const res=p.shortResult||p.result||'';if(!res)return;
            const bat=p.matchup?.batter?.fullName||'',pit=p.matchup?.pitcher?.fullName||'',bid=p.matchup?.batter?.id;
            const half=p.about?.halfInning||'',inn=p.about?.inning||'';
            const innTxt=half&&inn?(half==='top'?'▲ Top':'▼ Bot')+' '+inn:'';
            const pc=p.pitches?.length||0;
            let ic='event',ix='📋';const et=(p.eventType||'').toLowerCase();
            if(et.includes('strikeout')){ic='strike';ix='X';}else if(et==='walk'){ic='walk';ix='BB';}else if(et==='hit_by_pitch'){ic='walk';ix='HBP';}
            else if(et.includes('home_run')){ic='hit';ix='HR';}else if(et.includes('single')||et.includes('double')||et.includes('triple')){ic='hit';ix='⚾';}
            else if(et.includes('out')||et.includes('groundout')||et.includes('flyout')||et.includes('popout')||et.includes('lineout')||et.includes('sac')){ic='out';ix='✗';}
            else if(p.about?.isScoringPlay){ic='hit';ix='🏟';}
            const outs=p.about?.outs??ls.outs;
            const innLabel=half&&inn?(half==='top'?'Top':'Bot')+' '+inn:'';
            const topLine=`${ls.score?.away??''} - ${ls.score?.home??''} · ${innLabel} · ${outs} out${outs!==1?'s':''}`;
            const batStats=p.matchup?.battingStats||{};
            const pitStats=p.matchup?.pitchingStats||{};
            const batFirst=bat?bat.split(' '):[];
            const batLast=batFirst.pop()||'';
            const batLine=batLast?`${batLast}${batStats.ab!=null?' · '+batStats.ab+' AB':''}${batStats.h!=null?', '+batStats.h+' H':''}${batStats.rbi!=null?', '+batStats.rbi+' RBI':''}`:'';
            const pitFirst=pit?pit.split(' '):[];
            const pitLast=pitFirst.pop()||'';
            const pitLine=pitLast?`${pitLast}${pitStats.ip!=null?' · '+pitStats.ip+' ip':''}${pitStats.k!=null?', '+pitStats.k+' k':''}${pc?', '+pc+' p':''}`:'';
            const isNew=idx<newPlaysCount;
            html+=`<div class="feed-item${isNew?' feed-item-new':''}" style="${isNew?'animation-delay:'+(idx*80)+'ms':''}"><div class="feed-avatar-wrap"><img src="${playerHeadshotUrl(bid)}" alt="" class="feed-avatar" onerror="this.style.display='none'"><div class="feed-indicator ${ic}">${ix}</div></div><div class="feed-body"><div class="feed-top-line">${topLine}</div><div class="feed-title">${res}</div><div class="feed-meta">${batLine}</div><div class="feed-meta">${pitLine}</div></div></div>`;
        });
        lastPlayIndex=currentPlayCount;
        html+=`</div>`;
    }

    // Line score
    const innings=data.linescoreInnings||[];
    if(innings.length>0){
        html+=`<div class="linescore-section"><div style="overflow-x:auto"><table class="lineup-table"><thead><tr><th></th>${innings.map(i=>`<th>${i.num}</th>`).join('')}<th>R</th><th>H</th><th>E</th></tr></thead><tbody>`;
        html+=`<tr><td style="text-align:left;padding-left:12px;font-weight:700">${aw.abbr}</td>${innings.map(i=>`<td>${i.away?.runs??''}</td>`).join('')}<td style="font-weight:800">${innings.reduce((s,i)=>s+(i.away?.runs||0),0)}</td><td>${innings.reduce((s,i)=>s+(i.away?.hits||0),0)}</td><td>${innings.reduce((s,i)=>s+(i.away?.errors||0),0)}</td></tr>`;
        html+=`<tr><td style="text-align:left;padding-left:12px;font-weight:700">${hm.abbr}</td>${innings.map(i=>`<td>${i.home?.runs??''}</td>`).join('')}<td style="font-weight:800">${innings.reduce((s,i)=>s+(i.home?.runs||0),0)}</td><td>${innings.reduce((s,i)=>s+(i.home?.hits||0),0)}</td><td>${innings.reduce((s,i)=>s+(i.home?.errors||0),0)}</td></tr>`;
        html+=`</tbody></table></div></div>`;
    }

    panel.innerHTML=html||'<div style="color:var(--text-muted);text-align:center;padding:40px">No play data available</div>';
    animateLatestPitch();
}

function mapPitchX(x){return Math.max(0,Math.min(180,((x-19)/177)*180));}
function mapPitchY(y){return Math.max(0,Math.min(200,((260-y)/171)*200));}
function mapPitchMiniX(x){return Math.max(0,Math.min(150,((0.71-x)/1.42)*100+25));}
function mapPitchMiniY(z,szTop,szBottom){var top=szTop||3.5,bot=szBottom||1.5,rng=top-bot||2.0;return Math.max(0,Math.min(180,((top-z)/rng)*125+15));}

function getPitchClass(p){
    const c=p.callCode||p.code||'',e=(p.eventType||'').toLowerCase();
    if(e==='hit_by_pitch'||c==='H')return'hbp';if(e==='pitching_tip'||c==='T')return'foul';
    if(p.isInPlay||c==='X'||c==='D'||c==='E')return'in-play';
    if(c==='F'||c==='L'||c==='M')return'foul';
    if(e==='swinging_strike'||c==='S')return'strike-swinging';
    if(e==='called_strike'||c==='C')return'strike-called';
    if(e==='ball'||c==='B'||c==='*B')return'ball';
    if(p.isBall)return'ball';if(p.isStrike)return'strike-called';return'ball';
}
function getPitchBg(c){
    switch(c){case'ball':return'background:var(--ball-color)';case'strike-called':case'strike-swinging':return'background:var(--strike-color)';case'foul':return'background:var(--foul-color)';case'in-play':return'background:var(--hit-color)';case'hbp':return'background:var(--hbp-color)';default:return'background:var(--text-muted)';}
}

function animateLatestPitch(){
    const dot=document.querySelector('.pitch-dot-new');
    if(!dot)return;
    const eid=dot.dataset.eid;
    if(eid===lastAnimatedPitchEventId){dot.classList.remove('pitch-dot-new');return;}
    lastAnimatedPitchEventId=eid;
    if(window.matchMedia?.('(prefers-reduced-motion: reduce)').matches){
        dot.classList.remove('pitch-dot-new');
        return;
    }
    pitchAnimationUntil=performance.now()+800;
    let fallbackTimer=null;
    const finish=()=>{
        dot.removeEventListener('animationend',onAnimationEnd);
        if(fallbackTimer!==null)clearTimeout(fallbackTimer);
        dot.classList.remove('pitch-dot-new');
        pitchAnimationUntil=0;
        if(deferredGameRenderData)flushDeferredGameRender();
    };
    const onAnimationEnd=event=>{
        if(event.animationName==='pitchImpact')finish();
    };
    dot.addEventListener('animationend',onAnimationEnd);
    fallbackTimer=setTimeout(finish,900);
}

// ── TEAM TABS ─────────────────────────────────────────────────
function renderTeamTab(data,side){
    const container=document.getElementById('gcContent'),team=currentGame[side],box=data.boxscore?.[side];
    if(!box)return;
    let panel=document.getElementById('panel-'+side);
    if(!panel){panel=document.createElement('div');panel.id='panel-'+side;panel.className='gc-panel';container.appendChild(panel);}
    const batters=box.batters||[],pitchers=box.pitchers||[];
    let html=`<div class="game-tab-section"><h3>${team.abbr} Batting</h3><div style="overflow-x:auto"><table class="lineup-table"><thead><tr><th>#</th><th style="text-align:left">Player</th><th>AB</th><th>H</th><th>R</th><th>RBI</th><th>SO</th><th>BB</th><th>HR</th><th>TB</th><th>SB</th></tr></thead><tbody>`;
    html+=batters.map(b=>`<tr><td><span class="lineup-num">${b.battingOrder?Math.floor(b.battingOrder/100):''}</span></td><td><div class="player-cell"><img src="${playerHeadshotUrl(b.id)}" alt="" class="lineup-headshot" onerror="this.style.display='none'"><span class="lineup-player-name">${b.name}</span><span class="lineup-pos">${b.position}</span></div></td><td>${b.ab}</td><td>${b.h}</td><td>${b.r}</td><td>${b.rbi}</td><td class="${b.so>0?'lineup-so':''}">${b.so}</td><td>${b.bb}</td><td class="${b.hr>0?'lineup-hr':''}">${b.hr}</td><td>${b.tb}</td><td>${b.sb}</td></tr>`).join('');
    html+=`</tbody></table></div></div><div class="pitchers-section"><h3>Pitching</h3>`;
    html+=pitchers.map((p,i)=>`<div class="pitcher-card ${i===pitchers.length-1?'active':''}"><div class="pitcher-card-header"><div style="display:flex;align-items:center;gap:8px"><img src="${playerHeadshotUrl(p.id)}" alt="" class="player-headshot" onerror="this.style.display='none'"><span class="pitcher-name">${p.name}</span></div><span class="pitcher-status ${i===pitchers.length-1?'active':'previous'}">${i===pitchers.length-1?'Active':'Previous'}</span></div><div class="pitcher-stats"><div class="pitcher-stat"><span class="pitcher-stat-val">${p.ip}</span><span class="pitcher-stat-label">IP</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.h}</span><span class="pitcher-stat-label">H</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.r}</span><span class="pitcher-stat-label">R</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.er}</span><span class="pitcher-stat-label">ER</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.k}</span><span class="pitcher-stat-label">K</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.bb}</span><span class="pitcher-stat-label">BB</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.hr}</span><span class="pitcher-stat-label">HR</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.era}</span><span class="pitcher-stat-label">ERA</span></div><div class="pitcher-stat"><span class="pitcher-stat-val">${p.strk}</span><span class="pitcher-stat-label">STRK%</span></div></div></div>`).join('');
    html+=`</div>`;
    panel.innerHTML=html;
}
