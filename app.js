const API = '';
let currentDate = new Date();
let currentGame = null;
let currentGamePk = null;
let activeTab = 'game';
let refreshTimer = null;
let teamsCache = {};
let lastPlayIndex = -1;
let lastAnimatedPitchEventId = null;

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
function playerHeadshotUrl(p){return p?`https://content.mlb.com/images/headshots/current/60x60/${p}.png`:'';}
function teamLogoImg(a,id,s){
    s=s||24;const u=teamLogoUrl(a,id);
    return u?`<img src="${u}" alt="${a}" width="${s}" height="${s}" style="object-fit:contain;background:transparent" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'"><span class="team-logo" style="background:${teamColor(a)};display:none">${a?.[0]||'?'}</span>`:`<span class="team-logo" style="background:${teamColor(a)}">${a?.[0]||'?'}</span>`;
}

document.addEventListener('DOMContentLoaded',async()=>{
    await loadTeams();renderDatePicker();loadGames();
    document.getElementById('standingsBtn').addEventListener('click',showStandings);
});
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

async function loadGames(){
    showLoading();
    try{const r=await fetch(`${API}/api/schedule?date=${fmtDate(currentDate)}`);const d=await r.json();hideLoading();renderGames(d.games);}
    catch(e){hideLoading();document.getElementById('emptyState').style.display='flex';document.getElementById('emptyState').querySelector('p').textContent='Could not load games';document.getElementById('emptyState').querySelector('span').textContent=e.message;}
}

function renderGames(games){
    const list=document.getElementById('gamesList');list.innerHTML='';document.getElementById('emptyState').style.display='none';
    if(!games||games.length===0){document.getElementById('emptyState').style.display='flex';return;}
    games.forEach(g=>{
        const aw=g.away,hm=g.home,ls=g.linescore,st=g.status;
        const isLive=st.abstract==='Live',isFinal=st.abstract==='Final',isDelayed=st.detailed==='Delayed',isPreview=st.abstract==='Preview';
        let inningText='';
        if(isLive){const innN=ls.inning||'';const halfL=ls.inningState==='Middle'?'Mid':ls.isTopInning?'Top':'Bot';inningText=`${halfL} ${innN}`;}
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

function switchScreen(id){document.querySelectorAll('.screen').forEach(s=>s.classList.remove('active'));document.getElementById(id).classList.add('active');if(id!=='app-gamecenter'){clearInterval(refreshTimer);refreshTimer=null;}}

async function openGameCenter(pk,away,home){
    currentGamePk=pk;currentGame={away,home};lastPlayIndex=-1;lastAnimatedPitchEventId=null;switchScreen('app-gamecenter');
    document.getElementById('gcContent').innerHTML='<div class="loading-state" id="gcLoader"><div class="spinner"></div><p>Loading game...</p></div>';
    renderGCTabs(away,home);await loadGameFeed();
    clearInterval(refreshTimer);refreshTimer=setInterval(loadGameFeed,1500);
}
function renderGCTabs(away,home){document.getElementById('gcTabs').innerHTML=`<button class="gc-tab active" data-tab="game" onclick="showGCPanel('game')">Feed</button><button class="gc-tab" data-tab="away" onclick="showGCPanel('away')">${away.abbr}</button><button class="gc-tab" data-tab="home" onclick="showGCPanel('home')">${home.abbr}</button>`;}
function showGCPanel(tab){
    activeTab=tab;
    document.querySelectorAll('.gc-tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===tab));
    document.querySelectorAll('.gc-panel').forEach(p=>p.classList.toggle('active',p.id==='panel-'+tab));
    if(lastFeedData){
        if(tab==='game')renderGameTab(lastFeedData);
        else if(tab==='away')renderTeamTab(lastFeedData,'away');
        else if(tab==='home')renderTeamTab(lastFeedData,'home');
    }
}
function closeGameCenter(){clearInterval(refreshTimer);refreshTimer=null;currentGamePk=null;switchScreen('app-scores');}

let lastFeedData=null;
async function loadGameFeed(){
    if(!currentGamePk)return;
    try{const r=await fetch(`/api/game/${currentGamePk}/feed`);
    if(!r.ok)throw new Error(r.status);
    const d=await r.json();
    const loader=document.getElementById('gcLoader');
    if(loader)loader.remove();
    if(d.error){console.error('Feed error:',d.error);return;}
    lastFeedData=d;
    renderGCHeader(d);
    if(activeTab==='game')renderGameTab(d);
    else if(activeTab==='away')renderTeamTab(d,'away');
    else if(activeTab==='home')renderTeamTab(d,'home');
    showGCPanel(activeTab);}
    catch(e){console.error('Feed error:',e);}
}

function renderGCHeader(data){
    const aw=currentGame.away,hm=currentGame.home,ls=data.linescore,st=data.status;
    const isLive=st?.abstractGameState==='Live',isFinal=st?.abstractGameState==='Final';
    const awS=data.boxscore?.away?.batters?calcScore(data.boxscore.away):'?';
    const hmS=data.boxscore?.home?.batters?calcScore(data.boxscore.home):'?';
    let statusHtml='';
    if(isLive){
        const innNum=ls.inning||'';
        const halfLabel=ls.inningState==='Middle'?'Mid':ls.isTopInning?'Top':'Bot';
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

// ── GAME TAB ──────────────────────────────────────────────────
function renderGameTab(data){
    const container=document.getElementById('gcContent');
    document.getElementById('panel-away')?.remove();document.getElementById('panel-home')?.remove();
    let panel=document.getElementById('panel-game');
    if(!panel){panel=document.createElement('div');panel.id='panel-game';panel.className='gc-panel active';container.appendChild(panel);}

    const ls=data.linescore,isLive=data.status?.abstractGameState==='Live',isFinal=data.status?.abstractGameState==='Final';
    const aw=currentGame.away,hm=currentGame.home,offense=ls.offense||{};
    const currentPlay=data.currentPlay,allEvents=currentPlay?.pitches||[];
    const seen=new Set();
    const pitches=allEvents.filter(p=>{
        if(!p.isPitch)return false;
        const key=(p.eventId||'')+(p.pitchNumber||'')+(p.description||'')+(p.startSpeed||'');
        if(seen.has(key))return false;
        seen.add(key);
        return true;
    });
    const lastPitch=pitches.length>0?pitches[pitches.length-1]:null;
    const matchPitcher=currentPlay?.matchup?.pitcher;
    const offSide=offense.team?.id===aw.id?'away':'home',offBox=data.boxscore?.[offSide];
    const defSide=offSide==='away'?'home':'away',defBox=data.boxscore?.[defSide];
    const findB=id=>offBox?.batters?.find(b=>b.id===id),findP=id=>defBox?.pitchers?.find(p=>p.id===id);
    const bStats=findB(offense.batter?.id);
    const pitcherObj=matchPitcher?{id:matchPitcher.id,fullName:matchPitcher.fullName}:offense.pitcher||{};
    const pStats=findP(pitcherObj.id);
    const odStats=findB(offense.onDeck?.id),ihStats=findB(offense.inHole?.id);
    const bSide=offense.batSide?.code||'',pSide=offense.pitchHand?.code||'';
    const avg=s=>{if(!s||s.ab===0)return'.000';return'.'+String(Math.round(s.h/s.ab*1000)).padStart(3,'0');};
    let html='';

    // Situation bar
    if(isLive&&offense.batter){
        html+=`<div class="sit-bar"><div class="sit-bar-left">`;
        if(offense.onDeck)html+=`<div class="sit-row"><span class="sit-label">On deck · ${(offense.battingOrder||0)%9+1}</span><div class="sit-player"><img src="${playerHeadshotUrl(offense.onDeck.id)}" alt="" class="sit-avatar" onerror="this.style.display='none'"><div class="sit-player-info"><span class="sit-player-name">${offense.onDeck.fullName}</span><span class="sit-player-stats">${odStats?odStats.h+'/'+odStats.ab:'0/0'} · ${avg(odStats)}</span></div></div></div>`;
        if(offense.inHole)html+=`<div class="sit-row"><span class="sit-label">In the hole · ${(offense.battingOrder||0)%9+2}</span><div class="sit-player"><img src="${playerHeadshotUrl(offense.inHole.id)}" alt="" class="sit-avatar" onerror="this.style.display='none'"><div class="sit-player-info"><span class="sit-player-name">${offense.inHole.fullName}</span><span class="sit-player-stats">${ihStats?ihStats.h+'/'+ihStats.ab:'0/0'} · ${avg(ihStats)}</span></div></div></div>`;
        html+=`</div><div class="diamond-wrap"><div class="diamond-large">`;
        html+=`<div class="base base-pos first ${offense.first?'occupied':''}">${offense.first?`<img src="${playerHeadshotUrl(offense.first.id)}" alt="" class="base-avatar">`:''}</div>`;
        html+=`<div class="base base-pos second ${offense.second?'occupied':''}">${offense.second?`<img src="${playerHeadshotUrl(offense.second.id)}" alt="" class="base-avatar">`:''}</div>`;
        html+=`<div class="base base-pos third ${offense.third?'occupied':''}">${offense.third?`<img src="${playerHeadshotUrl(offense.third.id)}" alt="" class="base-avatar">`:''}</div>`;
        html+=`</div></div></div>`;
    }

    // Last pitch card
    if(isLive&&pitches.length>0){
        const lp=lastPitch,cls=getPitchClass(lp),bg=getPitchBg(cls);
        const velo=lp.startSpeed?Math.round(lp.startSpeed):'';
        const call=lp.call||lp.description||'';
        const cb=lp.count?.balls??ls.balls,cs=lp.count?.strikes??ls.strikes;
        const ct=cb===0&&cs===0?'No balls, no strikes':cb===1&&cs===0?'1 ball, no strikes':cb===0&&cs===1?'No balls, 1 strike':`${cb} ball${cb!==1?'s':''}, ${cs} strike${cs!==1?'s':''}`;
        const visiblePitches=pitches.filter(p=>(p.px!=null&&p.pz!=null)||(p.x!=null&&p.y!=null));
        const pitchDots=visiblePitches.map((p,pi)=>{
            const pcls=getPitchClass(p);
            const px=p.px!=null?mapPitchMiniX(p.px):mapPitchMiniX(((p.x||125)-80)/90*1.7-0.85);
            const py=p.pz!=null?mapPitchMiniY(p.pz):mapPitchMiniY(3.0-((p.y||150)-85)/130*3.0+1.0);
            const isLatest=pi===visiblePitches.length-1;
            return`<div class="mini-sz-dot ${pcls}" data-x="${px}" data-y="${py}" data-eid="${p.eventId||pi}" data-latest="${isLatest}" style="left:${isLatest?110:px}px;top:${isLatest?130:py}px;${isLatest?'opacity:0':''}">${p.pitchNumber||''}</div>`;
        }).join('');
        const pitchList=pitches.slice().reverse().map((p,pi)=>{
            const pcls=getPitchClass(p);
            const pbg=getPitchBg(pcls);
            const pvelo=p.startSpeed?Math.round(p.startSpeed)+' mph':'';
            const ptype=p.type||'';
            return`<div class="fpc-pitch-row"><span class="fpc-badge fpc-badge-sm" style="${pbg}">${p.pitchNumber}</span><div class="fpc-pitch-info"><span class="fpc-pitch-call">${p.call||p.description||''}</span><span class="fpc-pitch-detail">${ptype}${pvelo?' · '+pvelo:''}</span></div></div>`;
        }).join('');
        const batterName=offense.batter?.fullName||'';
        const pitcherName=pitcherObj.fullName||offense.pitcher?.fullName||'';
        const batterFirst=batterName.split(' ').pop()||'';
        const pitcherFirst=pitcherName.split(' ').pop()||'';
        html+=`<div class="feed-pitch-card">
            <div class="fpc-top">
                <div class="fpc-avatar-stack">
                    <img src="${playerHeadshotUrl(offense.batter?.id)}" alt="" class="fpc-avatar" onerror="this.style.display='none'">
                    <img src="${playerHeadshotUrl(pitcherObj.id)}" alt="" class="fpc-avatar fpc-avatar-pitcher" onerror="this.style.display='none'">
                </div>
                <div class="fpc-info"><div class="fpc-count">${ct}</div><div class="fpc-batter">${batterFirst} · ${bStats?bStats.h+'/'+bStats.ab:'0/0'}, ${avg(bStats)} (${bSide})</div><div class="fpc-pitcher">${pitcherFirst} · ${pStats?pStats.ip:'0'} ip, ${pStats?pStats.k:0} k, ${pitches.length} p (${pSide})</div></div></div>
            <div class="fpc-body"><div class="fpc-call-row"><span class="fpc-badge" style="${bg}">${pitches.length}</span><span class="fpc-call">${call}</span></div><span class="fpc-type">${lp.type||''} · ${velo?velo+' mph':''}</span>
            <div class="fpc-zone-wrap"><div class="mini-sz-zone"><div class="mini-sz-grid">${Array(9).fill('').map(()=>'<div class="mini-sz-cell"></div>').join('')}</div>${pitchDots}</div><div class="fpc-pitch-list">${pitchList}</div></div></div></div>`;
    }

    // Play-by-play
    const plays=data.plays||[],recent=plays.slice(-20).reverse();
    const currentPlayCount=recent.length;
    if(currentPlayCount>0){
        const newPlaysCount=lastPlayIndex>=0?Math.max(0,currentPlayCount-lastPlayIndex):1;
        html+=`<div class="play-feed-section">`;
        recent.forEach((p,idx)=>{
            const res=p.result||'';if(!res)return;
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

    // Game events
    const events=data.gameEvents||[];
    if(events.length>0){
        const im={'mound':'📋','pitcher-change':'🔄','sub':'🔀','steal':'🏃','event':'⚠️','replay':'📺','injury':'🏥'};
        html+=`<div class="events-section"><h3>Game Alerts</h3><div class="event-cards">${events.map(ev=>`<div class="event-card"><div class="event-icon ${ev.type}">${im[ev.type]||'📋'}</div><div class="event-text"><strong>${ev.title}</strong><br>${ev.description}</div><span class="event-time">${ev.inning}</span></div>`).join('')}</div></div>`;
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
function mapPitchMiniX(x){return Math.max(0,Math.min(220,((x+0.71)/1.42)*140+40));}
function mapPitchMiniY(z){return Math.max(0,Math.min(260,((3.5-z)/2.0)*180+40));}

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
    const dot=document.querySelector('.mini-sz-dot[data-latest="true"]');
    if(!dot)return;
    const eid=dot.dataset.eid;
    if(eid===lastAnimatedPitchEventId){dot.style.left=dot.dataset.x+'px';dot.style.top=dot.dataset.y+'px';dot.style.opacity='1';dot.style.transform='translate(-50%,-50%) scale(1)';dot.removeAttribute('data-latest');return;}
    lastAnimatedPitchEventId=eid;
    const targetX=parseFloat(dot.dataset.x),targetY=parseFloat(dot.dataset.y);
    const startX=110,startY=130;
    const duration=300;
    let start=null;
    function step(ts){
        if(!start)start=ts;
        const elapsed=ts-start;
        const t=Math.min(elapsed/duration,1);
        const ease=1-Math.pow(1-t,3);
        const cx=startX+(targetX-startX)*ease;
        const cy=startY+(targetY-startY)*ease;
        const scale=0.3+0.7*ease;
        const opacity=t<0.1?t/0.1:1;
        dot.style.left=cx+'px';
        dot.style.top=cy+'px';
        dot.style.opacity=opacity;
        dot.style.transform=`translate(-50%,-50%) scale(${scale})`;
        if(t<1)requestAnimationFrame(step);
        else{dot.style.transform='translate(-50%,-50%) scale(1)';dot.removeAttribute('data-latest');}
    }
    requestAnimationFrame(step);
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
