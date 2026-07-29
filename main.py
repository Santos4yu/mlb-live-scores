import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import json, time, os

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MLB = "https://statsapi.mlb.com/api/v1"
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 15

async def cached_get(url: str, ttl: int = CACHE_TTL, timeout: int = 30) -> dict:
    now = time.time()
    if url in _cache and now - _cache[url][0] < ttl:
        return _cache[url][1]
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url)
        data = r.json()
    _cache[url] = (now, data)
    return data


@app.get("/api/teams")
async def get_teams():
    data = await cached_get(f"{MLB}/teams?sportId=1", ttl=3600)
    out = {}
    for t in data.get("teams", []):
        abbr = t.get("abbreviation") or t.get("teamName", "")[:3].upper()
        out[abbr] = {
            "id": t["id"], "abbr": abbr,
            "name": t.get("name", ""), "teamName": t.get("teamName", ""),
            "division": t.get("division", {}).get("id"),
        }
    return out


@app.get("/api/schedule")
async def get_schedule(date: str = Query(...)):
    data = await cached_get(
        f"{MLB}/schedule?sportId=1&date={date}"
        f"&hydrate=linescore,team,probablePitcher",
        ttl=20, timeout=15,
    )
    games = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            away_t = g["teams"]["away"]["team"]
            home_t = g["teams"]["home"]["team"]
            ls = g.get("linescore", {})
            status = g["status"]
            games.append({
                "gamePk": g["gamePk"],
                "away": {
                    "id": away_t["id"],
                    "abbr": away_t.get("abbreviation", ""),
                    "name": away_t.get("teamName", ""),
                    "fullName": away_t.get("name", ""),
                    "score": g["teams"]["away"].get("score"),
                },
                "home": {
                    "id": home_t["id"],
                    "abbr": home_t.get("abbreviation", ""),
                    "name": home_t.get("teamName", ""),
                    "fullName": home_t.get("name", ""),
                    "score": g["teams"]["home"].get("score"),
                },
                "status": {
                    "abstract": status.get("abstractGameState"),
                    "detailed": status.get("detailedState"),
                    "code": status.get("statusCode"),
                },
                "linescore": {
                    "inning": ls.get("currentInning"),
                    "inningState": ls.get("inningState"),
                    "inningHalf": ls.get("inningHalf"),
                    "isTopInning": ls.get("isTopInning"),
                    "outs": ls.get("outs", 0),
                    "balls": ls.get("balls", 0),
                    "strikes": ls.get("strikes", 0),
                    "inningOrdinal": ls.get("currentInningOrdinal", ""),
                    "scheduledInnings": ls.get("scheduledInnings", 9),
                },
                "bases": {
                    "first": bool(ls.get("offense", {}).get("first")),
                    "second": bool(ls.get("offense", {}).get("second")),
                    "third": bool(ls.get("offense", {}).get("third")),
                },
                "venue": g.get("venue", {}).get("name", ""),
                "gameDate": g.get("gameDate", ""),
            })
    return {"date": date, "totalGames": len(games), "games": games}


@app.get("/api/game/{gamePk}/feed")
async def get_game_feed(gamePk: int):
    data = await cached_get(f"https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live", ttl=0, timeout=30)
    ld = data.get("liveData", {})
    plays_data = ld.get("plays", {})
    linescore_full = ld.get("linescore", {})
    boxscore = ld.get("boxscore", {})

    all_plays = []
    for p in plays_data.get("allPlays", []):
        result = p.get("result", {})
        about = p.get("about", {})
        matchup = p.get("matchup", {})

        play_obj = {
            "atBatIndex": p.get("atBatIndex"),
            "result": result.get("description", ""),
            "eventType": result.get("eventType", ""),
            "rbi": result.get("rbi", 0),
            "score": result.get("score", False),
            "about": {
                "halfInning": about.get("halfInning"),
                "inning": about.get("inning"),
                "isComplete": about.get("isComplete"),
                "isScoringPlay": about.get("isScoringPlay"),
                "hasOut": about.get("hasOut"),
            },
            "matchup": {
                "batter": {
                    "id": matchup.get("batter", {}).get("id"),
                    "fullName": matchup.get("batter", {}).get("fullName", ""),
                },
                "pitcher": {
                    "id": matchup.get("pitcher", {}).get("id"),
                    "fullName": matchup.get("pitcher", {}).get("fullName", ""),
                },
            },
            "pitches": [],
        }

        for ev in p.get("playEvents", []):
            det = ev.get("details", {})
            pd = ev.get("pitchData", {})
            coords = pd.get("coordinates", {})
            is_pitch = bool(pd.get("coordinates") or pd.get("startSpeed") or det.get("call", {}).get("code"))
            play_obj["pitches"].append({
                "eventId": ev.get("eventId"),
                "pitchNumber": ev.get("pitchNumber"),
                "isPitch": is_pitch,
                "type": det.get("type", {}).get("description", ""),
                "code": det.get("code", ""),
                "description": det.get("description", ""),
                "call": det.get("call", {}).get("description", ""),
                "callCode": det.get("call", {}).get("code", ""),
                "eventType": det.get("eventType", ""),
                "startSpeed": pd.get("startSpeed") or det.get("startSpeed"),
                "endSpeed": pd.get("endSpeed") or det.get("endSpeed"),
                "x": coords.get("x"),
                "y": coords.get("y"),
                "px": coords.get("pX"),
                "pz": coords.get("pZ"),
                "zone": pd.get("zone"),
                "szTop": pd.get("strikeZoneTop"),
                "szBottom": pd.get("strikeZoneBottom"),
                "isInPlay": det.get("isInPlay", False),
                "isStrike": det.get("isStrike", False),
                "isBall": det.get("isBall", False),
                "count": ev.get("count", {}),
                "hasDetails": bool(pd.get("coordinates")),
            })
        all_plays.append(play_obj)

    current_play = None
    for p in reversed(all_plays):
        if not p["about"]["isComplete"]:
            current_play = p
            break
    if current_play is None and all_plays:
        current_play = all_plays[-1]

    # Extract game events (mound visits, pitching changes, steals, etc.)
    game_events = []
    event_types_of_interest = {
        'mound_visit': ('Mound Visit', 'mound'),
        'pitching_substitution': ('Pitching Change', 'pitcher-change'),
        'offensive_substitution': ('Defensive Sub', 'sub'),
        'stolen_base_2b': ('Stolen Base', 'steal'),
        'stolen_base_3b': ('Stolen Base', 'steal'),
        'caught_stealing_2b': ('Caught Stealing', 'steal'),
        'caught_stealing_3b': ('Caught Stealing', 'steal'),
        'wild_pitch': ('Wild Pitch', 'event'),
        'passed_ball': ('Passed Ball', 'event'),
        'pickoff': ('Pickoff Attempt', 'mound'),
        'review': ('Replay Review', 'replay'),
        'challenge': ('Replay Review', 'replay'),
        'defensive_switch': ('Defensive Sub', 'sub'),
        'injury': ('Injury', 'injury'),
    }
    for p in all_plays:
        for ev in p.get("pitches", []):
            evt = ev.get("eventType", "")
            desc = ev.get("description", "")
            if not evt and desc:
                if 'Step Off' in desc: evt = 'stepoff'
                elif 'Hit By Pitch' in desc: evt = 'hit_by_pitch'
            if evt in event_types_of_interest:
                label, icon_type = event_types_of_interest[evt]
                game_events.append({
                    "type": icon_type,
                    "title": label if evt != 'pitching_substitution' else desc.split('.')[0] if '.' in desc else desc,
                    "description": desc,
                    "inning": f"{p.get('about',{}).get('halfInning','').replace('top','Top ').replace('bottom','Bot ')}{p.get('about',{}).get('inning','')}",
                })
    game_events.reverse()

    teams_box = boxscore.get("teams", {})
    def parse_team_box(team_data):
        players_data = team_data.get("players", {})
        batters = []
        pitchers = []
        for pid, pdata in players_data.items():
            stats = pdata.get("stats", {})
            batting = stats.get("batting", {})
            pitching = stats.get("pitching", {})
            person = pdata.get("person", {})
            position = pdata.get("position", {})

            if batting.get("atBats", 0) > 0 or batting.get("summary"):
                batters.append({
                    "id": person.get("id"),
                    "name": person.get("fullName", ""),
                    "position": position.get("abbreviation", ""),
                    "battingOrder": pdata.get("battingOrder", 0),
                    "ab": batting.get("atBats", 0),
                    "h": batting.get("hits", 0),
                    "r": batting.get("runs", 0),
                    "rbi": batting.get("rbi", 0),
                    "so": batting.get("strikeOuts", 0),
                    "bb": batting.get("baseOnBalls", 0),
                    "hr": batting.get("homeRuns", 0),
                    "tb": batting.get("totalBases", 0),
                    "sb": batting.get("stolenBases", 0),
                })
            if pitching.get("inningsPitched"):
                total_pitches = pitching.get("pitchesThrown", 0)
                strikes = pitching.get("strikesThrown", 0)
                strk_pct = f"{round(strikes/total_pitches*100)}%" if total_pitches > 0 else "0%"
                pitchers.append({
                    "id": person.get("id"),
                    "name": person.get("fullName", ""),
                    "ip": pitching.get("inningsPitched", "0.0"),
                    "h": pitching.get("hits", 0),
                    "r": pitching.get("runs", 0),
                    "er": pitching.get("earnedRuns", 0),
                    "k": pitching.get("strikeOuts", 0),
                    "bb": pitching.get("baseOnBalls", 0),
                    "hr": pitching.get("homeRuns", 0),
                    "era": pitching.get("era", "0.00"),
                    "strk": strk_pct,
                })
        batters.sort(key=lambda x: x.get("battingOrder", 99))
        return {"batters": batters, "pitchers": pitchers}

    def parse_linescore_detail():
        innings = linescore_full.get("innings", [])
        return [{"num": inn.get("num"), "away": inn.get("away", {}), "home": inn.get("home", {})} for inn in innings]

    cp_matchup = plays_data.get("currentPlay", {}).get("matchup", {})

    return {
        "gamePk": gamePk,
        "status": data.get("gameData", {}).get("status", {}),
        "plays": all_plays,
        "currentPlay": current_play,
        "linescore": {
            "inning": linescore_full.get("currentInning"),
            "inningState": linescore_full.get("inningState"),
            "inningHalf": linescore_full.get("inningHalf"),
            "isTopInning": linescore_full.get("isTopInning"),
            "outs": linescore_full.get("outs", 0),
            "balls": linescore_full.get("balls", 0),
            "strikes": linescore_full.get("strikes", 0),
            "inningOrdinal": linescore_full.get("currentInningOrdinal", ""),
            "scheduledInnings": linescore_full.get("scheduledInnings", 9),
            "offense": linescore_full.get("offense", {}),
        },
        "boxscore": {"away": parse_team_box(teams_box.get("away", {})), "home": parse_team_box(teams_box.get("home", {}))},
        "linescoreInnings": parse_linescore_detail(),
        "currentBatter": cp_matchup.get("batter"),
        "currentPitcher": cp_matchup.get("pitcher"),
        "gameEvents": game_events,
    }


@app.get("/api/standings")
async def get_standings():
    data = await cached_get(f"{MLB}/standings?leagueId=103,104&season=2025&hydrate=team", ttl=300)
    divisions = {}
    div_names = {201: "AL East", 202: "AL Central", 200: "AL West", 204: "NL East", 205: "NL Central", 203: "NL West"}
    for rec in data.get("records", []):
        div_id = rec.get("division", {}).get("id")
        teams = []
        for t in rec.get("teamRecords", []):
            team = t.get("team", {})
            abbr = team.get("abbreviation", "") or team.get("teamName", "")[:3].upper()
            teams.append({
                "abbr": abbr,
                "w": t.get("wins", 0),
                "l": t.get("losses", 0),
                "pct": t.get("winningPercentage", ".000"),
                "gb": t.get("gamesBack", "-"),
                "streak": t.get("streak", {}).get("streakCode", ""),
            })
        teams.sort(key=lambda x: float(x["pct"]), reverse=True)
        divisions[div_names.get(div_id, f"Div {div_id}")] = teams
    return divisions


@app.get("/")
def serve_index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))

app.mount("/", StaticFiles(directory=os.path.dirname(__file__)), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
