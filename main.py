import httpx
from dataclasses import dataclass, field
from fastapi import FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
import asyncio, hashlib, json, logging, os, time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MLB = "https://statsapi.mlb.com/api/v1"
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 15
_client: httpx.AsyncClient | None = None
logger = logging.getLogger("live-scores")

LIVE_FEED_CHECK_SECONDS = max(0.15, float(os.environ.get("LIVE_FEED_CHECK_SECONDS", "0.25")))
PREGAME_FEED_CHECK_SECONDS = max(0.5, float(os.environ.get("PREGAME_FEED_CHECK_SECONDS", "1")))
FINAL_FEED_CHECK_SECONDS = max(5.0, float(os.environ.get("FINAL_FEED_CHECK_SECONDS", "15")))
FEED_KEEPALIVE_SECONDS = 2
FEED_REST_MAX_AGE_SECONDS = 0.2
MAX_FEED_STATES = max(8, int(os.environ.get("MAX_FEED_STATES", "64")))
FEED_STATE_TTL_SECONDS = max(
    300.0, float(os.environ.get("FEED_STATE_TTL_SECONDS", "1800"))
)
FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
HOT_FEED_FIELDS = ",".join(
    (
        "metaData", "timeStamp", "gameData", "status", "abstractGameState",
        "detailedState", "liveData", "plays", "currentPlay", "atBatIndex",
        "result", "description", "eventType", "rbi", "score", "about",
        "halfInning", "inning", "isComplete", "isScoringPlay", "hasOut",
        "matchup", "batter", "pitcher", "id", "fullName", "batSide",
        "pitchHand", "code", "playEvents", "eventId", "pitchNumber",
        "playId",
        "isPitch", "details", "type", "call", "startSpeed", "endSpeed",
        "isInPlay", "isStrike", "isBall", "pitchData", "coordinates",
        "x", "y", "pX", "pZ", "zone", "strikeZoneTop",
        "strikeZoneBottom", "count", "balls", "strikes", "outs",
        "linescore", "currentInning", "inningState", "inningHalf",
        "isTopInning", "currentInningOrdinal", "scheduledInnings",
        "offense", "team", "onDeck", "inHole", "first", "second",
        "third", "battingOrder", "teams", "away", "home", "runs",
        "hits", "errors",
    )
)

async def get_client():
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(8.0, connect=3.0, pool=2.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Cache-Control": "no-cache",
            },
        )
    return _client

async def cached_get(url: str, ttl: int = CACHE_TTL, timeout: int = 20) -> dict:
    now = time.time()
    if url in _cache and now - _cache[url][0] < ttl:
        return _cache[url][1]
    client = await get_client()
    r = await client.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    _cache[url] = (time.time(), data)
    return data

@dataclass
class FeedState:
    data: dict | None = None
    payload: str | None = None
    revision: str | None = None
    source_timestamp: str | None = None
    full_timestamp: str | None = None
    hot_revision: str | None = None
    version_order: int = 0
    checked_at: float = 0.0
    last_success_at: float = 0.0
    last_accessed_at: float = field(default_factory=time.monotonic)
    updated_at: float = 0.0
    last_upstream_ms: float = 0.0
    error_count: int = 0
    last_error: str | None = None
    full_error_count: int = 0
    next_full_retry_at: float = 0.0
    last_full_attempt_timestamp: str | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    full_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    subscribers: set[asyncio.Queue] = field(default_factory=set)
    poller: asyncio.Task | None = None
    enrichment_task: asyncio.Task | None = None
    enrichment_timestamp: str | None = None
    pending_enrichment_timestamp: str | None = None


_feed_states: dict[int, FeedState] = {}


def _prune_feed_states(now: float) -> None:
    """Bound retained payloads without touching active streams or fetches."""
    removable = sorted(
        (
            (game_pk, state)
            for game_pk, state in _feed_states.items()
            if not state.subscribers
            and not state.lock.locked()
            and not state.full_lock.locked()
            and (state.poller is None or state.poller.done())
            and (state.enrichment_task is None or state.enrichment_task.done())
        ),
        key=lambda item: item[1].last_accessed_at,
    )
    for game_pk, state in removable:
        expired = now - state.last_accessed_at >= FEED_STATE_TTL_SECONDS
        over_limit = len(_feed_states) >= MAX_FEED_STATES
        if not expired and not over_limit:
            break
        _feed_states.pop(game_pk, None)


def _get_feed_state(game_pk: int) -> FeedState:
    now = time.monotonic()
    state = _feed_states.get(game_pk)
    if state is None:
        _prune_feed_states(now)
        state = FeedState()
        _feed_states[game_pk] = state
    state.last_accessed_at = now
    return state


def _cache_buster() -> str:
    return str(time.time_ns())


async def _fetch_hot_feed(game_pk: int) -> dict:
    """Fetch only pitch-critical fields while bypassing MLB's stale CDN cache."""
    client = await get_client()
    response = await client.get(
        FEED_URL.format(game_pk=game_pk),
        params={"fields": HOT_FEED_FIELDS, "_": _cache_buster()},
    )
    response.raise_for_status()
    return response.json()


def _version_feed(result: dict) -> tuple[str, dict]:
    result = {
        key: value
        for key, value in result.items()
        if key not in (
            "feedVersion",
            "feedOrder",
            "feedKind",
            "feedSourceTimestamp",
        )
    }
    logical_payload = json.dumps(
        result, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    revision = hashlib.blake2s(
        logical_payload.encode("utf-8"), digest_size=8
    ).hexdigest()
    versioned_result = {**result, "feedVersion": revision}
    return revision, versioned_result


def _publish_feed(state: FeedState) -> None:
    if state.payload is None or state.revision is None:
        return
    update = (state.revision, state.payload)
    for queue in tuple(state.subscribers):
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            queue.put_nowait(update)
        except asyncio.QueueFull:
            pass


def _store_processed_feed(state: FeedState, result: dict, kind: str) -> bool:
    revision, versioned_result = _version_feed(result)
    if revision == state.revision:
        return False
    state.version_order = max(int(time.time() * 1000), state.version_order + 1)
    versioned_result["feedOrder"] = state.version_order
    versioned_result["feedKind"] = kind
    versioned_result["feedSourceTimestamp"] = state.source_timestamp
    payload = json.dumps(
        versioned_result, ensure_ascii=False, separators=(",", ":")
    )
    state.data = versioned_result
    state.payload = payload
    state.revision = revision
    state.updated_at = time.monotonic()
    _publish_feed(state)
    return True


def _hot_revision(result: dict) -> str:
    payload = json.dumps(
        {
            key: result.get(key)
            for key in (
                "status",
                "currentPlay",
                "linescore",
                "currentBatter",
                "currentPitcher",
            )
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.blake2s(payload.encode("utf-8"), digest_size=8).hexdigest()


def _feed_body(data: dict) -> dict:
    return {
        key: value
        for key, value in data.items()
        if key not in (
            "feedVersion",
            "feedOrder",
            "feedKind",
            "feedSourceTimestamp",
        )
    }


def _prefer_hot(hot, cold):
    """Keep the newest projection while filling fields it does not have yet."""
    if hot is None or hot == "":
        return cold
    if isinstance(hot, dict) and isinstance(cold, dict):
        return {
            key: _prefer_hot(hot[key], cold.get(key))
            if key in hot
            else cold[key]
            for key in cold.keys() | hot.keys()
        }
    if isinstance(hot, list) and isinstance(cold, list):
        if not hot:
            return cold
        if all(isinstance(item, dict) for item in hot + cold):
            def item_keys(item):
                return tuple(
                    (key, item[key])
                    for key in ("eventId", "pitchNumber", "atBatIndex", "id")
                    if item.get(key) is not None
                )

            cold_by_key = {
                key: item
                for item in cold
                for key in item_keys(item)
            }
            return [
                _prefer_hot(
                    item,
                    next(
                        (
                            cold_by_key[key]
                            for key in item_keys(item)
                            if key in cold_by_key
                        ),
                        {},
                    ),
                )
                for item in hot
            ]
        if len(hot) == len(cold):
            return [_prefer_hot(new, old) for new, old in zip(hot, cold)]
    return hot


def _hot_progress(result: dict) -> tuple[int, int, int, int]:
    """Order pitch-critical state independently from MLB's coarse timestamp."""
    play = result.get("currentPlay") or {}
    pitches = [pitch for pitch in play.get("pitches", []) if pitch.get("isPitch")]
    pitch_numbers = [
        pitch.get("pitchNumber")
        for pitch in pitches
        if isinstance(pitch.get("pitchNumber"), int)
    ]
    at_bat_index = play.get("atBatIndex")
    return (
        at_bat_index if isinstance(at_bat_index, int) else -1,
        len(pitches),
        max(pitch_numbers, default=-1),
        int(bool((play.get("about") or {}).get("isComplete"))),
    )


def _overlay_hot_projection(base: dict, hot: dict) -> dict:
    merged = _feed_body(base)
    for key in ("status", "currentPlay", "linescore", "currentBatter", "currentPitcher"):
        if hot.get(key) is not None:
            merged[key] = _prefer_hot(hot[key], merged.get(key))

    current_play = merged.get("currentPlay")
    if current_play is not None:
        plays = list(merged.get("plays") or [])
        at_bat_index = current_play.get("atBatIndex")
        replaced = False
        for index in range(len(plays) - 1, -1, -1):
            if plays[index].get("atBatIndex") == at_bat_index:
                plays[index] = current_play
                replaced = True
                break
        if not replaced:
            plays.append(current_play)
        merged["plays"] = plays[-40:]
    return merged


def _merge_hot_feed(game_pk: int, state: FeedState, raw: dict) -> bool:
    """Publish the current pitch immediately while the full feed catches up."""
    if state.data is None:
        return False
    hot = _process_feed(game_pk, raw)
    hot_revision = _hot_revision(hot)
    if hot_revision == state.hot_revision:
        return False
    state.hot_revision = hot_revision
    if _hot_progress(hot) < _hot_progress(state.data):
        # MLB occasionally publishes a newer but partial snapshot. Keep the
        # last pitch projection so the UI never flashes backwards.
        hot["currentPlay"] = state.data.get("currentPlay")
    merged = _overlay_hot_projection(state.data, hot)
    return _store_processed_feed(state, merged, "hot")


async def _refresh_full_feed(
    game_pk: int, state: FeedState, expected_timestamp: str | None = None
) -> bool:
    """Fetch cold game data without blocking the pitch-critical hot loop."""
    async with state.full_lock:
        started = time.perf_counter()
        client = await get_client()
        response = await client.get(
            FEED_URL.format(game_pk=game_pk),
            # MLB advertises stale-while-revalidate for the bare URL. A unique
            # query key is required or the app can receive data tens of seconds old.
            params={"_": f"{expected_timestamp or 'initial'}-{_cache_buster()}"},
        )
        response.raise_for_status()
        raw = response.json()
        source_timestamp = raw.get("metaData", {}).get("timeStamp")
        full_timestamp = str(source_timestamp) if source_timestamp else None
        state.last_upstream_ms = (time.perf_counter() - started) * 1000
        if state.data is None:
            state.checked_at = time.monotonic()

        required_timestamp = expected_timestamp
        if (
            state.source_timestamp
            and (
                required_timestamp is None
                or state.source_timestamp > required_timestamp
            )
        ):
            required_timestamp = state.source_timestamp

        # Never let a lagging CDN snapshot overwrite a pitch already pushed.
        if required_timestamp and (
            full_timestamp is None or full_timestamp < required_timestamp
        ):
            return False

        result = _process_feed(game_pk, raw)
        result_hot_revision = _hot_revision(result)
        full_timestamp = full_timestamp or expected_timestamp
        if (
            state.data is not None
            and full_timestamp is not None
            and full_timestamp == state.source_timestamp
            and _hot_progress(result) <= _hot_progress(state.data)
        ):
            # The large response can be richer but must not move the live
            # projection backwards when both snapshots share a timestamp.
            result = _overlay_hot_projection(result, state.data)
        else:
            state.hot_revision = result_hot_revision

        state.full_timestamp = full_timestamp
        if (
            state.source_timestamp is None
            or (full_timestamp is not None and full_timestamp > state.source_timestamp)
        ):
            state.source_timestamp = full_timestamp
        state.full_error_count = 0
        state.next_full_retry_at = 0.0
        return _store_processed_feed(state, result, "full")


async def _run_full_enrichment(
    game_pk: int, state: FeedState, timestamp: str
) -> None:
    current_task = asyncio.current_task()
    cancelled = False
    state.last_full_attempt_timestamp = timestamp
    try:
        accepted = await _refresh_full_feed(game_pk, state, timestamp)
        if not accepted:
            state.full_error_count += 1
            state.next_full_retry_at = time.monotonic() + min(
                4.0, 0.5 * (2 ** min(state.full_error_count - 1, 3))
            )
    except asyncio.CancelledError:
        cancelled = True
        return
    except Exception as exc:
        state.full_error_count += 1
        state.next_full_retry_at = time.monotonic() + min(
            8.0, 0.5 * (2 ** min(state.full_error_count - 1, 4))
        )
        if state.full_error_count == 1 or state.full_error_count % 10 == 0:
            logger.warning(
                "Full feed enrichment failed for game %s (%s consecutive): %s",
                game_pk,
                state.full_error_count,
                exc,
            )
    finally:
        if state.enrichment_task is current_task:
            state.enrichment_task = None
            state.enrichment_timestamp = None
            pending = state.pending_enrichment_timestamp
            state.pending_enrichment_timestamp = None
            if not cancelled and pending is not None:
                _schedule_full_enrichment(game_pk, state, pending)


def _schedule_full_enrichment(
    game_pk: int, state: FeedState, timestamp: str
) -> None:
    if timestamp == state.full_timestamp:
        return
    existing = state.enrichment_task
    if existing is not None and not existing.done():
        if state.enrichment_timestamp == timestamp:
            return
        if state.enrichment_timestamp and state.enrichment_timestamp > timestamp:
            return
        if (
            state.pending_enrichment_timestamp is None
            or timestamp > state.pending_enrichment_timestamp
        ):
            # Let the current request finish and retain only the newest follow-up.
            # Repeated pitch timestamps therefore cannot starve cold enrichment.
            state.pending_enrichment_timestamp = timestamp
        return

    if (
        state.last_full_attempt_timestamp == timestamp
        and time.monotonic() < state.next_full_retry_at
    ):
        return
    state.enrichment_timestamp = timestamp
    state.enrichment_task = asyncio.create_task(
        _run_full_enrichment(game_pk, state, timestamp),
        name=f"full-feed-{game_pk}-{timestamp}",
    )


def _record_feed_error(game_pk: int, state: FeedState, exc: Exception) -> None:
    state.error_count += 1
    state.checked_at = time.monotonic()
    state.last_error = str(exc)
    if state.error_count == 1 or state.error_count % 20 == 0:
        logger.warning(
            "Live feed refresh failed for game %s (%s consecutive): %s",
            game_pk,
            state.error_count,
            exc,
        )


def _record_feed_success(state: FeedState) -> None:
    now = time.monotonic()
    state.checked_at = now
    state.last_success_at = now
    state.error_count = 0
    state.last_error = None


async def _refresh_feed_if_changed(
    game_pk: int,
    *,
    min_check_age: float = FEED_REST_MAX_AGE_SECONDS,
    force_full: bool = False,
) -> FeedState:
    """Coalesce refreshes and download the large feed only when MLB changes it."""
    state = _get_feed_state(game_pk)
    now = time.monotonic()
    if (
        not force_full
        and state.data is not None
        and now - state.checked_at < min_check_age
    ):
        return state

    async with state.lock:
        now = time.monotonic()
        if (
            not force_full
            and state.data is not None
            and now - state.checked_at < min_check_age
        ):
            return state

        try:
            if force_full:
                await _refresh_full_feed(game_pk, state)
                _record_feed_success(state)
            else:
                started = time.perf_counter()
                try:
                    hot_raw = await _fetch_hot_feed(game_pk)
                except Exception:
                    if state.data is not None:
                        raise
                    # Keep a cold-start escape hatch if the projected endpoint
                    # is temporarily unavailable.
                    await _refresh_full_feed(game_pk, state)
                    _record_feed_success(state)
                    return state

                state.last_upstream_ms = (time.perf_counter() - started) * 1000
                _record_feed_success(state)
                source_timestamp = hot_raw.get("metaData", {}).get("timeStamp")
                timestamp = str(source_timestamp) if source_timestamp else None
                timestamp_is_older = bool(
                    timestamp
                    and state.source_timestamp
                    and timestamp < state.source_timestamp
                )
                if not timestamp_is_older:
                    if timestamp is not None:
                        state.source_timestamp = timestamp
                    if state.data is None:
                        hot = _process_feed(game_pk, hot_raw)
                        state.hot_revision = _hot_revision(hot)
                        _store_processed_feed(state, hot, "hot")
                    else:
                        _merge_hot_feed(game_pk, state, hot_raw)
                    if timestamp is not None and timestamp != state.full_timestamp:
                        _schedule_full_enrichment(game_pk, state, timestamp)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _record_feed_error(game_pk, state, exc)
            if state.data is None:
                raise

    return state


def _feed_poll_interval(state: FeedState) -> float:
    status = (state.data or {}).get("status", {}).get("abstractGameState")
    if status == "Final":
        interval = FINAL_FEED_CHECK_SECONDS
    elif status == "Live" or state.data is None:
        interval = LIVE_FEED_CHECK_SECONDS
    else:
        interval = PREGAME_FEED_CHECK_SECONDS
    if state.error_count:
        interval = max(interval, min(4.0, LIVE_FEED_CHECK_SECONDS * (2 ** min(state.error_count, 4))))
    return interval


async def _run_feed_poller(game_pk: int, state: FeedState) -> None:
    try:
        while state.subscribers:
            started = time.monotonic()
            try:
                await _refresh_feed_if_changed(game_pk, min_check_age=0)
            except Exception:
                # The refresh function records and throttles the error log.
                pass
            delay = max(0.01, _feed_poll_interval(state) - (time.monotonic() - started))
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        pass
    finally:
        state.poller = None
        if state.subscribers:
            _ensure_feed_poller(game_pk, state)


def _ensure_feed_poller(game_pk: int, state: FeedState) -> None:
    if state.poller is None or state.poller.done():
        state.poller = asyncio.create_task(
            _run_feed_poller(game_pk, state),
            name=f"live-feed-{game_pk}",
        )

def _process_play(play: dict) -> dict:
    result = play.get("result", {})
    about = play.get("about", {})
    matchup = play.get("matchup", {})
    play_obj = {
        "atBatIndex": play.get("atBatIndex"),
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
    for event in play.get("playEvents", [])[-20:]:
        details = event.get("details", {})
        pitch_data = event.get("pitchData", {})
        coordinates = pitch_data.get("coordinates", {})
        explicit_is_pitch = event.get("isPitch")
        is_pitch = (
            bool(explicit_is_pitch)
            if explicit_is_pitch is not None
            else bool(
                coordinates
                or pitch_data.get("startSpeed")
                or details.get("call", {}).get("code")
            )
        )
        play_obj["pitches"].append({
            "eventId": event.get("playId") or event.get("eventId"),
            "pitchNumber": event.get("pitchNumber"),
            "isPitch": is_pitch,
            "type": details.get("type", {}).get("description", ""),
            "code": details.get("code", ""),
            "description": details.get("description", ""),
            "call": details.get("call", {}).get("description", ""),
            "callCode": details.get("call", {}).get("code", ""),
            "eventType": details.get("eventType", ""),
            "startSpeed": pitch_data.get("startSpeed") or details.get("startSpeed"),
            "endSpeed": pitch_data.get("endSpeed") or details.get("endSpeed"),
            "x": coordinates.get("x"),
            "y": coordinates.get("y"),
            "px": coordinates.get("pX"),
            "pz": coordinates.get("pZ"),
            "zone": pitch_data.get("zone"),
            "szTop": pitch_data.get("strikeZoneTop"),
            "szBottom": pitch_data.get("strikeZoneBottom"),
            "isInPlay": details.get("isInPlay", False),
            "isStrike": details.get("isStrike", False),
            "isBall": details.get("isBall", False),
            "count": event.get("count", {}),
            "hasDetails": bool(coordinates),
        })
    return play_obj


def _process_feed(gamePk, data):
    ld = data.get("liveData", {})
    plays_data = ld.get("plays", {})
    linescore_full = ld.get("linescore", {})
    boxscore = ld.get("boxscore", {})

    raw_plays = plays_data.get("allPlays", [])
    all_plays = [_process_play(play) for play in raw_plays[-40:]]

    raw_current_play = plays_data.get("currentPlay")
    current_play = _process_play(raw_current_play) if raw_current_play else None
    if current_play is None:
        for play in reversed(all_plays):
            if not play["about"]["isComplete"]:
                current_play = play
                break
    if current_play is None and all_plays:
        current_play = all_plays[-1]

    event_types = {
        'mound_visit': ('Mound Visit', 'mound'),
        'pitching_substitution': ('Pitching Change', 'pitcher-change'),
        'offensive_substitution': ('Defensive Sub', 'sub'),
        'stolen_base_2b': ('Stolen Base', 'steal'), 'stolen_base_3b': ('Stolen Base', 'steal'),
        'caught_stealing_2b': ('Caught Stealing', 'steal'), 'caught_stealing_3b': ('Caught Stealing', 'steal'),
        'wild_pitch': ('Wild Pitch', 'event'), 'passed_ball': ('Passed Ball', 'event'),
        'pickoff': ('Pickoff Attempt', 'mound'),
        'review': ('Replay Review', 'replay'), 'challenge': ('Replay Review', 'replay'),
        'defensive_switch': ('Defensive Sub', 'sub'), 'injury': ('Injury', 'injury'),
    }
    game_events = []
    for p in all_plays:
        for ev in p.get("pitches", []):
            evt = ev.get("eventType", "")
            desc = ev.get("description", "")
            if not evt and desc:
                if 'Step Off' in desc: evt = 'stepoff'
                elif 'Hit By Pitch' in desc: evt = 'hit_by_pitch'
            if evt in event_types:
                label, icon_type = event_types[evt]
                game_events.append({
                    "type": icon_type,
                    "title": label if evt != 'pitching_substitution' else desc.split('.')[0] if '.' in desc else desc,
                    "description": desc,
                    "inning": f"{p.get('about',{}).get('halfInning','').replace('top','Top ').replace('bottom','Bot ')}{p.get('about',{}).get('inning','')}",
                })
    game_events.reverse()

    teams_box = boxscore.get("teams", {})
    def parse_team_box(team_data):
        batters, pitchers = [], []
        for pid, pdata in team_data.get("players", {}).items():
            stats = pdata.get("stats", {})
            batting = stats.get("batting", {})
            pitching = stats.get("pitching", {})
            person = pdata.get("person", {})
            position = pdata.get("position", {})
            if batting.get("atBats", 0) > 0 or batting.get("summary"):
                batters.append({
                    "id": person.get("id"), "name": person.get("fullName", ""),
                    "position": position.get("abbreviation", ""),
                    "battingOrder": pdata.get("battingOrder", 0),
                    "ab": batting.get("atBats", 0), "h": batting.get("hits", 0),
                    "r": batting.get("runs", 0), "rbi": batting.get("rbi", 0),
                    "so": batting.get("strikeOuts", 0), "bb": batting.get("baseOnBalls", 0),
                    "hr": batting.get("homeRuns", 0), "tb": batting.get("totalBases", 0),
                    "sb": batting.get("stolenBases", 0),
                })
            if pitching.get("inningsPitched"):
                tp = pitching.get("pitchesThrown", 0)
                st = pitching.get("strikesThrown", 0)
                pitchers.append({
                    "id": person.get("id"), "name": person.get("fullName", ""),
                    "ip": pitching.get("inningsPitched", "0.0"), "h": pitching.get("hits", 0),
                    "r": pitching.get("runs", 0), "er": pitching.get("earnedRuns", 0),
                    "k": pitching.get("strikeOuts", 0), "bb": pitching.get("baseOnBalls", 0),
                    "hr": pitching.get("homeRuns", 0), "era": pitching.get("era", "0.00"),
                    "strk": f"{round(st/tp*100)}%" if tp > 0 else "0%",
                })
        batters.sort(key=lambda x: x.get("battingOrder", 99))
        return {"batters": batters, "pitchers": pitchers}

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
            "score": {
                "away": linescore_full.get("teams", {}).get("away", {}).get("runs"),
                "home": linescore_full.get("teams", {}).get("home", {}).get("runs"),
            },
        },
        "boxscore": {"away": parse_team_box(teams_box.get("away", {})), "home": parse_team_box(teams_box.get("home", {}))},
        "linescoreInnings": [{"num": inn.get("num"), "away": inn.get("away", {}), "home": inn.get("home", {})} for inn in linescore_full.get("innings", [])],
        "currentBatter": cp_matchup.get("batter"),
        "currentPitcher": cp_matchup.get("pitcher"),
        "gameEvents": game_events,
    }

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


def _set_feed_response_headers(response: Response, state: FeedState) -> None:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    if state.revision:
        response.headers["X-Feed-Version"] = state.revision
    response.headers["X-Feed-Checked-Age-Ms"] = str(
        max(0, round((time.monotonic() - state.checked_at) * 1000))
    )
    if state.last_success_at:
        response.headers["X-Feed-Success-Age-Ms"] = str(
            max(0, round((time.monotonic() - state.last_success_at) * 1000))
        )
    response.headers["X-Feed-Degraded"] = "1" if state.error_count else "0"
    response.headers["Server-Timing"] = f'upstream;dur={state.last_upstream_ms:.1f}'


@app.get("/api/game/{gamePk}/feed")
async def get_game_feed(gamePk: int, response: Response):
    try:
        state = await _refresh_feed_if_changed(gamePk)
    except Exception:
        return JSONResponse(
            {"error": "Live feed is temporarily unavailable", "status": "loading"},
            status_code=503,
            headers={"Cache-Control": "no-store"},
        )
    _set_feed_response_headers(response, state)
    return state.data


def _sse_message(revision: str, payload: str) -> str:
    return f"id: {revision}\nevent: feed\ndata: {payload}\n\n"


@app.get("/api/game/{gamePk}/stream")
async def stream_game_feed(gamePk: int, request: Request):
    """Push feed changes immediately; all viewers of a game share one MLB poller."""
    state = _get_feed_state(gamePk)
    queue: asyncio.Queue = asyncio.Queue(maxsize=1)
    state.subscribers.add(queue)
    last_revision = request.headers.get("last-event-id")

    async def events():
        nonlocal last_revision
        try:
            if state.payload is None:
                try:
                    await _refresh_feed_if_changed(gamePk)
                except Exception:
                    yield "event: feed-error\ndata: unavailable\n\n"

            # Start the shared 4 Hz poller only after first paint, avoiding a
            # duplicate upstream request when a game is opened cold.
            _ensure_feed_poller(gamePk, state)

            if (
                state.payload is not None
                and state.revision is not None
                and state.revision != last_revision
            ):
                last_revision = state.revision
                yield _sse_message(state.revision, state.payload)

            while True:
                if await request.is_disconnected():
                    break
                try:
                    revision, payload = await asyncio.wait_for(
                        queue.get(), timeout=FEED_KEEPALIVE_SECONDS
                    )
                except asyncio.TimeoutError:
                    degraded = "true" if state.error_count else "false"
                    yield f'event: heartbeat\ndata: {{"degraded":{degraded}}}\n\n'
                    continue
                if revision == last_revision:
                    continue
                last_revision = revision
                yield _sse_message(revision, payload)
        finally:
            state.subscribers.discard(queue)
            if not state.subscribers and state.poller is not None:
                state.poller.cancel()

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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


@app.on_event("shutdown")
async def close_live_feed_client():
    tasks = [
        task
        for state in _feed_states.values()
        for task in (state.poller, state.enrichment_task)
        if task is not None and not task.done()
    ]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    if _client is not None and not _client.is_closed:
        await _client.aclose()


app.mount("/", StaticFiles(directory=os.path.dirname(__file__)), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
