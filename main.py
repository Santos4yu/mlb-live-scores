import httpx
from dataclasses import dataclass, field
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
import asyncio, hashlib, json, logging, os, time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MLB = "https://statsapi.mlb.com/api/v1"
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 15
_client: httpx.AsyncClient | None = None
logger = logging.getLogger("live-scores")

LIVE_FEED_CHECK_SECONDS = max(0.2, float(os.environ.get("LIVE_FEED_CHECK_SECONDS", "0.25")))
HOT_FEED_TIMEOUT_SECONDS = max(
    0.35, float(os.environ.get("HOT_FEED_TIMEOUT_SECONDS", "0.65"))
)
FULL_FEED_MAX_AGE_SECONDS = max(
    2.0, float(os.environ.get("FULL_FEED_MAX_AGE_SECONDS", "5.0"))
)
PREGAME_FEED_CHECK_SECONDS = max(0.5, float(os.environ.get("PREGAME_FEED_CHECK_SECONDS", "1")))
FINAL_FEED_CHECK_SECONDS = max(5.0, float(os.environ.get("FINAL_FEED_CHECK_SECONDS", "15")))
FEED_KEEPALIVE_SECONDS = 1
FEED_REST_MAX_AGE_SECONDS = 0.1
MAX_FEED_STATES = max(8, int(os.environ.get("MAX_FEED_STATES", "64")))
FEED_STATE_TTL_SECONDS = max(
    300.0, float(os.environ.get("FEED_STATE_TTL_SECONDS", "1800"))
)
STANDINGS_CACHE_SECONDS = 900
FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
HOT_FEED_FIELDS = ",".join(
    (
        "metaData", "timeStamp", "gameData", "status", "abstractGameState",
        "detailedState", "liveData", "plays", "currentPlay", "atBatIndex",
        "result", "description", "eventType", "rbi", "score", "about",
        "halfInning", "inning", "isComplete", "isScoringPlay", "hasOut",
        "matchup", "batter", "pitcher", "id", "fullName", "batSide",
        "pitchHand", "code", "playEvents", "eventId", "pitchNumber",
        "playId", "index",
        "isPitch", "details", "type", "call", "startSpeed", "endSpeed",
        "isInPlay", "isStrike", "isBall", "pitchData", "coordinates",
        "x", "y", "pX", "pZ", "zone", "strikeZoneTop",
        "strikeZoneBottom", "count", "balls", "strikes", "outs",
        "linescore", "currentInning", "inningState", "inningHalf",
        "isTopInning", "currentInningOrdinal", "scheduledInnings",
        "offense", "defense", "team", "onDeck", "inHole", "first", "second",
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
    last_full_success_at: float = 0.0
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


def _hot_cache_buster() -> str:
    """Force every coalesced live check past upstream/CDN response caching."""
    return str(time.time_ns())


async def _fetch_hot_feed(game_pk: int) -> dict:
    """Fetch only pitch-critical fields while bypassing MLB's stale CDN cache."""
    client = await get_client()
    async with asyncio.timeout(HOT_FEED_TIMEOUT_SECONDS):
        response = await client.get(
            FEED_URL.format(game_pk=game_pk),
            params={"fields": HOT_FEED_FIELDS, "_": _hot_cache_buster()},
            timeout=httpx.Timeout(
                HOT_FEED_TIMEOUT_SECONDS,
                connect=min(0.75, HOT_FEED_TIMEOUT_SECONDS),
                pool=min(0.25, HOT_FEED_TIMEOUT_SECONDS),
            ),
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
                "currentPlayActive",
                "currentAlerts",
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
        for identity_key in ("atBatIndex", "eventId", "id"):
            hot_identity = hot.get(identity_key)
            cold_identity = cold.get(identity_key)
            if (
                hot_identity is not None
                and cold_identity is not None
                and hot_identity != cold_identity
            ):
                # This is a different at-bat, pitch, or player. Never fill its
                # intentionally empty fields from the previous entity.
                return hot
        return {
            key: (
                None if hot[key] is None
                else _prefer_hot(hot[key], cold.get(key))
            ) if key in hot
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
    for key in (
        "status",
        "currentPlay",
        "currentPlayActive",
        "linescore",
        "currentBatter",
        "currentPitcher",
    ):
        if hot.get(key) is not None:
            merged[key] = _prefer_hot(hot[key], merged.get(key))
    if "currentAlerts" in hot:
        # This is the alert set for exactly the projected current play. An
        # empty list on a new at-bat must clear the preceding play's alerts.
        merged["currentAlerts"] = hot["currentAlerts"]

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

        result = _process_feed(game_pk, raw)
        result_hot_revision = _hot_revision(result)
        full_is_behind = bool(
            required_timestamp
            and (full_timestamp is None or full_timestamp < required_timestamp)
        )
        full_timestamp = full_timestamp or expected_timestamp
        if full_is_behind:
            # The large endpoint can trail the projected endpoint by a pitch
            # while still being the only source of completed play history.
            # Accept that richer history, then lay the latest pitch-critical
            # state back over it so the live display never moves backward.
            result = _overlay_hot_projection(result, state.data or {})
        elif (
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
        state.last_full_success_at = time.monotonic()
        _store_processed_feed(state, result, "full")
        # A valid, current full response is successful even when its logical
        # payload is identical and therefore does not need publishing. A
        # lagging response still enriches history, but remains retryable so a
        # fully current large snapshot can catch up on the next opportunity.
        return not full_is_behind


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
            full_refresh_due = (
                not state.last_full_success_at
                or time.monotonic() - state.last_full_success_at
                >= FULL_FEED_MAX_AGE_SECONDS
            )
            if (
                not cancelled
                and pending is not None
                and (state.full_error_count or full_refresh_due)
            ):
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
                    full_refresh_due = (
                        not state.last_full_success_at
                        or time.monotonic() - state.last_full_success_at
                        >= FULL_FEED_MAX_AGE_SECONDS
                    )
                    if (
                        timestamp is not None
                        and timestamp != state.full_timestamp
                        and (state.full_error_count or full_refresh_due)
                    ):
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


TIMER_EVENT_LABELS = {
    "batter_timeout": ("Batter Timeout", "batter-timeout"),
    "pitch_timer_violation": ("Pitch Timer Violation", "pitch-timer"),
}
GAME_EVENT_LABELS = {
    "mound_visit": ("Mound Visit", "mound"),
    "pitching_substitution": ("Pitching Change", "pitcher-change"),
    "offensive_substitution": ("Offensive Sub", "sub"),
    "defensive_substitution": ("Defensive Sub", "sub"),
    "stolen_base_2b": ("Stolen Base", "steal"),
    "stolen_base_3b": ("Stolen Base", "steal"),
    "caught_stealing_2b": ("Caught Stealing", "steal"),
    "caught_stealing_3b": ("Caught Stealing", "steal"),
    "wild_pitch": ("Wild Pitch", "event"),
    "passed_ball": ("Passed Ball", "event"),
    "pickoff": ("Pickoff Attempt", "mound"),
    "stepoff": ("Pitcher Step Off", "mound"),
    "review": ("Replay Review", "replay"),
    "challenge": ("Replay Review", "replay"),
    "abs_challenge": ("ABS Challenge", "review"),
    "strikeout_abs_challenge": ("ABS Challenge", "review"),
    "defensive_switch": ("Defensive Sub", "sub"),
    "injury": ("Injury", "injury"),
    **TIMER_EVENT_LABELS,
}


def _event_is_pitch(event: dict) -> bool:
    explicit_is_pitch = event.get("isPitch")
    if explicit_is_pitch is not None:
        return bool(explicit_is_pitch)
    details = event.get("details", {})
    if str(details.get("eventType") or "").lower() in TIMER_EVENT_LABELS:
        return False
    event_kind = str(event.get("type") or "").lower()
    if event_kind == "action":
        return False
    if event_kind == "pitch":
        return True
    pitch_data = event.get("pitchData", {})
    return bool(
        pitch_data.get("coordinates")
        or pitch_data.get("startSpeed")
        or details.get("call", {}).get("code")
    )


def _strikeout_style(play_events: list[dict]) -> tuple[str | None, int]:
    pitch_events = [
        (index, event)
        for index, event in enumerate(play_events)
        if _event_is_pitch(event)
    ]
    if not pitch_events:
        return None, 0

    final_index, final_pitch = pitch_events[-1]
    for trailing_event in play_events[final_index + 1:]:
        trailing_type = str(
            trailing_event.get("details", {}).get("eventType") or ""
        ).lower()
        if trailing_type in TIMER_EVENT_LABELS:
            # A timer violation, rather than the preceding pitch, may have
            # produced strike three. Do not guess swinging versus looking.
            return None, len(pitch_events)

    details = final_pitch.get("details", {})
    call = details.get("call", {})
    code = str(call.get("code") or details.get("code") or "").upper()
    text = " ".join(
        str(value or "")
        for value in (call.get("description"), details.get("description"))
    ).lower()
    if any(
        phrase in text
        for phrase in ("swinging", "foul tip", "missed bunt", "foul bunt")
    ) or code in {"S", "W", "T", "M", "L"}:
        return "swinging", len(pitch_events)
    if "called strike" in text or code == "C":
        return "looking", len(pitch_events)
    return None, len(pitch_events)


def _game_event_alert(play: dict, event: dict) -> dict | None:
    event_type = str(event.get("eventType") or "").lower()
    description = str(event.get("description") or "")
    lowered = description.lower()
    if not event_type:
        if "step off" in lowered:
            event_type = "stepoff"
        elif "pickoff attempt" in lowered or "pickoff" in lowered:
            event_type = "pickoff"
        elif "hit by pitch" in lowered:
            event_type = "hit_by_pitch"
        elif "batter timeout" in lowered:
            event_type = "batter_timeout"
        elif (
            "pitch timer violation" in lowered
            or "pitch clock violation" in lowered
        ):
            event_type = "pitch_timer_violation"
        elif "abs challenge" in lowered or "automated ball-strike" in lowered:
            event_type = "abs_challenge"
        elif "challenge" in lowered and ("strike" in lowered or "ball" in lowered):
            event_type = "abs_challenge"
    if event_type not in GAME_EVENT_LABELS:
        return None

    label, icon_type = GAME_EVENT_LABELS[event_type]
    event_id = event.get("eventId") or (
        f"ab:{play.get('atBatIndex', 'unknown')}:"
        f"event:{event.get('eventIndex', 'unknown')}:{event_type}"
    )
    title = label
    if event_type == "pitching_substitution" and description:
        title = description.split(".", 1)[0]
    about = play.get("about", {})
    return {
        "type": icon_type,
        "title": title,
        "description": description,
        "inning": (
            f"{str(about.get('halfInning') or '').replace('top', 'Top ').replace('bottom', 'Bot ')}"
            f"{about.get('inning') or ''}"
        ),
        "eventId": event_id,
        "key": event_id,
    }


def _extract_distance(play_events: list[dict]) -> int | None:
    for ev in play_events:
        hit_data = ev.get("hitData") or {}
        dist = hit_data.get("totalDistance")
        if dist:
            return int(round(dist))
    return None


def _short_play_result(
    result: dict, play_events: list[dict] | None = None, distance: int | None = None
) -> str:
    event_type = str(result.get("eventType") or "").lower()
    description = str(result.get("description") or "")
    lowered = description.lower()
    rbi = result.get("rbi", 0) or 0
    rbi_prefix = ""
    if rbi == 1:
        rbi_prefix = "solo "
    elif rbi == 2:
        rbi_prefix = "2-run "
    elif rbi == 3:
        rbi_prefix = "3-run "
    elif rbi >= 4:
        rbi_prefix = "Grand slam "

    target = ""
    field_directions = (
        ("right field", "right"),
        ("left field", "left"),
        ("center field", "center"),
        ("right-center", "right-center"),
        ("left-center", "left-center"),
        ("up the middle", "up the middle"),
        ("third baseman", "third"),
        ("shortstop", "short"),
        ("second baseman", "second"),
        ("first baseman", "first"),
        ("left fielder", "left"),
        ("center fielder", "center"),
        ("right fielder", "right"),
        ("pitcher", "the mound"),
        ("catcher", "the plate"),
    )
    for phrase, label in field_directions:
        if phrase in lowered:
            target = f" to {label}"
            break

    if event_type == "force_out" or "grounds into a force out" in lowered:
        return f"Grounded into forceout{target}"
    if event_type in ("grounded_into_double_play", "double_play"):
        return f"Grounded into double play{target}"
    if event_type in ("groundout", "field_out") and "ground" in lowered:
        return f"Grounded out{target}"
    if event_type in ("flyout", "sac_fly") or "flies out" in lowered:
        return (
            f"Sacrifice fly{target}"
            if event_type == "sac_fly"
            else f"Flied out{target}"
        )
    if event_type == "lineout" or "lines out" in lowered:
        return f"Lined out{target}"
    if event_type in ("pop_out", "popout") or "pops out" in lowered:
        return f"Popped out{target}"

    if event_type == "strikeout":
        style, pitch_count = _strikeout_style(play_events or [])
        if style:
            prefix = "Three-pitch strikeout" if pitch_count == 3 else "Strikeout"
            return f"{prefix} {style}"

    simple_results = {
        "strikeout": "Struck out",
        "walk": "Walked",
        "intent_walk": "Intentionally walked",
        "hit_by_pitch": "Hit by pitch",
        "single": f"{rbi_prefix}Singled{target}" if rbi else f"Singled{target}",
        "double": f"{rbi_prefix}Doubled{target}" if rbi else f"Doubled{target}",
        "triple": f"{rbi_prefix}Tripled{target}" if rbi else f"Tripled{target}",
        "home_run": f"{rbi_prefix}{distance}' homer{target}" if distance else (f"{rbi_prefix}Homered{target}" if rbi else f"Homered{target}"),
        "field_error": "Reached on error",
        "fielders_choice": "Reached on fielder's choice",
        "sac_bunt": "Sacrifice bunt",
    }
    if event_type in simple_results:
        return simple_results[event_type]

    first_sentence = description.split(".", 1)[0].strip()
    return first_sentence[:80] if first_sentence else event_type.replace("_", " ").title()


def _process_play(play: dict) -> dict:
    result = play.get("result", {})
    about = play.get("about", {})
    matchup = play.get("matchup", {})
    result_event_type = str(result.get("eventType") or "").lower()
    result_description = str(result.get("description") or "")
    transient_timer_result = bool(
        about.get("isComplete") is not True
        and (
            result_event_type in TIMER_EVENT_LABELS
            or "batter timeout" in result_description.lower()
            or "pitch timer violation" in result_description.lower()
            or "pitch clock violation" in result_description.lower()
        )
    )
    display_result = {} if transient_timer_result else result
    play_events = play.get("playEvents", [])
    has_abs_challenge = False
    abs_resolved = False
    for _pe in play_events:
        _pe_details = _pe.get("details") if isinstance(_pe.get("details"), dict) else {}
        _pe_type = str(_pe_details.get("eventType") or "").lower()
        _pe_desc = str(_pe_details.get("description") or "").lower()
        if _pe_type in ("abs_challenge", "strikeout_abs_challenge") or "abs challenge" in _pe_desc or "automated ball-strike" in _pe_desc:
            has_abs_challenge = True
        if has_abs_challenge and _pe.get("isChallengeable") is False:
            abs_resolved = True
    transient_abs_challenge = bool(
        has_abs_challenge
        and not abs_resolved
        and about.get("isComplete") is not True
    )
    if transient_abs_challenge:
        display_result = {}
    play_obj = {
        "atBatIndex": play.get("atBatIndex"),
        "result": display_result.get("description", ""),
        "shortResult": _short_play_result(display_result, play_events, _extract_distance(play_events)),
        "distance": _extract_distance(play_events),
        "resultType": result.get("type", ""),
        "eventType": display_result.get("eventType", ""),
        "rbi": display_result.get("rbi", 0),
        "score": display_result.get("score", False),
        "about": {
            "halfInning": about.get("halfInning"),
            "inning": about.get("inning"),
            "isComplete": about.get("isComplete"),
            "isScoringPlay": about.get("isScoringPlay"),
            "hasOut": about.get("hasOut"),
            "outs": about.get("outs"),
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
    retained_events = play_events[-20:]
    retained_start = max(0, len(play_events) - len(retained_events))
    for fallback_index, event in enumerate(retained_events, start=retained_start):
        details = event.get("details", {})
        pitch_data = event.get("pitchData", {})
        coordinates = pitch_data.get("coordinates", {})
        event_index = (
            event.get("index")
            if event.get("index") is not None
            else fallback_index
        )
        source_event_id = event.get("playId") or event.get("eventId")
        event_id = (
            str(source_event_id)
            if source_event_id is not None
            else f"ab:{play.get('atBatIndex', 'unknown')}:event:{event_index}"
        )
        play_obj["pitches"].append({
            "eventId": event_id,
            "eventIndex": event_index,
            "pitchNumber": event.get("pitchNumber"),
            "isPitch": _event_is_pitch(event),
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
    processed_raw_current_play = (
        _process_play(raw_current_play) if raw_current_play else None
    )
    current_play = processed_raw_current_play
    if current_play is None:
        for play in reversed(all_plays):
            if not play["about"]["isComplete"]:
                current_play = play
                break
    if current_play is None and all_plays:
        current_play = all_plays[-1]

    game_events = []
    for p in all_plays:
        for ev in p.get("pitches", []):
            alert = _game_event_alert(p, ev)
            if alert is not None:
                game_events.append(alert)
    game_events.reverse()
    current_alerts = []
    if processed_raw_current_play is not None:
        for event in processed_raw_current_play.get("pitches", []):
            alert = _game_event_alert(processed_raw_current_play, event)
            if alert is not None:
                current_alerts.append(alert)

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
                season_batting = pdata.get("seasonStats", {}).get("batting", {})
                batters.append({
                    "id": person.get("id"), "name": person.get("fullName", ""),
                    "position": position.get("abbreviation", ""),
                    "battingOrder": pdata.get("battingOrder", 0),
                    "ab": batting.get("atBats", 0), "h": batting.get("hits", 0),
                    "r": batting.get("runs", 0), "rbi": batting.get("rbi", 0),
                    "so": batting.get("strikeOuts", 0), "bb": batting.get("baseOnBalls", 0),
                    "hr": batting.get("homeRuns", 0), "tb": batting.get("totalBases", 0),
                    "sb": batting.get("stolenBases", 0),
                    "seasonAvg": season_batting.get("avg"),
                    "seasonH": season_batting.get("hits"),
                    "seasonAB": season_batting.get("atBats"),
                })
            if (
                pitching.get("inningsPitched") is not None
                or pitching.get("numberOfPitches") is not None
                or pitching.get("pitchesThrown") is not None
            ):
                tp = pitching.get("numberOfPitches")
                if tp is None:
                    tp = pitching.get("pitchesThrown")
                st = pitching.get("strikes")
                if st is None:
                    st = pitching.get("strikesThrown")
                tp = tp or 0
                st = st or 0
                pitchers.append({
                    "id": person.get("id"), "name": person.get("fullName", ""),
                    "ip": pitching.get("inningsPitched", "0.0"), "h": pitching.get("hits", 0),
                    "r": pitching.get("runs", 0), "er": pitching.get("earnedRuns", 0),
                    "k": pitching.get("strikeOuts", 0), "bb": pitching.get("baseOnBalls", 0),
                    "hr": pitching.get("homeRuns", 0), "era": pitching.get("era", "0.00"),
                    "pitches": tp,
                    "strk": f"{round(st/tp*100)}%" if tp > 0 else "0%",
                })
        batters.sort(key=lambda x: x.get("battingOrder", 99))
        return {"batters": batters, "pitchers": pitchers}

    cp_matchup = plays_data.get("currentPlay", {}).get("matchup", {})
    line_offense = dict(linescore_full.get("offense", {}))
    for _base_key in ("first", "second", "third"):
        line_offense.setdefault(_base_key, line_offense.get(_base_key))
    line_defense = linescore_full.get("defense", {})
    offense_batter = line_offense.get("batter") or {}
    play_batter = cp_matchup.get("batter") or {}
    play_pitcher = cp_matchup.get("pitcher") or {}
    inning_state = linescore_full.get("inningState")
    between_innings = inning_state in ("Middle", "End")
    if between_innings:
        for _base_key in ("first", "second", "third"):
            line_offense[_base_key] = None
    matchup_is_current = bool(
        play_batter.get("id")
        and (
            not offense_batter.get("id")
            or play_batter.get("id") == offense_batter.get("id")
        )
    )
    defense_pitcher = line_defense.get("pitcher") or {}
    current_batter = (
        offense_batter
        if offense_batter.get("id")
        else play_batter if not between_innings else {}
    )
    current_pitcher = (
        defense_pitcher
        if defense_pitcher.get("id")
        else play_pitcher
        if matchup_is_current and not between_innings and play_pitcher.get("id")
        else {}
    )
    play_about = (raw_current_play or {}).get("about", {})
    play_half = str(play_about.get("halfInning") or "").lower()
    line_half = str(linescore_full.get("inningHalf") or "").lower()
    play_inning = play_about.get("inning")
    line_inning = linescore_full.get("currentInning")
    current_play_active = bool(
        raw_current_play
        and not between_innings
        and play_about.get("isComplete") is not True
        and (
            not offense_batter.get("id")
            or not play_batter.get("id")
            or offense_batter.get("id") == play_batter.get("id")
        )
        and (not play_half or not line_half or play_half == line_half)
        and (not play_inning or not line_inning or play_inning == line_inning)
    )

    return {
        "gamePk": gamePk,
        "status": data.get("gameData", {}).get("status", {}),
        "plays": all_plays,
        "currentPlay": current_play,
        "currentPlayActive": current_play_active,
        "currentAlerts": current_alerts,
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
            "offense": line_offense,
            "defense": line_defense,
            "score": {
                "away": linescore_full.get("teams", {}).get("away", {}).get("runs"),
                "home": linescore_full.get("teams", {}).get("home", {}).get("runs"),
            },
        },
        "boxscore": {"away": parse_team_box(teams_box.get("away", {})), "home": parse_team_box(teams_box.get("home", {}))},
        "linescoreInnings": [{"num": inn.get("num"), "away": inn.get("away", {}), "home": inn.get("home", {})} for inn in linescore_full.get("innings", [])],
        "currentBatter": current_batter,
        "currentPitcher": current_pitcher,
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
    state = _get_feed_state(gamePk)
    try:
        # When an SSE stream owns the shared poller, REST is a delivery
        # fallback only. Reusing its latest state avoids doubling MLB traffic
        # precisely while the stream is recovering.
        if not (
            state.data is not None
            and state.subscribers
            and state.poller is not None
            and not state.poller.done()
        ):
            state = await _refresh_feed_if_changed(gamePk)
    except Exception:
        return JSONResponse(
            {"error": "Live feed is temporarily unavailable", "status": "loading"},
            status_code=503,
            headers={"Cache-Control": "no-store"},
        )
    _set_feed_response_headers(response, state)
    return state.data


def _compact_multiwatch_payload(payload: str) -> str:
    """Keep pitch-critical Multi-Watch data without resending full box scores."""
    data = json.loads(payload)
    compact = {
        key: data.get(key)
        for key in (
            "gamePk", "status", "currentPlay", "currentPlayActive",
            "linescore", "currentBatter", "currentPitcher", "feedVersion",
            "feedOrder", "feedKind", "feedSourceTimestamp",
        )
    }
    completed_plays = []
    for play in data.get("plays") or []:
        if not (play.get("about") or {}).get("isComplete"):
            continue
        matchup = play.get("matchup") or {}
        completed_plays.append({
            "atBatIndex": play.get("atBatIndex"),
            "result": play.get("result"),
            "shortResult": play.get("shortResult"),
            "eventType": play.get("eventType"),
            "about": play.get("about"),
            "matchup": {"batter": matchup.get("batter") or {}},
        })
    compact["plays"] = completed_plays[-8:]
    return json.dumps(compact, ensure_ascii=False, separators=(",", ":"))


def _sse_message(revision: str, payload: str, compact: bool = False) -> str:
    if compact:
        payload = _compact_multiwatch_payload(payload)
    return f"id: {revision}\nevent: feed\ndata: {payload}\n\n"


def _feed_is_degraded(state: FeedState) -> bool:
    if state.error_count:
        return True
    if not state.last_success_at:
        return state.data is None
    max_success_age = HOT_FEED_TIMEOUT_SECONDS + _feed_poll_interval(state) + 0.5
    return time.monotonic() - state.last_success_at > max_success_age


@app.get("/api/game/{gamePk}/stream")
async def stream_game_feed(gamePk: int, request: Request, compact: bool = Query(False)):
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

            # Start the shared poller only after first paint, avoiding a
            # duplicate upstream request when a game is opened cold.
            _ensure_feed_poller(gamePk, state)

            if (
                state.payload is not None
                and state.revision is not None
                and state.revision != last_revision
            ):
                last_revision = state.revision
                yield _sse_message(state.revision, state.payload, compact)

            while True:
                if await request.is_disconnected():
                    break
                try:
                    revision, payload = await asyncio.wait_for(
                        queue.get(), timeout=FEED_KEEPALIVE_SECONDS
                    )
                except asyncio.TimeoutError:
                    degraded = "true" if _feed_is_degraded(state) else "false"
                    yield f'event: heartbeat\ndata: {{"degraded":{degraded}}}\n\n'
                    continue
                if revision == last_revision:
                    continue
                last_revision = revision
                yield _sse_message(revision, payload, compact)
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


def _current_mlb_season(timestamp: float | None = None) -> int:
    """Use the active season, retaining the prior season through the offseason."""
    today = time.gmtime(timestamp)
    return today.tm_year if today.tm_mon >= 3 else today.tm_year - 1


@app.get("/api/standings")
async def get_standings():
    season = _current_mlb_season()
    data = await cached_get(
        f"{MLB}/standings?leagueId=103,104&season={season}"
        "&standingsTypes=regularSeason&hydrate=team",
        ttl=STANDINGS_CACHE_SECONDS,
    )
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
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "index.html"),
        headers={"X-Content-Type-Options": "nosniff"},
    )


PUBLIC_ASSETS = {
    "app.js": "application/javascript",
    "styles.css": "text/css",
    "krazy.css": "text/css",
    "josoicon.png": "image/png",
}


@app.get("/{asset_name}")
def serve_public_asset(asset_name: str):
    media_type = PUBLIC_ASSETS.get(asset_name)
    if media_type is None:
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        os.path.join(os.path.dirname(__file__), asset_name),
        media_type=media_type,
        headers={"X-Content-Type-Options": "nosniff"},
    )


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
