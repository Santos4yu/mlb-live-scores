import asyncio
import copy
import unittest

import main


def raw_feed(
    timestamp="20260730_010000",
    event_id="pitch-1",
    *,
    pitch_number=1,
    start_speed=None,
    call_code=None,
    call_description=None,
    description=None,
    pitch_type=None,
    px=None,
    pz=None,
):
    details = {}
    call = {}
    if call_code is not None:
        call["code"] = call_code
    if call_description is not None:
        call["description"] = call_description
    if call:
        details["call"] = call
    if description is not None:
        details["description"] = description
    if pitch_type is not None:
        details["type"] = {"description": pitch_type}

    pitch_data = {}
    if start_speed is not None:
        pitch_data["startSpeed"] = start_speed
    if px is not None or pz is not None:
        pitch_data["coordinates"] = {"pX": px, "pZ": pz}

    play = {
        "atBatIndex": 0,
        "result": {},
        "about": {"isComplete": False},
        "matchup": {"batter": {}, "pitcher": {}},
        "playEvents": [
            {
                "playId": event_id,
                "pitchNumber": pitch_number,
                # MLB can mark the event as a pitch before
                # coordinates, velocity, or the call arrive.
                "isPitch": True,
                "details": details,
                "pitchData": pitch_data,
                "count": {"balls": 0, "strikes": 0, "outs": 0},
            }
        ],
    }
    return {
        "metaData": {"timeStamp": timestamp},
        "gameData": {
            "status": {
                "abstractGameState": "Live",
                "detailedState": "In Progress",
            }
        },
        "liveData": {
            "plays": {
                "currentPlay": play,
                "allPlays": [play],
            },
            "linescore": {},
            "boxscore": {"teams": {}},
        },
    }


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        return None

    def json(self):
        return copy.deepcopy(self._data)


class FakeClient:
    def __init__(self):
        self.timestamp = "20260730_010000"
        self.event_id = "pitch-1"
        self.full_calls = 0
        self.hot_calls = 0
        self.requests = []
        self.timestamp_delay = 0
        self.full_delay = 0
        self.full_timestamp_override = None
        self.full_event_override = None
        self.pitch = {"pitch_number": 1}
        self.full_pitch_override = None

    async def get(self, url, **kwargs):
        params = kwargs.get("params") or {}
        self.requests.append((url, params))
        if "fields" in params:
            self.hot_calls += 1
            if self.timestamp_delay:
                await asyncio.sleep(self.timestamp_delay)
            return FakeResponse(
                raw_feed(self.timestamp, self.event_id, **copy.deepcopy(self.pitch))
            )

        self.full_calls += 1
        # Capture the response snapshot before the delay. This models an
        # in-flight request while newer hot timestamps continue to arrive.
        timestamp = self.full_timestamp_override or self.timestamp
        event_id = self.full_event_override or self.event_id
        pitch = copy.deepcopy(
            self.full_pitch_override
            if self.full_pitch_override is not None
            else self.pitch
        )
        response = raw_feed(timestamp, event_id, **pitch)
        if self.full_delay:
            await asyncio.sleep(self.full_delay)
        return FakeResponse(response)


class ProcessFeedTests(unittest.TestCase):
    def test_explicit_pitch_is_visible_before_richer_pitch_data(self):
        result = main._process_feed(123, raw_feed())

        pitches = result["currentPlay"]["pitches"]
        self.assertEqual(1, len(pitches))
        self.assertTrue(pitches[0]["isPitch"])
        self.assertEqual("pitch-1", pitches[0]["eventId"])

    def test_partial_newer_hot_snapshot_does_not_remove_a_pitch(self):
        first = raw_feed("20260730_010000", "pitch-1")
        second_event = copy.deepcopy(
            first["liveData"]["plays"]["currentPlay"]["playEvents"][0]
        )
        second_event["playId"] = "pitch-2"
        second_event["pitchNumber"] = 2
        first["liveData"]["plays"]["currentPlay"]["playEvents"].append(second_event)
        first["liveData"]["plays"]["allPlays"][0]["playEvents"] = copy.deepcopy(
            first["liveData"]["plays"]["currentPlay"]["playEvents"]
        )
        state = main.FeedState()
        initial = main._process_feed(123, first)
        state.hot_revision = main._hot_revision(initial)
        main._store_processed_feed(state, initial, "hot")

        main._merge_hot_feed(
            123, state, raw_feed("20260730_010001", "pitch-1")
        )

        self.assertEqual(
            ["pitch-1", "pitch-2"],
            [
                pitch["eventId"]
                for pitch in state.data["currentPlay"]["pitches"]
            ],
        )

    def test_richer_pitch_fields_merge_by_identity_when_events_reorder(self):
        cold = [
            {"eventId": "pitch-1", "pitchNumber": 1, "call": "Ball"},
            {"eventId": "pitch-2", "pitchNumber": 2, "call": "Called Strike"},
        ]
        hot = [
            {"eventId": "pitch-2", "pitchNumber": 2, "call": ""},
            {"eventId": "pitch-1", "pitchNumber": 1, "call": ""},
        ]

        merged = main._prefer_hot(hot, cold)

        self.assertEqual(
            [("pitch-2", "Called Strike"), ("pitch-1", "Ball")],
            [(pitch["eventId"], pitch["call"]) for pitch in merged],
        )

    def test_stream_health_detects_a_stalled_upstream_check(self):
        state = main.FeedState(data={"status": {}})
        state.last_success_at = (
            main.time.monotonic()
            - main.HOT_FEED_TIMEOUT_SECONDS
            - main.LIVE_FEED_CHECK_SECONDS
            - 1
        )

        self.assertTrue(main._feed_is_degraded(state))


class RefreshFeedTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_get_client = main.get_client
        self.client = FakeClient()

        async def fake_get_client():
            return self.client

        main.get_client = fake_get_client
        main._feed_states.clear()

    async def asyncTearDown(self):
        states = list(main._feed_states.values())
        for state in states:
            state.subscribers.clear()
            state.pending_enrichment_timestamp = None
        tasks = {
            task
            for state in states
            for task in (state.poller, state.enrichment_task)
            if task is not None and not task.done()
        }
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        main.get_client = self.original_get_client
        main._feed_states.clear()

    async def await_enrichment(self, state):
        first_task = state.enrichment_task
        self.assertIsNotNone(first_task)
        while state.enrichment_task is not None:
            await state.enrichment_task
            await asyncio.sleep(0)
        return first_task

    async def test_large_feed_is_fetched_only_when_timestamp_changes(self):
        first = await main._refresh_feed_if_changed(123, force_full=True)
        first_revision = first.revision
        first_order = first.data["feedOrder"]

        await main._refresh_feed_if_changed(123, min_check_age=0)
        self.assertEqual(1, self.client.full_calls)
        self.assertEqual(1, self.client.hot_calls)

        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-2"
        changed = await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(changed)

        self.assertEqual(2, self.client.full_calls)
        self.assertEqual(2, self.client.hot_calls)
        self.assertNotEqual(first_revision, changed.revision)
        self.assertGreater(changed.data["feedOrder"], first_order)
        self.assertEqual(
            "pitch-2", changed.data["currentPlay"]["pitches"][0]["eventId"]
        )
        self.assertTrue(all("_" in params for _, params in self.client.requests))

    async def test_concurrent_browser_refreshes_share_one_upstream_check(self):
        await main._refresh_feed_if_changed(123, force_full=True)
        state = main._get_feed_state(123)
        state.checked_at = 0
        self.client.timestamp_delay = 0.02

        await asyncio.gather(
            *[
                main._refresh_feed_if_changed(123, min_check_age=0.2)
                for _ in range(12)
            ]
        )

        self.assertEqual(1, self.client.hot_calls)
        self.assertEqual(1, self.client.full_calls)

    async def test_stalled_hot_request_has_a_short_hard_deadline(self):
        original_timeout = main.HOT_FEED_TIMEOUT_SECONDS
        main.HOT_FEED_TIMEOUT_SECONDS = 0.01
        self.client.timestamp_delay = 0.1
        try:
            with self.assertRaises(TimeoutError):
                await main._fetch_hot_feed(123)
        finally:
            main.HOT_FEED_TIMEOUT_SECONDS = original_timeout

    async def test_new_pitch_is_published_before_large_feed_finishes(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        queue = asyncio.Queue(maxsize=1)
        state.subscribers.add(queue)
        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-2"
        self.client.full_delay = 0.1

        refresh = asyncio.create_task(
            main._refresh_feed_if_changed(123, min_check_age=0)
        )
        revision, payload = await asyncio.wait_for(queue.get(), timeout=0.05)
        await refresh
        enrichment = state.enrichment_task

        self.assertTrue(refresh.done())
        self.assertIsNotNone(enrichment)
        self.assertFalse(enrichment.done())
        self.assertIn('"eventId":"pitch-2"', payload)
        self.assertEqual(revision, state.revision)
        await enrichment

    async def test_older_full_snapshot_cannot_overwrite_hot_pitch(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        old_full_timestamp = state.full_timestamp
        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-2"
        self.client.full_timestamp_override = old_full_timestamp
        self.client.full_event_override = "pitch-1"

        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)

        self.assertEqual(
            "pitch-2", state.data["currentPlay"]["pitches"][0]["eventId"]
        )
        self.assertEqual("20260730_010001", state.source_timestamp)
        self.assertEqual(old_full_timestamp, state.full_timestamp)

        self.client.full_timestamp_override = None
        self.client.full_event_override = None
        state.next_full_retry_at = 0
        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)
        self.assertEqual("20260730_010001", state.full_timestamp)

    async def test_older_hot_snapshot_is_ignored(self):
        self.client.timestamp = "20260730_010002"
        self.client.event_id = "pitch-2"
        state = await main._refresh_feed_if_changed(123, force_full=True)

        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-1"
        await main._refresh_feed_if_changed(123, min_check_age=0)

        self.assertEqual("20260730_010002", state.source_timestamp)
        self.assertEqual(
            "pitch-2", state.data["currentPlay"]["pitches"][0]["eventId"]
        )
        self.assertEqual(1, self.client.full_calls)

    async def test_same_timestamp_can_publish_richer_pitch_details(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        first_order = state.data["feedOrder"]
        self.client.pitch.update(
            {
                "start_speed": 97.4,
                "call_code": "S",
                "call_description": "Swinging Strike",
                "pitch_type": "Slider",
                "px": 0.18,
                "pz": 2.61,
            }
        )

        await main._refresh_feed_if_changed(123, min_check_age=0)

        pitch = state.data["currentPlay"]["pitches"][0]
        self.assertEqual("pitch-1", pitch["eventId"])
        self.assertEqual(97.4, pitch["startSpeed"])
        self.assertEqual("S", pitch["callCode"])
        self.assertEqual("Swinging Strike", pitch["call"])
        self.assertEqual("Slider", pitch["type"])
        self.assertEqual(0.18, pitch["px"])
        self.assertEqual(2.61, pitch["pz"])
        self.assertGreater(state.data["feedOrder"], first_order)
        self.assertEqual(1, self.client.full_calls)
        self.assertIsNone(state.enrichment_task)

    async def test_cold_start_returns_hot_data_before_full_feed_finishes(self):
        self.client.full_delay = 0.1

        state = await asyncio.wait_for(
            main._refresh_feed_if_changed(123), timeout=0.05
        )

        self.assertEqual("hot", state.data["feedKind"])
        self.assertEqual(1, self.client.hot_calls)
        self.assertIsNotNone(state.enrichment_task)
        self.assertFalse(state.enrichment_task.done())
        await self.await_enrichment(state)

    async def test_equal_timestamp_full_feed_cannot_move_pitch_back(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-2"
        self.client.pitch = {"pitch_number": 2}
        self.client.full_timestamp_override = "20260730_010001"
        self.client.full_event_override = "pitch-1"
        self.client.full_pitch_override = {"pitch_number": 1}

        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)

        pitch = state.data["currentPlay"]["pitches"][0]
        self.assertEqual("pitch-2", pitch["eventId"])
        self.assertEqual(2, pitch["pitchNumber"])

    async def test_rapid_timestamps_retain_only_latest_queued_enrichment(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        self.client.full_delay = 0.05

        self.client.timestamp = "20260730_010002"
        self.client.event_id = "pitch-2"
        self.client.pitch = {"pitch_number": 2}
        await main._refresh_feed_if_changed(123, min_check_age=0)
        await asyncio.sleep(0)

        for suffix in (3, 4):
            self.client.timestamp = f"20260730_01000{suffix}"
            self.client.event_id = f"pitch-{suffix}"
            self.client.pitch = {"pitch_number": suffix}
            await main._refresh_feed_if_changed(123, min_check_age=0)

        await self.await_enrichment(state)

        self.assertEqual(3, self.client.full_calls)
        self.assertEqual("20260730_010004", state.full_timestamp)
        self.assertEqual(
            "pitch-4", state.data["currentPlay"]["pitches"][0]["eventId"]
        )

    async def test_missing_hot_timestamp_never_triggers_large_download_loop(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        original_source_timestamp = state.source_timestamp
        self.client.timestamp = None
        self.client.event_id = "pitch-without-timestamp"

        await main._refresh_feed_if_changed(123, min_check_age=0)
        await main._refresh_feed_if_changed(123, min_check_age=0)

        self.assertEqual(original_source_timestamp, state.source_timestamp)
        self.assertEqual(1, self.client.full_calls)
        self.assertEqual(
            "pitch-without-timestamp",
            state.data["currentPlay"]["pitches"][0]["eventId"],
        )


if __name__ == "__main__":
    unittest.main()
