import asyncio
import copy
import json
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


def matchup_feed(
    timestamp,
    *,
    at_bat_index,
    play_batter,
    play_pitcher,
    line_batter=None,
    defense_pitcher=None,
    inning=3,
    inning_state="Top",
    inning_half="Top",
    complete=False,
    include_pitch=True,
):
    feed = raw_feed(timestamp, f"pitch-{at_bat_index}")
    play = feed["liveData"]["plays"]["currentPlay"]
    play["atBatIndex"] = at_bat_index
    play["about"].update(
        {
            "inning": inning,
            "halfInning": inning_half.lower(),
            "isComplete": complete,
        }
    )
    play["matchup"] = {
        "batter": {"id": play_batter[0], "fullName": play_batter[1]},
        "pitcher": {"id": play_pitcher[0], "fullName": play_pitcher[1]},
    }
    if not include_pitch:
        play["playEvents"] = []
    feed["liveData"]["linescore"] = {
        "currentInning": inning,
        "inningState": inning_state,
        "inningHalf": inning_half,
        "isTopInning": inning_half == "Top",
        "balls": 0,
        "strikes": 0,
        "outs": 0 if not complete else 3,
        "offense": {
            "team": {"id": 111},
            "batter": {
                "id": (line_batter or play_batter)[0],
                "fullName": (line_batter or play_batter)[1],
            },
        },
        "defense": (
            {
                "team": {"id": 133},
                "pitcher": {
                    "id": (defense_pitcher or play_pitcher)[0],
                    "fullName": (defense_pitcher or play_pitcher)[1],
                },
            }
            if defense_pitcher is not False
            else {}
        ),
    }
    return feed


def strikeout_play(final_code, final_description, *, pitch_count=3):
    events = []
    for pitch_number in range(1, pitch_count + 1):
        code = final_code if pitch_number == pitch_count else "C"
        description = (
            final_description
            if pitch_number == pitch_count
            else "Called Strike"
        )
        events.append(
            {
                "index": pitch_number - 1,
                "playId": f"pitch-{pitch_number}",
                "pitchNumber": pitch_number,
                "isPitch": True,
                "details": {
                    "code": code,
                    "description": description,
                    "call": {"code": code, "description": description},
                },
                "pitchData": {},
                "count": {},
            }
        )
    return {
        "atBatIndex": 7,
        "result": {
            "eventType": "strikeout",
            "description": "Batter strikes out.",
        },
        "about": {"isComplete": True},
        "matchup": {"batter": {}, "pitcher": {}},
        "playEvents": events,
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
        self.full_history = []

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
        if self.full_history:
            response["liveData"]["plays"]["allPlays"] = (
                copy.deepcopy(self.full_history)
                + response["liveData"]["plays"]["allPlays"]
            )
        if self.full_delay:
            await asyncio.sleep(self.full_delay)
        return FakeResponse(response)


class ProcessFeedTests(unittest.TestCase):
    def test_standings_select_the_active_season(self):
        self.assertEqual(2026, main._current_mlb_season(1785542400))
        self.assertEqual(2025, main._current_mlb_season(1768435200))

    def test_compact_multiwatch_payload_keeps_current_pitch_without_boxscore(self):
        current = main._process_play(
            raw_feed()["liveData"]["plays"]["currentPlay"]
        )
        completed = copy.deepcopy(current)
        completed["about"]["isComplete"] = True
        completed["result"] = "Singled to right"
        payload = json.dumps({
            "gamePk": 123,
            "status": {"abstractGameState": "Live"},
            "currentPlay": current,
            "linescore": {"inning": 1},
            "plays": [completed],
            "boxscore": {"large": "unused"},
            "linescoreInnings": [{"num": 1}],
            "feedVersion": "v1",
            "feedOrder": 10,
        })

        compact = json.loads(main._compact_multiwatch_payload(payload))

        self.assertNotIn("boxscore", compact)
        self.assertNotIn("linescoreInnings", compact)
        self.assertEqual("pitch-1", compact["currentPlay"]["pitches"][0]["eventId"])
        self.assertNotIn("pitches", compact["plays"][0])
        self.assertEqual("Singled to right", compact["plays"][0]["result"])

    def test_forceout_description_is_short_and_position_specific(self):
        short = main._short_play_result(
            {
                "eventType": "force_out",
                "description": (
                    "Jordan Walker grounds into a force out, fielded by third "
                    "baseman Alex Bregman. Nathan Church scores. Lars Nootbaar "
                    "out at 3rd."
                ),
            }
        )

        self.assertEqual("Grounded into forceout to third", short)

    def test_strikeout_label_uses_final_pitch_call_and_actual_pitch_count(self):
        cases = (
            ("S", "Swinging Strike", 3, "Three-pitch strikeout swinging"),
            ("C", "Called Strike", 3, "Three-pitch strikeout looking"),
            ("W", "Swinging Strike (Blocked)", 4, "Strikeout swinging"),
            ("C", "Called Strike", 5, "Strikeout looking"),
        )
        for code, description, pitch_count, expected in cases:
            with self.subTest(expected=expected):
                play = strikeout_play(
                    code, description, pitch_count=pitch_count
                )
                play["playEvents"].insert(
                    1,
                    {
                        "index": 20,
                        "isPitch": False,
                        "details": {
                            "eventType": "mound_visit",
                            "description": "Mound Visit.",
                        },
                    },
                )

                self.assertEqual(
                    expected, main._process_play(play)["shortResult"]
                )

    def test_strikeout_label_stays_generic_without_terminal_pitch_evidence(self):
        ambiguous = strikeout_play("", "", pitch_count=3)
        timed_out = strikeout_play("C", "Called Strike", pitch_count=3)
        timed_out["playEvents"].append(
            {
                "index": 3,
                "isPitch": False,
                "details": {
                    "eventType": "batter_timeout",
                    "description": "Batter Timeout.",
                },
            }
        )

        self.assertEqual("Struck out", main._process_play(ambiguous)["shortResult"])
        self.assertEqual("Struck out", main._process_play(timed_out)["shortResult"])

    def test_timer_violation_is_a_stable_alert_not_a_temporary_play_result(self):
        for event_type, description, expected_title, expected_type in (
            (
                "batter_timeout",
                "Batter Timeout.",
                "Batter Timeout",
                "batter-timeout",
            ),
            (
                "pitch_timer_violation",
                "Pitch Timer Violation.",
                "Pitch Timer Violation",
                "pitch-timer",
            ),
        ):
            with self.subTest(event_type=event_type):
                feed = matchup_feed(
                    "20260730_022900",
                    at_bat_index=18,
                    play_batter=(663330, "Jahmai Jones"),
                    play_pitcher=(682052, "Jacob Lopez"),
                    include_pitch=False,
                )
                play = feed["liveData"]["plays"]["currentPlay"]
                play["result"] = {
                    "type": "atBat",
                    "eventType": event_type,
                    "description": description,
                }
                play["playEvents"] = [
                    {
                        "index": 4,
                        "isPitch": False,
                        "details": {
                            "eventType": event_type,
                            "description": description,
                        },
                    }
                ]

                first = main._process_feed(824973, feed)
                second = main._process_feed(824973, copy.deepcopy(feed))
                alert = first["gameEvents"][0]
                hot_feed = copy.deepcopy(feed)
                hot_feed["liveData"]["plays"]["allPlays"] = []
                hot = main._process_feed(824973, hot_feed)

                self.assertEqual("", first["plays"][-1]["result"])
                self.assertEqual("", first["plays"][-1]["shortResult"])
                self.assertEqual("atBat", first["plays"][-1]["resultType"])
                self.assertEqual(expected_title, alert["title"])
                self.assertEqual(expected_type, alert["type"])
                self.assertEqual("ab:18:event:4", alert["eventId"])
                self.assertEqual(alert["eventId"], alert["key"])
                self.assertEqual(
                    alert["eventId"], second["gameEvents"][0]["eventId"]
                )
                self.assertEqual([], hot["gameEvents"])
                self.assertEqual(alert, hot["currentAlerts"][0])

    def test_hot_current_alert_is_merged_without_all_plays(self):
        initial_feed = matchup_feed(
            "20260730_022900",
            at_bat_index=18,
            play_batter=(663330, "Jahmai Jones"),
            play_pitcher=(682052, "Jacob Lopez"),
            include_pitch=False,
        )
        state = main.FeedState()
        initial = main._process_feed(824973, initial_feed)
        state.hot_revision = main._hot_revision(initial)
        main._store_processed_feed(state, initial, "full")

        hot_feed = copy.deepcopy(initial_feed)
        hot_feed["metaData"]["timeStamp"] = "20260730_022901"
        hot_feed["liveData"]["plays"]["allPlays"] = []
        hot_feed["liveData"]["plays"]["currentPlay"]["playEvents"] = [
            {
                "index": 4,
                "isPitch": False,
                "details": {
                    "eventType": "batter_timeout",
                    "description": "Batter Timeout.",
                },
            }
        ]

        changed = main._merge_hot_feed(824973, state, hot_feed)

        self.assertTrue(changed)
        self.assertEqual("Batter Timeout", state.data["currentAlerts"][0]["title"])
        self.assertEqual([], state.data["gameEvents"])

        next_feed = matchup_feed(
            "20260730_022902",
            at_bat_index=19,
            play_batter=(665161, "Next Batter"),
            play_pitcher=(682052, "Jacob Lopez"),
            include_pitch=False,
        )
        next_feed["liveData"]["plays"]["allPlays"] = []
        main._merge_hot_feed(824973, state, next_feed)

        self.assertEqual([], state.data["currentAlerts"])

    def test_completed_plate_appearance_keeps_result_after_timeout_alert(self):
        feed = matchup_feed(
            "20260730_022901",
            at_bat_index=18,
            play_batter=(663330, "Jahmai Jones"),
            play_pitcher=(682052, "Jacob Lopez"),
            complete=True,
            include_pitch=False,
        )
        play = feed["liveData"]["plays"]["currentPlay"]
        play["result"] = {
            "type": "atBat",
            "eventType": "single",
            "description": "Jahmai Jones singles on a line drive.",
        }
        play["playEvents"] = [
            {
                "index": 4,
                "isPitch": False,
                "details": {
                    "eventType": "batter_timeout",
                    "description": "Batter Timeout.",
                },
            }
        ]

        result = main._process_feed(824973, feed)

        self.assertEqual(
            "Jahmai Jones singles on a line drive.",
            result["plays"][-1]["result"],
        )
        self.assertEqual("Singled", result["plays"][-1]["shortResult"])
        self.assertEqual("atBat", result["plays"][-1]["resultType"])
        self.assertEqual("Batter Timeout", result["gameEvents"][0]["title"])

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
        state = main.FeedState(
            data={"status": {"abstractGameState": "Live"}}
        )
        state.last_success_at = (
            main.time.monotonic()
            - main.HOT_FEED_TIMEOUT_SECONDS
            - main.LIVE_FEED_CHECK_SECONDS
            - 1
        )

        self.assertTrue(main._feed_is_degraded(state))

    def test_stream_health_uses_the_status_specific_poll_cadence(self):
        state = main.FeedState(
            data={"status": {"abstractGameState": "Final"}},
            last_success_at=main.time.monotonic() - 5,
        )

        self.assertFalse(main._feed_is_degraded(state))

    def test_live_errors_do_not_slow_the_low_latency_retry_cadence(self):
        state = main.FeedState(
            data={"status": {"abstractGameState": "Live"}},
            error_count=4,
        )

        self.assertEqual(
            main.LIVE_FEED_CHECK_SECONDS, main._feed_poll_interval(state)
        )

    def test_pitcher_boxscore_includes_authoritative_full_game_pitch_count(self):
        feed = matchup_feed(
            "20260730_022853",
            at_bat_index=18,
            play_batter=(663330, "Jahmai Jones"),
            play_pitcher=(682052, "Jacob Lopez"),
        )
        feed["liveData"]["boxscore"]["teams"] = {
            "away": {},
            "home": {
                "players": {
                    "ID682052": {
                        "person": {"id": 682052, "fullName": "Jacob Lopez"},
                        "position": {"abbreviation": "P"},
                        "stats": {
                            "pitching": {
                                "inningsPitched": "5.2",
                                "numberOfPitches": 87,
                                "pitchesThrown": 999,
                                "strikes": 56,
                                "strikesThrown": 1,
                            }
                        },
                    }
                }
            },
        }

        result = main._process_feed(824973, feed)

        pitcher = result["boxscore"]["home"]["pitchers"][0]
        self.assertEqual(682052, pitcher["id"])
        self.assertEqual(87, pitcher["pitches"])
        self.assertEqual("64%", pitcher["strk"])

    def test_end_inning_uses_upcoming_batter_and_defensive_pitcher_together(self):
        feed = matchup_feed(
            "20260730_022107",
            at_bat_index=19,
            play_batter=(545361, "Old Batter"),
            play_pitcher=(669713, "Old Pitcher"),
            line_batter=(665161, "Jeremy Pena"),
            defense_pitcher=(680570, "Grayson Rodriguez"),
            inning=2,
            inning_state="End",
            inning_half="Bottom",
            complete=True,
        )

        result = main._process_feed(824002, feed)

        self.assertEqual(665161, result["currentBatter"]["id"])
        self.assertEqual(680570, result["currentPitcher"]["id"])
        self.assertEqual(
            680570, result["linescore"]["defense"]["pitcher"]["id"]
        )
        self.assertFalse(result["currentPlayActive"])

    def test_stale_completed_play_never_supplies_transition_pitcher(self):
        feed = matchup_feed(
            "20260730_022108",
            at_bat_index=19,
            play_batter=(545361, "Old Batter"),
            play_pitcher=(669713, "Old Pitcher"),
            line_batter=(665161, "Jeremy Pena"),
            defense_pitcher=False,
            inning=2,
            inning_state="End",
            inning_half="Bottom",
            complete=True,
        )

        result = main._process_feed(824002, feed)

        self.assertEqual(665161, result["currentBatter"]["id"])
        self.assertEqual({}, result["currentPitcher"])
        self.assertFalse(result["currentPlayActive"])

    def test_new_zero_pitch_at_bat_replaces_old_zone_atomically(self):
        old_feed = matchup_feed(
            "20260730_022055",
            at_bat_index=19,
            play_batter=(545361, "Old Batter"),
            play_pitcher=(669713, "Old Pitcher"),
            inning=2,
            inning_state="Bottom",
            inning_half="Bottom",
        )
        next_feed = matchup_feed(
            "20260730_022302",
            at_bat_index=20,
            play_batter=(665161, "Jeremy Pena"),
            play_pitcher=(680570, "Grayson Rodriguez"),
            inning=3,
            inning_state="Top",
            inning_half="Top",
            include_pitch=False,
        )
        state = main.FeedState()
        initial = main._process_feed(824002, old_feed)
        state.hot_revision = main._hot_revision(initial)
        main._store_processed_feed(state, initial, "hot")

        main._merge_hot_feed(824002, state, next_feed)

        self.assertEqual(20, state.data["currentPlay"]["atBatIndex"])
        self.assertEqual([], state.data["currentPlay"]["pitches"])
        self.assertEqual([], state.data["plays"][-1]["pitches"])
        self.assertEqual(665161, state.data["currentBatter"]["id"])
        self.assertEqual(680570, state.data["currentPitcher"]["id"])
        self.assertTrue(state.data["currentPlayActive"])

    def test_hot_projection_requests_linescore_defense(self):
        self.assertIn("defense", main.HOT_FEED_FIELDS.split(","))


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

    @staticmethod
    def make_full_refresh_due(state):
        state.last_full_success_at -= main.FULL_FEED_MAX_AGE_SECONDS + 1

    async def test_large_feed_is_fetched_only_when_timestamp_changes(self):
        first = await main._refresh_feed_if_changed(123, force_full=True)
        first_revision = first.revision
        first_order = first.data["feedOrder"]

        await main._refresh_feed_if_changed(123, min_check_age=0)
        self.assertEqual(1, self.client.full_calls)
        self.assertEqual(1, self.client.hot_calls)

        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-2"
        self.make_full_refresh_due(first)
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

    async def test_rest_fallback_reuses_active_stream_poller_state(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        state.subscribers.add(asyncio.Queue(maxsize=1))
        state.poller = asyncio.create_task(asyncio.Event().wait())
        calls_before = (self.client.hot_calls, self.client.full_calls)

        result = await main.get_game_feed(123, main.Response())

        self.assertIs(result, state.data)
        self.assertEqual(calls_before, (self.client.hot_calls, self.client.full_calls))

    async def test_metadata_only_timestamp_defers_large_refresh_until_due(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        self.client.timestamp = "20260730_010001"

        await main._refresh_feed_if_changed(123, min_check_age=0)

        self.assertEqual(1, self.client.full_calls)
        self.assertIsNone(state.enrichment_task)

        state.last_full_success_at -= main.FULL_FEED_MAX_AGE_SECONDS + 1
        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)

        self.assertEqual(2, self.client.full_calls)
        self.assertEqual("20260730_010001", state.full_timestamp)
        self.assertEqual(0, state.full_error_count)

    async def test_hot_pitch_does_not_bypass_full_refresh_cadence(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        self.client.timestamp = "20260730_010001"
        self.client.event_id = "pitch-2"
        self.client.pitch = {"pitch_number": 2}

        await main._refresh_feed_if_changed(123, min_check_age=0)

        self.assertEqual(
            "pitch-2", state.data["currentPlay"]["pitches"][0]["eventId"]
        )
        self.assertEqual(1, self.client.full_calls)
        self.assertIsNone(state.enrichment_task)

        self.make_full_refresh_due(state)
        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)

        self.assertEqual(2, self.client.full_calls)
        self.assertEqual("20260730_010001", state.full_timestamp)

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
        self.make_full_refresh_due(state)

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
        self.make_full_refresh_due(state)

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
        self.make_full_refresh_due(state)

        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)

        pitch = state.data["currentPlay"]["pitches"][0]
        self.assertEqual("pitch-2", pitch["eventId"])
        self.assertEqual(2, pitch["pitchNumber"])

    async def test_older_full_feed_adds_history_without_moving_pitch_back(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        completed = raw_feed()["liveData"]["plays"]["currentPlay"]
        completed["atBatIndex"] = -1
        completed["about"]["isComplete"] = True
        completed["result"] = {
            "eventType": "single",
            "description": "Batter singles.",
        }
        self.client.full_history = [completed]
        self.client.timestamp = "20260730_010002"
        self.client.event_id = "pitch-2"
        self.client.pitch = {"pitch_number": 2}
        self.client.full_timestamp_override = "20260730_010001"
        self.client.full_event_override = "pitch-1"
        self.client.full_pitch_override = {"pitch_number": 1}
        self.make_full_refresh_due(state)

        await main._refresh_feed_if_changed(123, min_check_age=0)
        await self.await_enrichment(state)

        self.assertEqual("pitch-2", state.data["currentPlay"]["pitches"][0]["eventId"])
        self.assertEqual(2, len(state.data["plays"]))
        self.assertTrue(state.data["plays"][0]["about"]["isComplete"])
        self.assertEqual("full", state.data["feedKind"])

    async def test_rapid_timestamps_retain_only_latest_queued_enrichment(self):
        state = await main._refresh_feed_if_changed(123, force_full=True)
        self.client.full_delay = 0.05
        self.make_full_refresh_due(state)

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
