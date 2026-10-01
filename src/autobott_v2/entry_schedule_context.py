"""Read-only exchange sessions and selected official BLS release schedules.

Calendar observation is not comprehensive event clearance or a forecast.
Only entry eligibility uses this module; exits and broker orders are untouched.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, date, datetime, time, timedelta
import hashlib
import json
import re
import urllib.request
from zoneinfo import ZoneInfo

from .bar_timing import aware_utc

NY = ZoneInfo("America/New_York")
BLS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
BLS_TITLES = ("Consumer Price Index", "Producer Price Index", "Employment Situation",
              "Job Openings and Labor Turnover Survey", "Employment Cost Index", "Productivity and Costs")
POLICY = {"id": "exchange_session_and_listed_bls.v1", "before_seconds": 600,
          "after_seconds": 300, "max_source_age_seconds": 900,
          "bls_titles": list(BLS_TITLES)}
FOMC_POLICY = {**POLICY, "id": "exchange_session_listed_bls_fomc.v2",
               "fomc_titles": ["FOMC Press Conference", "FOMC Meeting", "FOMC Minutes"]}
MAX_BYTES = 1_000_000


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(",", ":")).encode()).hexdigest()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("schedule_source_redirect_not_allowed")


def fetch_bls_text() -> str:
    # No broker credential or user-selected URL reaches this public request.
    request = urllib.request.Request(BLS_URL, headers={"User-Agent": "AutoBott calendar reader",
                                                     "Accept": "text/calendar"})
    with urllib.request.build_opener(_NoRedirect).open(request, timeout=5) as response:
        raw = response.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("schedule_source_size_limit")
    return raw.decode("utf-8-sig")


def parse_bls_calendar(text: str) -> list[dict]:
    if not isinstance(text, str) or len(text.encode()) > MAX_BYTES:
        raise ValueError("schedule_source_size_limit")
    lines = re.sub(r"\r?\n[ \t]", "", text).splitlines()
    if not lines or lines[0] != "BEGIN:VCALENDAR" or lines[-1].strip() != "END:VCALENDAR":
        raise ValueError("schedule_calendar_envelope_invalid")
    events, current = {}, None
    for line in lines:
        if line == "BEGIN:VEVENT":
            if current is not None:
                raise ValueError("schedule_nested_event")
            current = {}
        elif line == "END:VEVENT":
            if current is None:
                raise ValueError("schedule_event_envelope_invalid")
            uid, title = current.get("UID"), current.get("SUMMARY")
            start = current.get("DTSTART")
            if not uid or not title or not start or len(uid[1]) > 200 or len(title[1]) > 500:
                raise ValueError("schedule_event_identity_required")
            if "RRULE" in current or "RDATE" in current or "EXDATE" in current or "RECURRENCE-ID" in current:
                raise ValueError("schedule_recurring_event_unsupported")
            params, value = start
            if not params and re.fullmatch(r"\d{8}T\d{6}Z", value):
                when = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            elif params in {"TZID=US-Eastern", "TZID=US/Eastern", "TZID=America/New_York"} and re.fullmatch(r"\d{8}T\d{6}", value):
                local = datetime.strptime(value, "%Y%m%dT%H%M%S")
                when = local.replace(tzinfo=NY)
                if when.replace(fold=0).utcoffset() != when.replace(fold=1).utcoffset():
                    raise ValueError("schedule_ambiguous_local_time")
                if when.astimezone(UTC).astimezone(NY).replace(tzinfo=None) != local:
                    raise ValueError("schedule_nonexistent_local_time")
            else:
                raise ValueError("schedule_explicit_event_timezone_required")
            status = current.get("STATUS", ("", "CONFIRMED"))[1]
            if status not in {"CONFIRMED", "TENTATIVE", "CANCELLED"}:
                raise ValueError("schedule_unknown_event_status")
            event = {"id": uid[1], "title": title[1], "at": when.astimezone(UTC).isoformat(), "status": status}
            if event["id"] in events and events[event["id"]] != event:
                raise ValueError("schedule_conflicting_event_identity")
            events[event["id"]] = event
            if len(events) > 2000:
                raise ValueError("schedule_event_count_limit")
            current = None
        elif current is not None:
            if line.startswith("BEGIN:") or ":" not in line:
                raise ValueError("schedule_event_line_invalid")
            key, value = line.split(":", 1)
            field, _, params = key.partition(";")
            if field in current:
                raise ValueError("schedule_duplicate_event_field")
            current[field] = (params, value)
    if current is not None or not events:
        raise ValueError("schedule_incomplete_or_empty_calendar")
    return sorted(events.values(), key=lambda r: (r["at"], r["id"]))


def market_session(rows, day: date) -> dict:
    if not isinstance(rows, list) or len(rows) > 1:
        raise ValueError("schedule_session_response_invalid")
    if not rows:
        return {"date": day.isoformat(), "trading_day": False, "open": None, "close": None}
    row = rows[0]
    if not isinstance(row, Mapping) or row.get("date") != day.isoformat():
        raise ValueError("schedule_session_date_mismatch")
    opening, closing = time.fromisoformat(row["open"]), time.fromisoformat(row["close"])
    if opening.tzinfo is not None or closing.tzinfo is not None or not opening < closing:
        raise ValueError("schedule_session_hours_invalid")
    return {"date": day.isoformat(), "trading_day": True,
            "open": datetime.combine(day, opening, NY).astimezone(UTC).isoformat(),
            "close": datetime.combine(day, closing, NY).astimezone(UTC).isoformat()}


class EntryScheduleSource:
    """Cache each exact day's observations for at most 15 minutes, never across accounts/clients."""
    def __init__(self, calendar_fetch: Callable, *, public_fetch: Callable | None = None,
                 fomc_fetch: Callable | None = None, now_fn: Callable | None = None):
        from .entry_fomc_context import fetch_fomc_text
        self._calendar_fetch = calendar_fetch
        self._public_fetch = public_fetch or fetch_bls_text
        self._fomc_fetch = fomc_fetch or fetch_fomc_text
        self._now = now_fn or (lambda: datetime.now(UTC))
        self._cache = None

    def collect(self, cutoff) -> dict:
        day = aware_utc(cutoff).astimezone(NY).date()
        before = aware_utc(self._now())
        if self._cache is not None and self._cache["date"] == day.isoformat():
            age = (before - aware_utc(self._cache["received_at"])).total_seconds()
            if 0 <= age <= POLICY["max_source_age_seconds"]:
                return deepcopy(self._cache)
        result = {"schema_version": "entry_schedule.v2", "policy": deepcopy(FOMC_POLICY),
                  "date": day.isoformat(), "source_authenticity_verified": False,
                  "calendar_source_url": "https://paper-api.alpaca.markets/v2/calendar",
                  "earnings_status": "not_integrated", "fomc_status": "unavailable",
                  "all_market_event_coverage_verified": False}
        try:
            session = market_session(self._calendar_fetch({"start": day.isoformat(), "end": day.isoformat()}), day)
            events = parse_bls_calendar(self._public_fetch())
            first, last = (aware_utc(events[i]["at"]).astimezone(NY).date() for i in (0, -1))
            if not first <= day <= last:
                raise ValueError("schedule_date_outside_observed_bls_span")
            selected = [e for e in events if e["title"] in BLS_TITLES and aware_utc(e["at"]).astimezone(NY).date() == day]
            from .entry_fomc_context import parse_fomc_calendar
            fomc = parse_fomc_calendar(self._fomc_fetch(day), day)
            after = aware_utc(self._now())
            if after < before:
                raise ValueError("schedule_collection_clock_regressed")
            result.update(status="observed", session=session, bls={"source_url": BLS_URL,
                "scope": "listed_selected_bls_releases_only", "events": selected,
                "observed_span": [first.isoformat(), last.isoformat()], "dataset_hash": digest(events),
                "dataset_event_count": len(events), "historical_asof_verified": False},
                received_at=after.isoformat())
            result.update(fomc=fomc, fomc_status="listed_month_observed")
            result["payload_hash"] = digest(result)
            self._cache = deepcopy(result)
            return result
        except Exception as exc:
            # A missing release calendar never becomes an empty valid list.
            return {**result, "status": "unavailable", "reason": str(exc) if isinstance(exc, ValueError)
                    and str(exc).startswith("schedule_") else type(exc).__name__}


def attach_entry_schedule(context: dict, source: EntryScheduleSource, cutoff) -> dict:
    if context.get("status") != "observed":
        return context
    return {**context, "schema_version": "entry_context.v2", "schedule": source.collect(cutoff),
            "scheduled_event_calendar_status": "selected_bls_fomc_and_exchange_session_only"}


def assess_entry_schedule(schedule, *, checked_at, snapshot_received_at, bar_times: list) -> dict:
    if schedule is None:
        return {"status": "not_recorded", "reason": "legacy_context_without_schedule"}
    if not isinstance(schedule, Mapping):
        raise ValueError("schedule_schema_or_policy_invalid")
    schema = schedule.get("schema_version")
    policy = FOMC_POLICY if schema == "entry_schedule.v2" else POLICY
    if schema not in {"entry_schedule.v1", "entry_schedule.v2"} or schedule.get("policy") != policy:
        raise ValueError("schedule_schema_or_policy_invalid")
    if schedule.get("status") != "observed":
        return {"status": "unavailable", "reason": "schedule_source_unavailable"}
    if schedule.get("payload_hash") != digest({k: v for k, v in schedule.items() if k != "payload_hash"}):
        raise ValueError("schedule_payload_integrity_mismatch")
    now, receipt, known = aware_utc(checked_at), aware_utc(snapshot_received_at), aware_utc(schedule["received_at"])
    if not known <= receipt <= now or not 0 <= (now - known).total_seconds() <= POLICY["max_source_age_seconds"]:
        return {"status": "unavailable", "reason": "schedule_stale_or_not_known_at_decision"}
    day = now.astimezone(NY).date().isoformat()
    session, bls = schedule["session"], schedule["bls"]
    if schedule.get("calendar_source_url") != "https://paper-api.alpaca.markets/v2/calendar":
        raise ValueError("schedule_exchange_source_mismatch")
    if schedule["date"] != day or session["date"] != day:
        raise ValueError("schedule_decision_date_mismatch")
    common = {"policy_id": policy["id"], "schedule_hash": schedule["payload_hash"],
              "scope": "listed_selected_bls_releases_only", "earnings_status": "not_integrated",
              "fomc_status": "not_assessed" if schema == "entry_schedule.v2" else "not_integrated",
              "all_market_event_coverage_verified": False}
    if type(session.get("trading_day")) is not bool:
        raise ValueError("schedule_session_flag_invalid")
    if not session["trading_day"]:
        return {**common, "status": "wait", "reason": "exchange_session_closed"}
    opening, closing = aware_utc(session["open"]), aware_utc(session["close"])
    if (not opening < closing or opening.astimezone(NY).date().isoformat() != day
            or closing.astimezone(NY).date().isoformat() != day):
        raise ValueError("schedule_session_hours_invalid")
    if not opening <= now < closing:
        return {**common, "status": "wait", "reason": "exchange_session_closed"}
    if not bar_times or any(not opening <= aware_utc(t) or aware_utc(t) + timedelta(minutes=1) > closing for t in bar_times):
        return {**common, "status": "unavailable", "reason": "trigger_bars_outside_exchange_session"}
    if (bls.get("source_url") != BLS_URL or bls.get("scope") != common["scope"]
            or not isinstance(bls.get("events"), list) or len(bls["events"]) > 100
            or not bls["observed_span"][0] <= day <= bls["observed_span"][1]):
        raise ValueError("schedule_bls_coverage_invalid")
    ids, active = set(), []
    for event in bls["events"]:
        if event["id"] in ids or event["title"] not in BLS_TITLES or event["status"] not in {"CONFIRMED", "TENTATIVE", "CANCELLED"}:
            raise ValueError("schedule_bls_event_invalid")
        ids.add(event["id"])
        at = aware_utc(event["at"])
        if at.astimezone(NY).date().isoformat() != day:
            raise ValueError("schedule_bls_event_date_mismatch")
        if event["status"] == "CANCELLED":
            continue
        start, end = at - timedelta(seconds=POLICY["before_seconds"]), at + timedelta(seconds=POLICY["after_seconds"])
        if start <= now <= end or (now > end and aware_utc(bar_times[-1]) < end):
            active.append(event)
    if active:
        return {**common, "status": "wait", "reason": "scheduled_bls_release_window", "events": active}
    if schema == "entry_schedule.v2":
        from .entry_fomc_context import fomc_url, SCOPE, TITLES
        fomc = schedule.get("fomc")
        if (not isinstance(fomc, Mapping) or fomc.get("source_url") != fomc_url(now.astimezone(NY).date())
                or fomc.get("scope") != SCOPE or fomc.get("month") != day[:7]
                or not isinstance(fomc.get("events"), list) or len(fomc["events"]) > 100
                or fomc.get("dataset_hash") != digest(fomc["events"])):
            raise ValueError("schedule_fomc_coverage_invalid")
        common.update(fomc_status="listed_month_observed", scope="listed_selected_bls_and_fomc_events_only")
        ids, active = set(), []
        for event in fomc["events"]:
            at = aware_utc(event["at"])
            if (event["id"] in ids or event["title"] not in TITLES or event["status"] != "CONFIRMED"
                    or at.astimezone(NY).strftime("%Y-%m") != day[:7]):
                raise ValueError("schedule_fomc_event_invalid")
            ids.add(event["id"])
            if at.astimezone(NY).date().isoformat() != day:
                continue
            start = at - timedelta(seconds=policy["before_seconds"])
            end = at + timedelta(seconds=policy["after_seconds"])
            if start <= now <= end or (now > end and aware_utc(bar_times[-1]) < end):
                active.append(event)
        if active:
            return {**common, "status": "wait", "reason": "scheduled_fomc_release_window", "events": active}
        return {**common, "status": "observed_no_listed_event_block", "reason": "no_listed_bls_or_fomc_event_in_window"}
    return {**common, "status": "observed_no_listed_event_block", "reason": "no_listed_bls_event_in_window"}
