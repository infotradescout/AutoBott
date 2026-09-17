"""Synthetic scheduled-release and exchange-session contracts; no live orders."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import pytest
from autobott_v2.entry_schedule_context import (
    EntryScheduleSource, parse_bls_calendar, market_session, attach_entry_schedule,
    assess_entry_schedule, digest, POLICY, BLS_URL,
)
from test_entry_market_context import AT, Provider, minute_rows, snapshot, assess


def event(uid="target", at="20260701T120000", title="Consumer Price Index", params="TZID=US-Eastern", extra=""):
    return ("BEGIN:VEVENT\nUID:"+uid+"\nDTSTART"+(";"+params if params else "")+":"+at+
            "\nSUMMARY:"+title+"\n"+extra+"END:VEVENT\n")


def calendar(*events):
    return ("BEGIN:VCALENDAR\nVERSION:2.0\n"+
            event("prior", "20260101T083000")+"".join(events)+
            event("future", "20261231T083000")+"END:VCALENDAR")


def source(at=AT, text=None, rows=None, calls=None):
    def read(params):
        if calls is not None: calls.append(("calendar",params))
        return rows if rows is not None else [{"date":at.date().isoformat(),"open":"09:30","close":"16:00"}]
    def public():
        if calls is not None: calls.append(("bls",None))
        return calendar() if text is None else text
    return EntryScheduleSource(read, public_fetch=public, now_fn=lambda:at)


def scheduled_snapshot(*, at=AT, text=None, rows=None):
    result=snapshot(Provider(rows=minute_rows(at=at)),at=at)
    result["entry_context"]=attach_entry_schedule(result["entry_context"],source(at,text,rows),at)
    return result


def test_captured_schedule_has_limited_scope_not_all_clear():
    row=scheduled_snapshot(); before=deepcopy(row)
    evidence=assess(row)["schedule_evidence"]
    assert evidence["status"]=="observed_no_listed_event_block"
    assert evidence["all_market_event_coverage_verified"] is False
    assert evidence["earnings_status"]==evidence["fomc_status"]=="not_integrated"
    assert row==before


@pytest.mark.parametrize("offset,blocked",[(601,False),(600,True),(0,True),(-300,True),(-361,False)])
def test_scheduled_event_window_is_fixed_and_inclusive(offset,blocked):
    target=AT+timedelta(seconds=offset)
    row=scheduled_snapshot(text=calendar(event(at=target.strftime("%Y%m%dT%H%M%SZ"),params="")))
    result=assess(row)
    assert (result["status"]=="wait") is blocked,result
    if blocked: assert result["reason"]=="scheduled_bls_release_window"


def test_post_event_trigger_cannot_be_from_before_window_end():
    target=AT-timedelta(seconds=301)
    row=scheduled_snapshot(text=calendar(event(at=target.strftime("%Y%m%dT%H%M%SZ"),params="")))
    assert assess(row)["reason"]=="scheduled_bls_release_window"


def test_refresh_crossing_into_event_window_blocks_previously_clear_signal():
    target=AT+timedelta(seconds=610)
    row=scheduled_snapshot(text=calendar(event(at=target.strftime("%Y%m%dT%H%M%SZ"),params="")))
    assert assess(row)["status"]=="confirmed"
    assert assess(row,at=AT+timedelta(seconds=10))["reason"]=="scheduled_bls_release_window"


def test_cancelled_event_does_not_create_a_release_block():
    row=scheduled_snapshot(text=calendar(event(at=AT.strftime("%Y%m%dT%H%M%SZ"),params="",extra="STATUS:CANCELLED\n")))
    assert assess(row)["status"]=="confirmed"
    assert row["entry_context"]["schedule"]["bls"]["events"][0]["status"]=="CANCELLED"


def test_early_close_uses_observed_exchange_hours():
    at=datetime(2026,11,27,18,0,tzinfo=UTC)
    row=scheduled_snapshot(at=at,rows=[{"date":"2026-11-27","open":"09:30","close":"13:00"}])
    assert assess(row,at=at)["reason"]=="exchange_session_closed"


def test_no_market_day_cannot_authorize_an_entry():
    row=scheduled_snapshot(rows=[])
    assert assess(row)["reason"]=="exchange_session_closed"


def test_source_observed_after_decision_is_not_historical_evidence():
    row=scheduled_snapshot(); packet=row["entry_context"]["schedule"]
    packet["received_at"]=(AT+timedelta(seconds=1)).isoformat()
    packet["payload_hash"]=digest({k:v for k,v in packet.items() if k!="payload_hash"})
    assert assess(row)["reason"]=="schedule_stale_or_not_known_at_decision"


def test_schedule_expiration_never_becomes_a_clear_window():
    row=scheduled_snapshot()
    assert assess(row,at=AT+timedelta(seconds=901))["reason"]=="schedule_stale_or_not_known_at_decision"


def test_missing_native_schedule_cannot_downgrade_to_legacy():
    row=scheduled_snapshot();del row["entry_context"]["schedule"]
    with pytest.raises(ValueError,match="native_schedule_required"):assess(row)


def test_unavailable_schedule_blocks_without_inventing_zero_events():
    row=scheduled_snapshot(text="provider-error")
    assert assess(row)["reason"]=="schedule_source_unavailable"
    assert "bls" not in row["entry_context"]["schedule"]


def test_changed_schedule_payload_fails_integrity():
    row=scheduled_snapshot();row["entry_context"]["schedule"]["session"]["close"]="2026-07-01T22:00:00Z"
    with pytest.raises(ValueError,match="integrity"):assess(row)


def test_same_day_cache_is_bounded_and_copy_isolated():
    calls=[];reader=source(calls=calls)
    first=reader.collect(AT);again=reader.collect(AT)
    assert len(calls)==2 and first==again
    again["session"]["trading_day"]=False
    assert reader.collect(AT)["session"]["trading_day"] is True
    reader._now=lambda:AT+timedelta(seconds=901)
    reader.collect(AT)
    assert len(calls)==4


def test_regressed_clock_does_not_reuse_future_cache():
    calls=[];reader=source(calls=calls);reader.collect(AT)
    reader._now=lambda:AT-timedelta(seconds=1)
    reader.collect(AT)
    assert len(calls)==4


def test_provider_error_does_not_send_credentials_or_disclose_error_details():
    def fail(_):raise PermissionError("secret transport detail")
    reader=EntryScheduleSource(fail,public_fetch=lambda:pytest.fail("must stop before public request"),now_fn=lambda:AT)
    packet=reader.collect(AT)
    assert packet["status"]=="unavailable" and packet["reason"]=="PermissionError"


@pytest.mark.parametrize("params",["", "VALUE=DATE", "TZID=Europe/London", "TZID=Etc/GMT+4"])
def test_floating_or_unrecognized_event_times_are_not_guessed(params):
    with pytest.raises(ValueError):parse_bls_calendar(calendar(event(params=params)))


@pytest.mark.parametrize("stamp",["20260308T023000","20261101T013000"])
def test_ambiguous_dst_times_are_rejected(stamp):
    with pytest.raises(ValueError):parse_bls_calendar(calendar(event(at=stamp)))


@pytest.mark.parametrize("stamp,utc",[("20260701T083000","2026-07-01T12:30:00+00:00"),("20260102T083000","2026-01-02T13:30:00+00:00")])
def test_eastern_time_uses_date_specific_offset(stamp,utc):
    target=next(e for e in parse_bls_calendar(calendar(event(at=stamp))) if e["id"]=="target")
    assert target["at"]==utc


def test_recurring_event_not_expanded_with_an_invented_calendar():
    with pytest.raises(ValueError,match="recurring"):parse_bls_calendar(calendar(event(extra="RRULE:FREQ=DAILY\n")))


def test_conflicting_uid_is_not_silently_overwritten():
    with pytest.raises(ValueError,match="conflicting"):parse_bls_calendar(calendar(event(),event(at="20260701T130000")))
    assert len(parse_bls_calendar(calendar(event(),event())))==3


def test_folded_ical_lines_are_parsed_without_instructions():
    parsed=parse_bls_calendar(calendar(event(title="Consumer Price\n Index")))
    assert next(e for e in parsed if e["id"]=="target")["title"]=="Consumer PriceIndex"


@pytest.mark.parametrize("rows",[[{"date":"2026-07-02","open":"09:30","close":"16:00"}],
    [{"date":"2026-07-01","open":"16:00","close":"09:30"}], [{},{}], {}, None])
def test_invalid_exchange_sessions_are_not_defaulted(rows):
    with pytest.raises((ValueError,TypeError,KeyError)):market_session(rows,AT.date())


def test_observed_calendar_span_does_not_cover_unknown_year():
    packet=source(datetime(2030,7,1,15,35,tzinfo=UTC)).collect(datetime(2030,7,1,15,35,tzinfo=UTC))
    assert packet["reason"]=="schedule_date_outside_observed_bls_span"


def test_declared_exchange_source_cannot_be_substituted():
    row=scheduled_snapshot();packet=row["entry_context"]["schedule"]
    packet["calendar_source_url"]="https://example.test/calendar"
    packet["payload_hash"]=digest({k:v for k,v in packet.items() if k!="payload_hash"})
    with pytest.raises(ValueError,match="exchange_source_mismatch"):assess(row)


def test_extended_overnight_hours_cannot_relabel_a_day_session():
    row=scheduled_snapshot();packet=row["entry_context"]["schedule"]
    packet["session"]["close"]="2026-07-02T20:00:00Z"
    packet["payload_hash"]=digest({k:v for k,v in packet.items() if k!="payload_hash"})
    with pytest.raises(ValueError,match="session_hours_invalid"):assess(row)
