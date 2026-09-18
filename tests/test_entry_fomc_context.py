"""Synthetic Fed source and real entry assessment; no broker/network activity."""
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo
import pytest
from autobott_v2.entry_fomc_context import parse_fomc_calendar, fomc_url
from autobott_v2.entry_schedule_context import attach_entry_schedule, digest, POLICY
from test_entry_schedule_context import source, fomc_calendar
from test_entry_market_context import AT, Provider, minute_rows, snapshot, assess


def panel(title='FOMC Meeting', stamp='2:00 p.m.', days='16'):
    return ('<div class="panel-body"><div class="row"><div class="col-xs-2"><p>'+stamp+
            '</p></div><div class="col-xs-7"><p>'+title+'</p></div><div class="col-xs-3"><p>'+
            days+'</p></div></div></div>')


def observed(at=AT, target=None, title='FOMC Meeting', content=None):
    target = target or at
    local = target.astimezone(ZoneInfo('America/New_York'))
    stamp = f'{local.hour % 12 or 12}:{local.minute:02} {"a" if local.hour < 12 else "p"}.m.'
    html = content if content is not None else fomc_calendar(local.date(), panel(title, stamp, str(local.day)))
    row = snapshot(Provider(rows=minute_rows(at=at)), at=at)
    row['entry_context'] = attach_entry_schedule(row['entry_context'], source(at=at, fomc=html), at)
    return row


@pytest.mark.parametrize('title', ['FOMC Meeting', 'FOMC Press Conference', 'FOMC Minutes'])
def test_each_listed_fed_release_blocks_actual_entry(title):
    result = assess(observed(title=title))
    assert result['status'] == 'wait'
    assert result['reason'] == 'scheduled_fomc_release_window'
    assert result['schedule_evidence']['events'][0]['title'] == title


@pytest.mark.parametrize('offset,wait', [(660,False),(600,True),(0,True),(-300,True),(-420,False)])
def test_existing_release_window_policy_applies_to_observed_times(offset, wait):
    result = assess(observed(target=AT + timedelta(seconds=offset)))
    assert (result['status'] == 'wait') is wait, result


def test_refresh_cannot_cross_into_release_window_with_old_trigger():
    row = observed(target=AT + timedelta(seconds=660))
    assert assess(row)['status'] == 'confirmed'
    assert assess(row, at=AT+timedelta(seconds=60))['reason'] == 'scheduled_fomc_release_window'


def test_trigger_must_start_after_the_release_window():
    at = AT + timedelta(seconds=30)
    assert assess(observed(at=at, target=AT-timedelta(seconds=300)), at=at)['reason'] == 'scheduled_fomc_release_window'


@pytest.mark.parametrize('html', ['', '<html>access denied</html>', '<div id="article"><div class="row-title">June 2026</div></div>'])
def test_unavailable_or_wrong_month_is_not_an_empty_success(html):
    row = observed(content=html)
    assert assess(row)['reason'] == 'schedule_source_unavailable'
    assert row['entry_context']['schedule']['fomc_status'] == 'unavailable'
    assert 'fomc' not in row['entry_context']['schedule']


@pytest.mark.parametrize('stamp', ['', 'TBD', '2 p.m.', '14:00', '2:00 UTC', '13:00 p.m.'])
def test_unknown_release_time_is_never_guessed(stamp):
    with pytest.raises(ValueError):
        parse_fomc_calendar(fomc_calendar(date(2026,9,16), panel(stamp=stamp)), date(2026,9,16))


def test_timezone_tracks_summer_and_winter_calendar_dates():
    for day, expected in [(date(2026,9,16), '18:00:00+00:00'), (date(2026,1,16), '19:00:00+00:00')]:
        parsed = parse_fomc_calendar(fomc_calendar(day, panel()), day)
        assert parsed['events'][0]['at'].endswith(expected)
        assert parsed['historical_asof_verified'] is False
        assert parsed['source_url'] == fomc_url(day)


def test_a_listed_month_does_not_claim_earnings_or_unannounced_events_are_clear():
    row = observed(content=fomc_calendar(AT.date()))
    evidence = assess(row)['schedule_evidence']
    assert evidence['status'] == 'observed_no_listed_event_block'
    assert evidence['earnings_status'] == 'not_integrated'
    assert evidence['all_market_event_coverage_verified'] is False


@pytest.mark.parametrize('message', ['', 'Service temporarily unavailable'])
def test_empty_or_soft_error_panel_is_not_an_observed_calendar(message):
    html = fomc_calendar(AT.date(), '<div class="panel-body">'+message+'</div>')
    row = observed(content=html)
    assert row['entry_context']['schedule']['status'] == 'unavailable'
    assert assess(row)['reason'] == 'schedule_source_unavailable'


def test_structured_non_fomc_rows_can_establish_only_limited_month_observation():
    month = parse_fomc_calendar(fomc_calendar(AT.date(), panel('Beige Book', days='2')), AT.date())
    assert month['events'] == []
    assert month['historical_asof_verified'] is False


def test_conflicting_release_times_are_not_silently_selected():
    day = date(2026,9,16)
    with pytest.raises(ValueError, match='conflicting'):
        parse_fomc_calendar(fomc_calendar(day, panel()+panel(stamp='3:00 p.m.')), day)


def test_deleted_native_fed_source_fails_validation():
    row = observed(); packet = row['entry_context']['schedule']; del packet['fomc']
    packet['payload_hash'] = digest({k:v for k,v in packet.items() if k != 'payload_hash'})
    with pytest.raises(ValueError, match='fomc_coverage_invalid'):
        assess(row)


def test_legacy_capsule_retains_explicit_unintegrated_fomc_boundary():
    row = observed(content=fomc_calendar(AT.date())); packet = row['entry_context']['schedule']
    packet.update(schema_version='entry_schedule.v1', policy=deepcopy(POLICY), fomc_status='not_integrated')
    del packet['fomc']
    packet['payload_hash'] = digest({k:v for k,v in packet.items() if k != 'payload_hash'})
    assert assess(row)['schedule_evidence']['fomc_status'] == 'not_integrated'
