"""Synthetic entry-signal integrity checks; not market-performance evidence."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo
import pytest

from autobott_v2.phase1_models import MarketBar, DirectionBias
from autobott_v2.signal_evidence import score_direction_evidence, _session_vwap
from autobott_v2.phase1_engine import _session_vwap as legacy_reference
from test_signal_evidence import _cycle

START = datetime(2026, 9, 16, 14, 0, tzinfo=UTC)


def bars(start=START, price=100.0, step=0.0, count=35, volume=100):
    result=[]
    for i in range(count):
        opening=price+i*step;close=opening+step
        result.append(MarketBar(start+timedelta(minutes=i),opening,max(opening,close)+.1,
                                min(opening,close)-.1,close,volume))
    return result


@pytest.mark.parametrize("reference", [_session_vwap, legacy_reference])
def test_previous_market_date_cannot_move_current_price_reference(reference):
    previous=bars(start=START-timedelta(days=1),price=20,count=20,volume=100000)
    current=bars(price=100,count=15)
    assert reference(previous+current)==pytest.approx(reference(current))


@pytest.mark.parametrize("reference", [_session_vwap, legacy_reference])
def test_reference_uses_new_york_date_not_utc_midnight(reference):
    # 23:59 UTC and 00:01 UTC are on the same NY market date in September.
    a=MarketBar(datetime(2026,9,16,23,59,tzinfo=UTC),100,100,100,100,100)
    b=MarketBar(datetime(2026,9,17,0,1,tzinfo=UTC),200,200,200,200,100)
    assert reference([a,b])==150
    previous=replace(a,timestamp=datetime(2026,9,16,3,59,tzinfo=UTC),open=10,high=10,low=10,close=10)
    assert reference([previous,a,b])==150


def test_previous_day_high_volume_cannot_supply_current_day_reference():
    previous=bars(start=START-timedelta(days=1),price=30,count=20,volume=100000)
    current=bars(price=100,count=15,volume=0)
    assert _session_vwap(previous+current)==100


@pytest.mark.parametrize("step", [.2,-.2])
def test_missing_benchmarks_do_not_duplicate_own_momentum_as_relative_strength(step):
    result,evidence=score_direction_evidence(bars(step=step),[],[],_cycle())
    assert evidence.medium_momentum!=0
    assert result.relative_strength==0
    assert evidence.relative_strength==0


def test_old_benchmark_window_is_not_a_current_relative_strength_measurement():
    own=bars(step=.2)
    old=bars(start=START-timedelta(days=1),price=200,step=-3)
    result,evidence=score_direction_evidence(own,old,old,_cycle())
    assert result.relative_strength==0
    assert evidence.relative_strength==0


def test_future_benchmark_bars_do_not_change_entry_direction():
    own=bars(step=.2);benchmark=bars(price=200,step=.4)
    future=bars(start=START+timedelta(minutes=35),price=500,step=20,count=30)
    original=score_direction_evidence(own,benchmark,benchmark,_cycle())
    augmented=score_direction_evidence(own,benchmark+future,benchmark+future,_cycle())
    assert augmented==original


def test_exactly_aligned_benchmark_returns_are_used():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    result,_=score_direction_evidence(own,benchmark,benchmark,_cycle())
    expected=(own[-1].close/own[-20].close-1)-(benchmark[-1].close/benchmark[-20].close-1)
    assert result.relative_strength==pytest.approx(expected)


def test_one_matching_benchmark_is_not_diluted_by_one_missing_window():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    old=bars(start=START-timedelta(days=1),price=300,step=-2)
    expected=score_direction_evidence(own,benchmark,[],_cycle())
    actual=score_direction_evidence(own,benchmark,old,_cycle())
    assert actual[0].relative_strength==pytest.approx(expected[0].relative_strength)
    assert actual[1].composite_score==expected[1].composite_score


@pytest.mark.parametrize("flags", [{"late_up_cycle":True},{"late_down_cycle":True},
                                     {"late_up_cycle":True,"late_down_cycle":True}])
def test_cycle_age_alone_cannot_create_direction_on_flat_prices(flags):
    tape=bars();result,evidence=score_direction_evidence(tape,tape,tape,_cycle(**flags))
    assert result.bias==DirectionBias.NEUTRAL
    assert evidence.composite_score==0
    assert evidence.reversal_adjustment==0


@pytest.mark.parametrize("step, flags", [(0.005,{"late_up_cycle":True}),(-0.005,{"late_down_cycle":True})])
def test_unconfirmed_exhaustion_cannot_reverse_the_price_evidence(step,flags):
    own=bars(step=step)
    own=[replace(b,high=max(b.open,b.close),low=min(b.open,b.close)) for b in own]
    _,base=score_direction_evidence(own,own,own,_cycle())
    _,candidate=score_direction_evidence(own,own,own,_cycle(**flags))
    assert base.composite_score*candidate.composite_score>=0
    assert abs(candidate.composite_score)<=abs(base.composite_score)


def test_strong_matching_trend_can_still_produce_an_entry_direction():
    tape=bars(step=.35)
    result,_=score_direction_evidence(tape,tape,tape,_cycle())
    assert result.bias==DirectionBias.BULLISH


def test_signal_does_not_modify_input_prices_or_clock():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    before=deepcopy((own,benchmark))
    score_direction_evidence(own,benchmark,benchmark,_cycle())
    assert (own,benchmark)==before



def test_conflicting_benchmark_endpoint_is_not_trusted():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    conflict=replace(benchmark[-1],close=999)
    result,proof=score_direction_evidence(own,benchmark+[conflict],[],_cycle())
    assert result.relative_strength==0
    assert proof.benchmark_series_used==0


def test_duplicate_and_reordered_benchmark_rows_do_not_reweight_return():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    expected=score_direction_evidence(own,benchmark,benchmark,_cycle())
    actual=score_direction_evidence(own,list(reversed(benchmark))+deepcopy(benchmark),benchmark,_cycle())
    assert actual==expected


def test_benchmark_timezone_formats_refer_to_the_same_instant():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    local=[replace(b,timestamp=b.timestamp.astimezone(ZoneInfo("America/New_York"))) for b in benchmark]
    assert score_direction_evidence(own,local,local,_cycle())==score_direction_evidence(own,benchmark,benchmark,_cycle())


@pytest.mark.parametrize("value", [0,-1,float("nan"),float("inf"),True])
def test_invalid_benchmark_endpoint_cannot_provide_relative_strength(value):
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    benchmark[-1]=replace(benchmark[-1],close=value)
    result,proof=score_direction_evidence(own,benchmark,[],_cycle())
    assert result.relative_strength==0 and proof.benchmark_series_used==0


def test_missing_endpoint_does_not_use_nearest_future_value():
    own=bars(step=.2);benchmark=bars(price=200,step=.1)
    benchmark[-1]=replace(benchmark[-1],timestamp=benchmark[-1].timestamp+timedelta(seconds=1))
    result,proof=score_direction_evidence(own,benchmark,[],_cycle())
    assert result.relative_strength==0 and proof.benchmark_series_used==0


@pytest.mark.parametrize("day,hour", [(datetime(2026,1,12,tzinfo=UTC),5),
                                      (datetime(2026,7,13,tzinfo=UTC),4)])
def test_market_date_reference_respects_daylight_saving_time(day,hour):
    midnight=day.replace(hour=hour)
    prior=MarketBar(midnight-timedelta(seconds=1),20,20,20,20,100000)
    current=MarketBar(midnight,100,100,100,100,100)
    assert _session_vwap([prior,current])==100


@pytest.mark.parametrize("late", ["up","down"])
def test_full_v2_builder_has_no_selected_option_for_cycle_age_only(late):
    from autobott_v2.phase1_engine_v2 import build_decision_card
    from autobott_v2.phase1_models import CycleProfile,CycleStatus,MarketContext,DecisionStatus
    from test_phase1_decision_cards import _input,BASE_TIME
    own=bars(start=BASE_TIME-timedelta(minutes=34),price=215)
    profile=(CycleProfile(bars_since_last_valley=10,median_valley_to_peak_bars=10,cycle_confidence=CycleStatus.HIGH)
             if late=="up" else CycleProfile(bars_since_last_peak=10,median_peak_to_valley_bars=10,cycle_confidence=CycleStatus.HIGH))
    data=replace(_input(bars=own,cycle_profile=profile),context=MarketContext(spy_bars=own,qqq_bars=own,vix_bars=own))
    card=build_decision_card(data)
    assert card.direction.bias==DirectionBias.NEUTRAL
    assert card.decision==DecisionStatus.NO_TRADE
    assert card.selected_contract is None
