"""Synthetic entry-path regressions; no market-performance or execution claims."""
from __future__ import annotations

import copy
import math
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from itertools import permutations

from autobott_v2.core_runner import CoreRunnerRules, select_core_runner_pair
from autobott_v2.phase1_engine_v2 import determine_trade_setup_v2
from autobott_v2.phase1_models import (
    ContractScore, CycleAssessment, CycleStatus, DirectionBias, DirectionResult,
    MarketBar, OptionContractSnapshot, OptionType, Phase1Rules, SelectedContract,
    TradeSetup,
)
from autobott_v2.signal_evidence import DirectionEvidence, _volume_impulse, score_direction_evidence

START = datetime(2026, 9, 16, 14, tzinfo=timezone.utc)
EXPIRY = date(2026, 9, 25)


def bars(*, close: float = 100.1, volume: int = 50) -> list[MarketBar]:
    rows = [MarketBar(START + timedelta(minutes=i), 100, 100.2, 99.8, 100, 100)
            for i in range(29)]
    return rows + [MarketBar(START + timedelta(minutes=29), 100, 100.2, 99.8, close, volume)]


def cycle(**changes) -> CycleAssessment:
    base = CycleAssessment(CycleStatus.UNKNOWN, 0, None, None, None, None,
                           False, False, False, False, False, "unknown", "synthetic", "synthetic")
    return replace(base, **changes)


def evidence(adjustment=0.0) -> DirectionEvidence:
    return DirectionEvidence(0, 0, 0, 0, 0, 0, 0, adjustment, .5)


def direction(bias=DirectionBias.BULLISH, explanation="synthetic") -> DirectionResult:
    return DirectionResult(bias, .5 if bias == DirectionBias.BULLISH else -.5,
                           .01, 0, 0, False, explanation)


def quote(symbol="TEST260925C00100000", strike=100.0, bid=.95, ask=1.05,
          delta=.55, **changes) -> OptionContractSnapshot:
    base = OptionContractSnapshot(symbol, "TEST", EXPIRY, strike, OptionType.CALL,
                                  bid, ask, None, 200, 500, delta, -.02, .05, .5)
    return replace(base, **changes)


def selected(contract=None) -> SelectedContract:
    return SelectedContract.from_score(ContractScore(contract or quote(), .9, 1, ["approved"]), Phase1Rules())


def pair(chain, primary=None):
    return select_core_runner_pair(primary or selected(), chain, rules=CoreRunnerRules())


class VolumeEvidenceIntegrityTests(unittest.TestCase):
    def test_low_volume_red_does_not_support_buying_calls(self):
        self.assertEqual(_volume_impulse(bars(close=99.9)), 0)

    def test_low_volume_green_does_not_support_buying_puts(self):
        self.assertEqual(_volume_impulse(bars()), 0)

    def test_zero_volume_is_no_confirmation_not_max_opposite_evidence(self):
        for close in (99.9, 100, 100.1):
            with self.subTest(close=close):
                self.assertEqual(_volume_impulse(bars(close=close, volume=0)), 0)

    def test_high_volume_doji_is_not_bullish(self):
        self.assertEqual(_volume_impulse(bars(close=100, volume=300)), 0)

    def test_high_volume_green_still_confirms_bullish_direction(self):
        self.assertAlmostEqual(_volume_impulse(bars(volume=200)), math.tanh(1 / .75))

    def test_high_volume_red_still_confirms_bearish_direction(self):
        self.assertAlmostEqual(_volume_impulse(bars(close=99.9, volume=200)), -math.tanh(1 / .75))

    def test_baseline_volume_has_no_impulse(self):
        self.assertEqual(_volume_impulse(bars(volume=100)), 0)

    def test_volume_evidence_cannot_flip_candle_sign_across_range(self):
        for volume in (0, 1, 50, 99, 100, 101, 150, 300, 10000):
            with self.subTest(volume=volume):
                self.assertGreaterEqual(_volume_impulse(bars(volume=volume)), 0)
                self.assertLessEqual(_volume_impulse(bars(close=99.9, volume=volume)), 0)
                self.assertEqual(_volume_impulse(bars(close=100, volume=volume)), 0)

    def test_entire_scorer_uses_corrected_volume_evidence(self):
        for close in (99.9, 100.1):
            result, proof = score_direction_evidence(bars(close=close), [], [], cycle())
            self.assertEqual(result.volume_confirmation, 0)
            self.assertEqual(proof.volume_impulse, 0)
            self.assertNotIn("volume_impulse:", result.explanation)

    def test_scorer_does_not_mutate_input_bars(self):
        rows = bars()
        before = copy.deepcopy(rows)
        score_direction_evidence(rows, [], [], cycle())
        self.assertEqual(rows, before)


class PrimaryContractIntegrityTests(unittest.TestCase):
    def test_valid_primary_and_runner_remain_eligible(self):
        result = pair([quote(), quote("TEST260925C00105000", 105, .20, .25, .18)])
        self.assertIsNotNone(result)
        self.assertEqual(result.primary.option_symbol, selected().option_symbol)
        self.assertEqual(result.estimated_group_cost, 130)
        self.assertEqual(result.runner_cost_ratio, .2381)

    def test_cannot_swap_primary_to_make_expensive_runner_fit(self):
        chain = [quote(), quote("TEST260925C00095000", 95, 1.9, 2, .60),
                 quote("TEST260925C00105000", 105, .45, .50, .18)]
        self.assertIsNone(pair(chain))

    def test_missing_primary_cannot_fall_back_to_other_contract(self):
        self.assertIsNone(pair([quote("TEST260925C00095000", 95, 1.9, 2, .60),
                                quote("TEST260925C00105000", 105, .45, .50, .18)]))

    def test_failed_primary_liquidity_cannot_fall_back(self):
        self.assertIsNone(pair([quote(open_interest=0),
                                quote("TEST260925C00095000", 95, 1.9, 2, .60),
                                quote("TEST260925C00105000", 105, .45, .50, .18)]))

    def test_ambiguous_duplicate_primary_fails_closed(self):
        self.assertIsNone(pair([quote(), quote(), quote("TEST260925C00105000", 105, .20, .25, .18)]))

    def test_primary_symbol_with_changed_identity_fails_closed(self):
        for changes in ({"strike": 99}, {"expiration": date(2026, 10, 2)}, {"option_type": OptionType.PUT}):
            with self.subTest(changes=changes):
                self.assertIsNone(pair([quote(**changes), quote("TEST260925C00105000", 105, .20, .25, .18)]))

    def test_runner_cannot_come_from_different_underlying(self):
        self.assertIsNone(pair([quote(), quote("OTHER260925C00105000", 105, .20, .25, .18,
                                              underlying="OTHER")]))

    def test_wrong_expiry_and_option_type_are_not_runners(self):
        for changes in ({"expiration": date(2026, 10, 2)}, {"option_type": OptionType.PUT}):
            with self.subTest(changes=changes):
                self.assertIsNone(pair([quote(), quote("TEST_OTHER", 105, .20, .25, .18, **changes)]))

    def test_runner_cannot_reuse_primary_symbol(self):
        self.assertIsNone(pair([quote()]))

    def test_quote_spread_and_runner_delta_protections_still_apply(self):
        for changes in ({"bid": .1}, {"delta": .03}, {"volume": 0}, {"open_interest": 0}):
            with self.subTest(changes=changes):
                runner = replace(quote("TEST260925C00105000", 105, .20, .25, .18), **changes)
                self.assertIsNone(pair([quote(), runner]))

    def test_missing_feed_volume_still_uses_existing_oi_policy(self):
        self.assertIsNotNone(pair([quote(volume=0, volume_available=False),
                                  quote("TEST260925C00105000", 105, .20, .25, .18,
                                        volume=0, volume_available=False)]))

    def test_put_primary_and_out_of_money_put_runner_remain_valid(self):
        core = quote("TEST260925P00100000", option_type=OptionType.PUT, delta=-.55)
        runner = quote("TEST260925P00095000", 95, .20, .25, -.18, option_type=OptionType.PUT)
        result = pair([core, runner], selected(core))
        self.assertIsNotNone(result)
        self.assertEqual(result.primary.option_symbol, core.option_symbol)

    def test_order_of_chain_does_not_change_pair(self):
        chain = [quote(), quote("TEST260925C00105000", 105, .20, .25, .18),
                 quote("TEST260925C00106000", 106, .20, .25, .18)]
        results = [pair(list(order)) for order in permutations(chain)]
        self.assertTrue(all(item == results[0] for item in results))

    def test_generator_chain_and_list_chain_match(self):
        chain = [quote(), quote("TEST260925C00105000", 105, .20, .25, .18)]
        self.assertEqual(pair(iter(chain)), pair(chain))

    def test_pair_selection_does_not_mutate_engine_approval_or_chain(self):
        chain = [quote(), quote("TEST260925C00105000", 105, .20, .25, .18)]
        primary = selected()
        before = copy.deepcopy((primary, chain))
        pair(chain, primary)
        self.assertEqual((primary, chain), before)

    def test_existing_exit_metadata_is_not_replaced_by_entry_repair(self):
        result = pair([quote(), quote("TEST260925C00105000", 105, .20, .25, .18)])
        self.assertEqual(result.primary.target_exit_mid, 1.5)
        self.assertEqual(result.primary.stop_exit_mid, .55)
        self.assertEqual(result.primary.exit_rule, "primary_harvest_when_profit_funds_runner_then_retain_runner")
        self.assertEqual(result.runner.exit_rule, "runner_hold_for_convex_upside_after_funding_with_trailing_and_dte_risk_controls")


class StructuredSetupIntegrityTests(unittest.TestCase):
    def test_opposing_reversal_reason_does_not_reclassify_bullish_continuation(self):
        d = direction(explanation="bullish continuous-evidence continuation; reversal_adjustment:-0.55")
        self.assertEqual(determine_trade_setup_v2(d, cycle(), evidence(-.55)), TradeSetup.BULLISH_CONTINUATION)

    def test_opposing_reversal_reason_does_not_reclassify_bearish_continuation(self):
        d = direction(DirectionBias.BEARISH, "bearish continuous-evidence continuation; reversal_adjustment:+0.55")
        self.assertEqual(determine_trade_setup_v2(d, cycle(), evidence(.55)), TradeSetup.BEARISH_CONTINUATION)

    def test_changing_human_explanation_cannot_change_setup(self):
        for text in ("reversal", "no reversal", "continuation", "", "reversal_adjustment:+0.55"):
            with self.subTest(text=text):
                self.assertEqual(determine_trade_setup_v2(direction(explanation=text), cycle(), evidence()),
                                 TradeSetup.BULLISH_CONTINUATION)

    def test_confirmed_same_side_bullish_reversal_is_preserved(self):
        self.assertEqual(determine_trade_setup_v2(direction(), cycle(), evidence(.55)),
                         TradeSetup.LATE_CYCLE_BULLISH_REVERSAL)

    def test_confirmed_same_side_bearish_reversal_is_preserved(self):
        self.assertEqual(determine_trade_setup_v2(direction(DirectionBias.BEARISH), cycle(), evidence(-.55)),
                         TradeSetup.LATE_CYCLE_BEARISH_REVERSAL)

    def test_late_cycle_without_confirmation_is_not_a_reversal(self):
        self.assertEqual(determine_trade_setup_v2(direction(), cycle(late_down_cycle=True)),
                         TradeSetup.BULLISH_CONTINUATION)

    def test_two_argument_call_requires_corresponding_confirmation(self):
        self.assertEqual(determine_trade_setup_v2(direction(), cycle(late_down_cycle=True, bullish_confirmation=True)),
                         TradeSetup.LATE_CYCLE_BULLISH_REVERSAL)
        self.assertEqual(determine_trade_setup_v2(direction(DirectionBias.BEARISH), cycle(late_up_cycle=True, bearish_confirmation=True)),
                         TradeSetup.LATE_CYCLE_BEARISH_REVERSAL)

    def test_structured_evidence_takes_precedence_over_bare_cycle_flag(self):
        self.assertEqual(determine_trade_setup_v2(direction(), cycle(late_down_cycle=True, bullish_confirmation=True), evidence(0)),
                         TradeSetup.BULLISH_CONTINUATION)

    def test_neutral_does_not_become_a_reversal_candidate(self):
        self.assertEqual(determine_trade_setup_v2(direction(DirectionBias.NEUTRAL), cycle(), evidence(.55)), TradeSetup.NO_TRADE)


if __name__ == "__main__":
    unittest.main()
