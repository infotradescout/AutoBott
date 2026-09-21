from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from autobott_v2 import accounting_recovery as r

SYMBOL = 'NKE260925P00035500'
OTHER = 'RIVN261016P00014000'


def order(identity, side, symbol=SYMBOL, qty=1, price=1, minute=0, status='filled'):
    at = (datetime.now(UTC) - timedelta(minutes=10-minute)).isoformat()
    return {'id':identity,'client_order_id':'fixture-'+identity,'symbol':symbol,'side':side,
            'qty':str(qty),'filled_qty':str(qty),'filled_avg_price':str(price),
            'submitted_at':at,'created_at':at,'filled_at':at,'status':status,'asset_class':'us_option'}


class Broker:
    def __init__(self):
        self.config=SimpleNamespace(environment='paper',allow_live_trading=False,trading_base_url='https://paper-api.alpaca.markets')
        self.orders=[order('closed-buy','buy'),order('closed-sell','sell',price=1.2,minute=1),order('open-buy','buy',symbol=OTHER,minute=2)]
        self.positions=[{'symbol':OTHER,'qty':'1','asset_class':'us_option'}]
        self.account='fixture-paper-account'
        self.calls=[]
    def get_account(self):
        self.calls.append('get_account');return {'id':self.account}
    def list_order_history(self,**kwargs):
        assert kwargs=={'status':'all'};self.calls.append('list_order_history');return deepcopy(self.orders)
    def list_open_positions(self):
        self.calls.append('list_open_positions');return deepcopy(self.positions)
    def submit_order(self,*args,**kwargs):
        raise AssertionError('broker write is forbidden')


def fixture(tmp_path):
    journal=tmp_path/'trade_outcomes.jsonl'
    original=b'{"outcome_id":"unverified-old-row","pnl":-500}\n{"not":"discarded"}\n'
    journal.write_bytes(original)
    store=tmp_path/'open_positions.json'
    store.write_text(json.dumps([{'option_symbol':OTHER,'broker_order_id':'open-buy','quantity':1}]))
    return Broker(),journal,store,original


def prepare(tmp_path):
    broker,journal,store,original=fixture(tmp_path)
    plan=r.prepare_recovery(broker=broker,journal_path=journal,store_path=store,execution_rows=[])
    return broker,journal,store,original,plan


def pending(identity='open-buy',quantity=1):
    return [{'pending_kind':'open_filled_buy','symbol':OTHER,'broker_order_id':identity,'remaining_filled_qty':quantity}]


def test_native_plan_preserves_old_bytes_and_verifies_native_matcher(tmp_path):
    broker,journal,store,original,plan=prepare(tmp_path)
    assert journal.read_bytes()==original
    assert plan['active_row_count']==1 and plan['current_day_realized_pnl']==20
    assert plan['legacy_history_resolved'] is False
    assert plan['open_lot_identity_and_quantity_verified'] is True
    assert set(broker.calls)=={'get_account','list_order_history','list_open_positions'}
    assert plan['_mapping'][0]['disposition']=='unverified_or_outside_window_record_preserved'
    receipt=r.public_receipt(plan)
    assert not any(k.startswith('_') for k in receipt)
    assert broker.account not in json.dumps(receipt)


def test_native_apply_keeps_original_archive_and_leaves_positions_untouched(tmp_path):
    broker,journal,store,original,plan=prepare(tmp_path)
    positions=store.read_bytes()
    result=r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
        expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    directory=Path(result['archive_directory'])
    assert (directory/'original.jsonl').read_bytes()==original
    assert journal.read_bytes()==plan['_candidate']
    assert store.read_bytes()==positions
    assert json.loads(journal.read_text())['account_scope']=='alpaca:paper:fixture-paper-account'
    assert json.loads((directory/'legacy_record_map.json').read_text())[0]['line']==1
    assert json.loads((directory/'applied.json').read_text())['status']=='applied'
    from autobott_v2.outcome_ingestion import plan_outcome_append
    rows=[json.loads(line) for line in journal.read_text().splitlines()]
    assert plan_outcome_append(rows,rows,account_scope=plan['_scope'])['append_safe'] is True


@pytest.mark.parametrize('field',['original','scope'])
def test_native_apply_requires_explicit_digest_confirmation(tmp_path,field):
    broker,journal,store,original,plan=prepare(tmp_path)
    with pytest.raises(r.RecoveryBlocked,match='confirmation_required'):
        r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
            expected_original='' if field=='original' else plan['original_sha256'],
            expected_scope='' if field=='scope' else plan['account_scope_sha256'])
    assert journal.read_bytes()==original


def test_native_replay_cannot_overwrite_new_journal_activity(tmp_path):
    broker,journal,store,original,plan=prepare(tmp_path)
    r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
        expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    current=journal.read_bytes()+b'\n'
    journal.write_bytes(current)
    with pytest.raises(r.RecoveryBlocked,match='local_records_changed_before_apply'):
        r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
            expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    assert journal.read_bytes()==current


@pytest.mark.parametrize('change',['account','orders','positions','store','journal'])
def test_native_changed_input_prevents_apply(tmp_path,change):
    broker,journal,store,original,plan=prepare(tmp_path)
    if change=='account':broker.account='other-account'
    elif change=='orders':broker.orders[0]['filled_avg_price']='2'
    elif change=='positions':broker.positions[0]['qty']='2'
    elif change=='store':store.write_text('[]')
    else:journal.write_bytes(original+b'\n')
    before=journal.read_bytes()
    with pytest.raises(r.RecoveryBlocked):
        r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
            expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    assert journal.read_bytes()==before


@pytest.mark.parametrize('change',['live','host','current_unmatched','open_order','same_quantity_wrong_lot','quantity_mismatch'])
def test_native_invalid_broker_evidence_prevents_plan(tmp_path,change):
    broker,journal,store,original=fixture(tmp_path)
    if change=='live':broker.config.environment='live'
    elif change=='host':broker.config.trading_base_url='https://paper-api.alpaca.markets.attacker.example'
    elif change=='current_unmatched':broker.orders.append(order('unmatched','sell',symbol='SPY260925C00600000'))
    elif change=='open_order':broker.orders[-1]['status']='partially_filled'
    elif change=='same_quantity_wrong_lot':store.write_text(json.dumps([{'option_symbol':OTHER,'broker_order_id':'different-id','quantity':1}]))
    else:broker.positions[0]['qty']='2'
    with pytest.raises(r.RecoveryBlocked):
        r.prepare_recovery(broker=broker,journal_path=journal,store_path=store,execution_rows=[])
    assert journal.read_bytes()==original


def test_native_archive_failure_cannot_replace_active_journal(tmp_path,monkeypatch):
    broker,journal,store,original,plan=prepare(tmp_path)
    monkeypatch.setattr(r,'_exclusive',lambda *a:(_ for _ in ()).throw(OSError('fixture-full-disk')))
    with pytest.raises(OSError):
        r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
            expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    assert journal.read_bytes()==original


def test_native_candidate_tamper_cannot_replace_active_journal(tmp_path):
    broker,journal,store,original,plan=prepare(tmp_path)
    plan['_candidate']=b'[]\n'
    with pytest.raises(r.RecoveryBlocked,match='integrity_failed'):
        r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
            expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    assert journal.read_bytes()==original


def test_pure_same_symbol_quantity_is_not_order_identity_proof():
    with pytest.raises(r.RecoveryBlocked,match='lots_disagree'):
        r.verify_open_lots(pending(),[{'symbol':OTHER,'qty':'1'}],[{'option_symbol':OTHER,'broker_order_id':'wrong','quantity':1}])


def test_pure_exact_open_lots_match():
    r.verify_open_lots(pending(),[{'symbol':OTHER,'qty':'1'}],[{'option_symbol':OTHER,'broker_order_id':'open-buy','quantity':1}])


@pytest.mark.parametrize('value',['NaN','Infinity',True,-1,0,'1.5',None])
def test_pure_bad_quantities_rejected(value):
    with pytest.raises(r.RecoveryBlocked):r._quantity(value)


def test_pure_duplicate_stored_identity_rejected():
    row={'option_symbol':OTHER,'broker_order_id':'open-buy','quantity':1}
    with pytest.raises(r.RecoveryBlocked,match='identity_ambiguous'):
        r.verify_open_lots(pending(),[{'symbol':OTHER,'qty':'1'}],[row,row])


def test_pure_archive_never_overwritten(tmp_path):
    path=tmp_path/'archive.jsonl';r._exclusive(path,b'first')
    r._exclusive(path,b'first')
    with pytest.raises(r.RecoveryBlocked,match='content_conflict'):r._exclusive(path,b'second')
    assert path.read_bytes()==b'first'


def test_pure_unreadable_legacy_bytes_preserved_in_map():
    original=b'{"bad":\n\xff\n\n'
    result=r._legacy_map(original,[],'alpaca:paper:fixture')
    assert len(result)==2 and all(row['disposition']=='unreadable_legacy_record_preserved' for row in result)


def test_pure_hook_inert_without_explicit_mode(monkeypatch):
    monkeypatch.delenv('AUTOBOTT_ACCOUNTING_RECOVERY_MODE',raising=False)
    monkeypatch.setattr(r,'_ATTEMPTED',False)
    assert r.maybe_recover_blocked_cycle({'execution_outcomes':[]}) is None
    assert r._ATTEMPTED is False


def test_native_post_commit_receipt_failure_does_not_claim_rollback(tmp_path,monkeypatch):
    broker,journal,store,original,plan=prepare(tmp_path)
    exclusive=r._exclusive
    def fail_receipt(path,content):
        if path.name=='applied.json':raise OSError('fixture-disk-error')
        return exclusive(path,content)
    monkeypatch.setattr(r,'_exclusive',fail_receipt)
    result=r.apply_recovery(plan,broker=broker,journal_path=journal,store_path=store,
        expected_original=plan['original_sha256'],expected_scope=plan['account_scope_sha256'])
    assert result['status']=='applied_unconfirmed'
    assert journal.read_bytes()==plan['_candidate']
    assert (Path(result['archive_directory'])/'original.jsonl').read_bytes()==original
