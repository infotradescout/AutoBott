from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from .vix_trader import VixBrokerAdapter, VixContractMetadata, VixProduct


class VixBrokerExecutionDisabled(RuntimeError):
    """Raised when VIX broker execution is not dual-armed."""


@dataclass(frozen=True)
class VixBrokerSelection:
    broker_id: str
    execution_enabled: bool
    adapter_ready: bool
    detail: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "broker_id": self.broker_id,
            "execution_enabled": self.execution_enabled,
            "adapter_ready": self.adapter_ready,
            "detail": self.detail,
            "affects_alpaca_paper": False,
        }


def vix_broker_id(*, environ: dict[str, str] | None = None) -> str:
    source = environ if environ is not None else os.environ
    return str(source.get("AUTOBOTT_VIX_BROKER", "disabled")).strip().lower() or "disabled"


def vix_execution_enabled(*, environ: dict[str, str] | None = None) -> bool:
    source = environ if environ is not None else os.environ
    return str(source.get("AUTOBOTT_VIX_EXECUTION_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}


class DisabledVixBrokerAdapter:
    """Default adapter: never submits, never touches Alpaca paper."""

    def get_account(self) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_option_chain(self, product: VixProduct, expiration: date) -> list[dict[str, Any]]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_contract_metadata(self, option_symbol: str) -> VixContractMetadata:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_session_status(self, at: datetime) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_quote(self, option_symbol: str) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def preview_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def cancel_order(self, broker_order_id: str) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def replace_order(self, broker_order_id: str, order: dict[str, Any]) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_order(self, broker_order_id: str) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_fills(self, broker_order_id: str) -> list[dict[str, Any]]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")

    def get_positions(self) -> list[dict[str, Any]]:
        raise VixBrokerExecutionDisabled("vix_broker_execution_disabled")


class IbkrVixBrokerAdapter:
    """Scaffold for Interactive Brokers VIX/VIXW access.

    Construction is allowed only after dual opt-in. Network/TWS calls remain unimplemented until
    credentials and contract probes are wired. This class must never be imported by trading_cycle.
    """

    def __init__(self, *, environ: dict[str, str] | None = None) -> None:
        source = environ if environ is not None else os.environ
        host = str(source.get("AUTOBOTT_IBKR_HOST") or "").strip()
        port = str(source.get("AUTOBOTT_IBKR_PORT") or "").strip()
        client_id = str(source.get("AUTOBOTT_IBKR_CLIENT_ID") or "").strip()
        account = str(source.get("AUTOBOTT_IBKR_ACCOUNT_ID") or "").strip()
        missing = [name for name, value in {
            "AUTOBOTT_IBKR_HOST": host,
            "AUTOBOTT_IBKR_PORT": port,
            "AUTOBOTT_IBKR_CLIENT_ID": client_id,
            "AUTOBOTT_IBKR_ACCOUNT_ID": account,
        }.items() if not value]
        if missing:
            raise VixBrokerExecutionDisabled("ibkr_credentials_incomplete:" + ",".join(missing))
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self.account_id = account
        self._ready = False
        self._detail = "ibkr_adapter_constructed_pending_capability_probe"

    def capability_probe(self) -> dict[str, Any]:
        # Intentional: do not open sockets here until paper isolation tests and operator arming land.
        return {
            "ok": False,
            "broker_id": "ibkr",
            "ready": False,
            "detail": "ibkr_capability_probe_not_implemented",
            "requires": [
                "tws_or_gateway",
                "vix_vixw_market_data",
                "index_options_trading_permission",
            ],
            "affects_alpaca_paper": False,
        }

    def get_account(self) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_option_chain(self, product: VixProduct, expiration: date) -> list[dict[str, Any]]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_contract_metadata(self, option_symbol: str) -> VixContractMetadata:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_session_status(self, at: datetime) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_quote(self, option_symbol: str) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def preview_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def cancel_order(self, broker_order_id: str) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def replace_order(self, broker_order_id: str, order: dict[str, Any]) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_order(self, broker_order_id: str) -> dict[str, Any]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_fills(self, broker_order_id: str) -> list[dict[str, Any]]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")

    def get_positions(self) -> list[dict[str, Any]]:
        raise VixBrokerExecutionDisabled("ibkr_capability_probe_not_implemented")


def describe_vix_broker(*, environ: dict[str, str] | None = None) -> VixBrokerSelection:
    broker = vix_broker_id(environ=environ)
    enabled = vix_execution_enabled(environ=environ)
    if broker in {"", "disabled", "none", "off"}:
        return VixBrokerSelection(broker_id="disabled", execution_enabled=False, adapter_ready=False, detail="vix_broker_disabled")
    if broker != "ibkr":
        return VixBrokerSelection(broker_id=broker, execution_enabled=False, adapter_ready=False, detail="unsupported_vix_broker")
    if not enabled:
        return VixBrokerSelection(broker_id="ibkr", execution_enabled=False, adapter_ready=False, detail="vix_execution_not_armed")
    return VixBrokerSelection(broker_id="ibkr", execution_enabled=True, adapter_ready=False, detail="ibkr_selected_pending_capability_probe")


def load_vix_broker_adapter(*, environ: dict[str, str] | None = None) -> VixBrokerAdapter:
    """Factory for VIX-only brokers. Defaults disabled. Never used by Alpaca paper cycle."""

    selection = describe_vix_broker(environ=environ)
    if selection.broker_id == "disabled" or not selection.execution_enabled:
        return DisabledVixBrokerAdapter()
    if selection.broker_id == "ibkr":
        return IbkrVixBrokerAdapter(environ=environ)
    raise VixBrokerExecutionDisabled(f"unsupported_vix_broker:{selection.broker_id}")
