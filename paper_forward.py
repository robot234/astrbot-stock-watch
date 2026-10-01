"""Versioned local paper entry and close valuation rules.

Only synthetic validated fixtures can currently supply execution evidence. This
module does not submit orders or infer a real fill from a quote.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, time, timedelta, timezone
import hashlib
import json

PROTOCOL_VERSION = "stock-watch-forward-capture/0.3.0-local-B"
ACCOUNTING_VERSION = "mark-to-close-entry-cash-v1"
FEE_MODEL_ID = "proposal-v0.2"
CHINA = timezone(timedelta(hours=8))
LOT_SIZE = 100
CAPITAL_CNY = Decimal("10000")
COMMISSION_RATE = Decimal("0.0003")
COMMISSION_MIN_CNY = Decimal("5")
TRANSFER_RATE = Decimal("0.00001")
ENTRY_SLIPPAGE_RATE = Decimal("0.001")
CENT = Decimal("0.01")
PERCENT_UNIT = Decimal("0.000001")
PRICE_UNIT = Decimal("0.0001")
ENTRY_WINDOWS = {"A": (time(9, 35), time(10, 0)), "B": (time(9, 35), time(14, 57))}


def aware(value) -> datetime:
    moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("aware_time_required")
    return moment.astimezone(timezone.utc)


def in_entry_window(value, arm: str) -> bool:
    local = aware(value).astimezone(CHINA)
    if arm not in ENTRY_WINDOWS:
        return False
    wall = local.time().replace(tzinfo=None)
    start, end = ENTRY_WINDOWS[arm]
    session = time(9, 30) <= wall < time(11, 30) or time(13) <= wall < time(15)
    return session and start <= wall <= end


def money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def b_limit_price(reference_close) -> Decimal:
    """Frozen B limit on the same four-decimal grid as simulated fills."""
    close = Decimal(str(reference_close))
    if not close.is_finite() or close <= 0:
        raise ValueError("reference_close_invalid")
    return (close * Decimal("1.005") * Decimal("1.002")).quantize(PRICE_UNIT, rounding=ROUND_HALF_UP)


def entry_terms(observed_quote_price, *, capacity: Decimal = CAPITAL_CNY) -> dict:
    """Choose whole lots under a per-candidate opportunity cap."""
    quote = Decimal(str(observed_quote_price))
    if not quote.is_finite() or quote <= 0:
        raise ValueError("entry_quote_invalid")
    filled = (quote * (Decimal(1) + ENTRY_SLIPPAGE_RATE)).quantize(PRICE_UNIT, rounding=ROUND_HALF_UP)
    max_quantity = int(capacity / (filled * LOT_SIZE)) * LOT_SIZE
    if max_quantity > 1_000_000:
        raise ValueError("entry_quantity_unbounded")
    for quantity in range(max_quantity, 0, -LOT_SIZE):
        notional = filled * quantity
        commission = money(max(notional * COMMISSION_RATE, COMMISSION_MIN_CNY))
        transfer = money(notional * TRANSFER_RATE)
        fees = commission + transfer
        total = notional + fees
        if total <= capacity:
            return {"quantity": quantity, "filled_price": filled, "entry_notional_cny": notional,
                    "commission_cny": commission, "transfer_fee_cny": transfer,
                    "entry_fees_cny": fees, "total_cost_cny": total}
    raise ValueError("whole_lot_unaffordable")


def close_mark(close, *, quantity, total_cost_cny) -> Decimal:
    target = Decimal(str(close))
    cash = Decimal(str(total_cost_cny))
    shares = Decimal(str(quantity))
    if (not target.is_finite() or target <= 0 or not cash.is_finite() or cash <= 0
            or not shares.is_finite() or shares <= 0 or shares != shares.to_integral_value()
            or int(shares) % LOT_SIZE):
        raise ValueError("close_mark_inputs_invalid")
    return ((target * shares - cash) / cash * 100).quantize(PERCENT_UNIT, rounding=ROUND_HALF_UP)


def frozen_rule_digest() -> str:
    rules = {"protocol_version": PROTOCOL_VERSION, "accounting_version": ACCOUNTING_VERSION,
             "fee_model_id": FEE_MODEL_ID, "lot_size": LOT_SIZE, "capital": str(CAPITAL_CNY),
             "commission_rate": str(COMMISSION_RATE), "commission_min": str(COMMISSION_MIN_CNY),
             "transfer_rate": str(TRANSFER_RATE), "entry_slippage": str(ENTRY_SLIPPAGE_RATE),
             "windows": {arm: [a.isoformat(), b.isoformat()] for arm, (a, b) in ENTRY_WINDOWS.items()}}
    return hashlib.sha256(json.dumps(rules, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def synthetic_bar_evidence(*, bar_start, bar_close, received_at, volume="100", amount="100000") -> dict:
    """Build explicit engineering evidence; never represents a market feed."""
    start = aware(bar_start)
    row = {"kind": "synthetic_fixture", "bar_start": start.isoformat(),
           "bar_end": (start + timedelta(minutes=1)).isoformat(),
           "bar_open": str(bar_close), "bar_high": str(bar_close),
           "bar_low": str(bar_close), "bar_close": str(bar_close),
           "received_at": aware(received_at).isoformat(),
           "volume": str(volume), "volume_unit": "shares",
           "amount": str(amount), "amount_unit": "CNY",
           "grid_id": start.isoformat(), "grid_count": 1,
           "price_basis": "unadjusted"}
    row["sha256"] = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return row


def synthetic_execution_evidence(*, record_id, quote_at, quote_price, received_at) -> dict:
    row = {"kind": "synthetic_fixture", "record_id": str(record_id),
           "quote_at": aware(quote_at).isoformat(), "quote_price": str(quote_price),
           "received_at": aware(received_at).isoformat(), "price_basis": "unadjusted"}
    row["sha256"] = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return row


def verify_fixture_evidence(row, *, kind, reference_at, received_by, record_id=None, price=None, bar_close=None) -> str | None:
    if not isinstance(row, dict) or row.get("kind") != "synthetic_fixture" or row.get("price_basis") != "unadjusted":
        return None
    data = dict(row)
    supplied = str(data.pop("sha256", ""))
    digest = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if supplied != digest or aware(data.get("received_at")) > aware(received_by):
        return None
    if kind == "bar":
        start = aware(reference_at)
        if (aware(data.get("bar_start")) != start or aware(data.get("bar_end")) != start + timedelta(minutes=1)
                or start.second or start.microsecond or data.get("grid_id") != start.isoformat()
                or data.get("grid_count") != 1 or data.get("volume_unit") != "shares"
                or data.get("amount_unit") != "CNY"
                or Decimal(str(data.get("bar_close"))) != Decimal(str(bar_close))):
            return None
        prices = [Decimal(str(data.get(field))) for field in ("bar_open", "bar_high", "bar_low", "bar_close")]
        if (any(not value.is_finite() or value <= 0 for value in prices)
                or prices[1] < max(prices[0], prices[3]) or prices[2] > min(prices[0], prices[3])):
            return None
        for field in ("volume", "amount"):
            value = Decimal(str(data.get(field)))
            if not value.is_finite() or value <= 0:
                return None
        if aware(data["received_at"]) < start + timedelta(minutes=1):
            return None
    elif kind == "execution":
        if (data.get("record_id") != str(record_id) or aware(data.get("quote_at")) != aware(reference_at)
                or Decimal(str(data.get("quote_price"))) != Decimal(str(price))):
            return None
    else:
        return None
    return digest


def validated_terms(entry, terms) -> bool:
    """Reject missing or internally inconsistent persisted CNY accounting."""
    try:
        if terms["accounting_version"] != ACCOUNTING_VERSION or terms["protocol_version"] != PROTOCOL_VERSION:
            return False
        if terms["fee_model_id"] != FEE_MODEL_ID or terms["freeze_rules_sha256"] != frozen_rule_digest():
            return False
        quantity = int(terms["quantity"])
        if quantity <= 0 or quantity % LOT_SIZE:
            return False
        price = Decimal(str(entry["entry_price"]))
        notional = price * quantity
        commission = money(max(notional * COMMISSION_RATE, COMMISSION_MIN_CNY))
        transfer = money(notional * TRANSFER_RATE)
        fees = commission + transfer
        total = notional + fees
        execution = json.loads(terms["execution_evidence_json"])
        quote = Decimal(str(execution["quote_price"]))
        expected_filled = (quote * (Decimal(1) + ENTRY_SLIPPAGE_RATE)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        return (price == expected_filled and Decimal(str(entry["entry_slippage_pct"])) == Decimal("0.10")
                and notional == Decimal(str(terms["entry_notional_cny"]))
                and commission == Decimal(str(terms["commission_cny"]))
                and transfer == Decimal(str(terms["transfer_fee_cny"]))
                and fees == Decimal(str(terms["entry_fees_cny"]))
                and total == Decimal(str(terms["total_cost_cny"]))
                and total <= CAPITAL_CNY and entry["fill_status"] == "simulated_fill"
                and entry["source_quality"] == "validated_minute")
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False
