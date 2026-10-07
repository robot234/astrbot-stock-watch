"""Versioned, fail-closed permission to consume negative formal risk facts.

Research observations may be retained without being licensed for a current
formal decision. No real source scenario has passed acceptance yet. The tuple
requires a source, a reviewed scenario version, and the exact risk field;
old rows without that version can never become eligible by cache replay.
"""
from __future__ import annotations


POLICY_VERSION = "formal-negative-source-gate/2026-09-29-v1"
ACCEPTED_NEGATIVE_SCENARIOS: frozenset[tuple[str, str, str]] = frozenset()
RISK_FIELDS = ("suspended", "limit_up", "limit_down", "st")


def accepted_negative(source: object, scenario_version: object, field: object) -> bool:
    """Only a code-reviewed, exact source/scenario/field tuple can pass."""
    source_id = str(source or "").strip()
    version = str(scenario_version or "").strip()
    field_name = str(field or "").strip()
    return bool(source_id and version and field_name in RISK_FIELDS and
                (source_id, version, field_name) in ACCEPTED_NEGATIVE_SCENARIOS)


def unlicensed_fields() -> tuple[str, ...]:
    """Fields no accepted scenario covers; while any remains, no retry can make a risk tuple eligible."""
    licensed = {field for _source, _version, field in ACCEPTED_NEGATIVE_SCENARIOS}
    return tuple(field for field in RISK_FIELDS if field not in licensed)


def formal_value(value: object, *, source: object, scenario_version: object, field: object) -> bool | None:
    if value is True:
        return True
    if value is False and accepted_negative(source, scenario_version, field):
        return False
    return None


def mask_quote_negatives(quote):
    """Remove unlicensed negatives, including legacy cache rows with no proof."""
    source = getattr(quote, "risk_source", None)
    version = getattr(quote, "risk_scenario_version", None)
    for field in ("suspended", "limit_up", "limit_down", "st"):
        value = getattr(quote, field, None)
        if value is False and not accepted_negative(source, version, field):
            setattr(quote, field, None)
    return quote
