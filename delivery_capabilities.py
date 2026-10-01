"""Non-invasive capability evidence, never an inferred delivery guarantee."""
from __future__ import annotations

import inspect


def delivery_capabilities(context):
    sender = getattr(context, "send_message", None)
    result = {
        "schema_version": 1, "sender_available": callable(sender),
        "idempotency": "unknown", "receipt_lookup": "not_exposed",
        "unknown_delivery_retry_allowed": False,
        "return_value_is_downstream_receipt": False,
    }
    if not callable(sender):
        return result
    result["sender"] = f"{getattr(sender, '__module__', '')}.{getattr(sender, '__qualname__', type(sender).__name__)}"
    try:
        params = inspect.signature(sender).parameters
        result["parameters"] = list(params)
        extensible = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        advertised = any(name in params for name in ("idempotency_key", "request_id", "deduplication_key"))
        result["idempotency"] = "unverified" if extensible or advertised else "not_exposed"
    except (TypeError, ValueError):
        result["parameters"] = []
    result["receipt_methods"] = [
        name for name in ("get_delivery_receipt", "lookup_delivery_receipt", "get_message_status")
        if callable(getattr(context, name, None))
    ]
    if result["receipt_methods"]:
        result["receipt_lookup"] = "unverified"
    return result
