from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable

REQUIRED_MASTER_FIELDS = [
    "click_id",
    "txn_id",
    "status",
    "commission_value",
    "commission_currency",
]

MASTER_SYNONYMS = {
    "click_id": ["click_id", "clickid", "subid", "sub_id", "s1", "sub1"],
    "txn_id": ["txn_id", "transaction_id", "conversion_id", "order_id", "tid", "txn", "order"],
    "status": ["status", "state"],
    "commission_value": ["payout", "commission", "commission_value", "value", "amount"],
    "commission_currency": ["currency", "commission_currency", "payout_currency", "sale_currency"],
}

# --- Default status aliases ---

DEFAULT_APPROVED_ALIASES: list[str] = [
    "approved", "approve", "approved_conversion",
    "delivered", "entregue", "delivered_order",
    "confirmed", "confirmation", "validated", "valid",
    "paid", "payable", "complete", "completed",
    "success", "successful",
    "aprovado", "aprovada",
    "ready_to_pay",
]

DEFAULT_PENDING_ALIASES: list[str] = [
    "pending", "in_review", "hold",
    "em_analise", "em analise", "em análise",
    "aguardando", "processando",
]

DEFAULT_DECLINED_ALIASES: list[str] = [
    "declined", "rejected", "recusado", "refused",
    "canceled", "cancelled", "cancelado",
]

MASTER_PAID_ALIASES: list[str] = [
    "paid", "pago", "paga", "pagos", "pagas",
]

NON_NUMERIC_RE = re.compile(r"[^0-9,.\-]")
TOKEN_RE = re.compile(r"[a-z0-9_]+")


def build_status_keywords(
    custom_approved: list[str] | None = None,
    custom_pending: list[str] | None = None,
    custom_declined: list[str] | None = None,
) -> dict[str, list[str]]:
    """Build a merged status-keyword dict for a run, merging defaults with optional custom aliases."""
    approved = list(DEFAULT_APPROVED_ALIASES)
    pending = list(DEFAULT_PENDING_ALIASES)
    declined = list(DEFAULT_DECLINED_ALIASES)
    if custom_approved:
        approved += [a.strip() for a in custom_approved if a.strip()]
    if custom_pending:
        pending += [a.strip() for a in custom_pending if a.strip()]
    if custom_declined:
        declined += [a.strip() for a in custom_declined if a.strip()]
    return {"approved": approved, "pending": pending, "declined": declined}


# Default keywords used when no custom ones are provided for this run.
STATUS_KEYWORDS: dict[str, list[str]] = build_status_keywords()


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_header_name(value: object) -> str:
    raw = "" if value is None else str(value)
    lowered = _strip_accents(raw).strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "_", lowered)
    return lowered.strip("_")


def normalize_identifier(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"\s+", "", text).lower()
    return text or None


def normalize_currency(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_text_for_matching(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = _strip_accents(text)
    return re.sub(r"\s+", " ", text)


def parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(" ", "")
    text = NON_NUMERIC_RE.sub("", text)
    if not text:
        return None

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "")
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def decimal_to_string(value: Decimal | None) -> str:
    if value is None:
        return ""
    formatted = format(value, "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted or "0"


def format_money(value: object, places: int = 2) -> str:
    """Format a monetary value to exactly `places` decimal places (default 2)."""
    parsed = parse_decimal(value)
    if parsed is None:
        return ""
    quantizer = Decimal(10) ** -places
    return str(parsed.quantize(quantizer, rounding=ROUND_HALF_UP))


def extract_status_from_texts(
    values: Iterable[object],
    status_keywords: dict[str, list[str]] | None = None,
) -> str | None:
    kw = status_keywords if status_keywords is not None else STATUS_KEYWORDS
    found_statuses: set[str] = set()
    for value in values:
        normalized = normalize_text_for_matching(value)
        if not normalized:
            continue
        tokens = set(TOKEN_RE.findall(normalized.replace(" ", "_")))
        for status, keywords in kw.items():
            for keyword in keywords:
                keyword_norm = normalize_text_for_matching(keyword).replace(" ", "_")
                if "_" in keyword_norm:
                    if keyword_norm in tokens or keyword_norm.replace("_", " ") in normalized:
                        found_statuses.add(status)
                        break
                elif keyword_norm in tokens:
                    found_statuses.add(status)
                    break

    if not found_statuses:
        return None
    if len(found_statuses) == 1:
        return next(iter(found_statuses))
    return "unknown"


def normalize_status_value(
    value: object,
    status_keywords: dict[str, list[str]] | None = None,
) -> str:
    extracted = extract_status_from_texts([value], status_keywords=status_keywords)
    if extracted is not None:
        return extracted
    fallback = normalize_text_for_matching(value)
    return fallback if fallback else "unknown"


def normalize_master_status_value(value: object) -> str:
    """
    Normalize MASTER operational status without collapsing `paid` into `approved`.

    MASTER gating relies on four operational buckets:
    pending, declined, approved, paid.
    """
    normalized = normalize_text_for_matching(value)
    if not normalized:
        return "unknown"

    tokens = set(TOKEN_RE.findall(normalized.replace(" ", "_")))

    for alias in MASTER_PAID_ALIASES:
        alias_norm = normalize_text_for_matching(alias).replace(" ", "_")
        if alias_norm in tokens or alias_norm.replace("_", " ") in normalized:
            return "paid"

    for alias in DEFAULT_PENDING_ALIASES:
        alias_norm = normalize_text_for_matching(alias).replace(" ", "_")
        if alias_norm in tokens or alias_norm.replace("_", " ") in normalized:
            return "pending"

    for alias in DEFAULT_DECLINED_ALIASES:
        alias_norm = normalize_text_for_matching(alias).replace(" ", "_")
        if alias_norm in tokens or alias_norm.replace("_", " ") in normalized:
            return "declined"

    approved_aliases = [alias for alias in DEFAULT_APPROVED_ALIASES if normalize_text_for_matching(alias) != "paid"]
    for alias in approved_aliases:
        alias_norm = normalize_text_for_matching(alias).replace(" ", "_")
        if alias_norm in tokens or alias_norm.replace("_", " ") in normalized:
            return "approved"

    return normalized or "unknown"
