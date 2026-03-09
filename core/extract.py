from __future__ import annotations

import re
from decimal import Decimal
from typing import Iterable
from urllib.parse import parse_qsl, unquote_plus, urlsplit

import pandas as pd

from .normalize import extract_status_from_texts, normalize_header_name, normalize_identifier, parse_decimal

TOKEN_SPLIT_RE = re.compile(r"""[\s;,|&?=/#:()\[\]{}"'`]+""")
KEY_VALUE_RE = re.compile(r"([A-Za-z0-9_]+)\s*=\s*([^&\s|;,#]+)")

URL_PARAM_KEYS = {
    "subid",
    "sub_id",
    "clickid",
    "click_id",
    "cid",
    "tid",
    "txn",
    "txn_id",
    "transaction_id",
    "order",
    "orderid",
    "order_id",
    "saleid",
    "conversionid",
    "conversion_id",
}
for idx in range(1, 11):
    URL_PARAM_KEYS.add(f"sub{idx}")
    URL_PARAM_KEYS.add(f"s{idx}")

PRIMARY_VALUE_HINTS = ["comissao", "commission", "payout", "comm"]
SECONDARY_VALUE_HINTS = ["valor", "amount", "value", "revenue"]
SECONDARY_PENALTY_HINTS = ["total", "sale_amount", "usd"]


def _add_candidate(raw_value: str, click_set: set[str], txn_set: set[str], found_clicks: set[str], found_txns: set[str]) -> None:
    normalized = normalize_identifier(raw_value)
    if not normalized:
        return
    if normalized in click_set:
        found_clicks.add(normalized)
    if normalized in txn_set:
        found_txns.add(normalized)


def _parse_query_values(cell_text: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    if "?" in cell_text:
        try:
            parsed = urlsplit(cell_text)
            query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
            candidates.extend(query_pairs)
        except Exception:
            pass
    return candidates


def extract_ids_from_row(row_values: Iterable[object], click_set: set[str], txn_set: set[str]) -> dict[str, list[str]]:
    found_clicks: set[str] = set()
    found_txns: set[str] = set()

    for raw_cell in row_values:
        cell_text = "" if raw_cell is None else str(raw_cell)
        if not cell_text.strip():
            continue

        decoded_text = unquote_plus(cell_text)

        # 1) Exact cell match.
        _add_candidate(decoded_text, click_set, txn_set, found_clicks, found_txns)

        # 2) key=value extraction where the key hints at ID context.
        for key, value in KEY_VALUE_RE.findall(decoded_text):
            if normalize_header_name(key) in URL_PARAM_KEYS:
                _add_candidate(value, click_set, txn_set, found_clicks, found_txns)

        # 3) URL query extraction.
        for key, value in _parse_query_values(decoded_text):
            if normalize_header_name(key) in URL_PARAM_KEYS:
                _add_candidate(value, click_set, txn_set, found_clicks, found_txns)

        # 4) Generic tokenization for embedded IDs.
        for token in TOKEN_SPLIT_RE.split(decoded_text):
            if token:
                _add_candidate(token, click_set, txn_set, found_clicks, found_txns)

    return {
        "click_ids": sorted(found_clicks),
        "txn_ids": sorted(found_txns),
    }


def detect_status_in_row(
    row_values: Iterable[object],
    status_keywords: dict[str, list[str]] | None = None,
) -> str | None:
    return extract_status_from_texts(row_values, status_keywords=status_keywords)


def _score_value_column(normalized_column_name: str) -> tuple[int, bool, bool]:
    primary_hits = sum(1 for hint in PRIMARY_VALUE_HINTS if hint in normalized_column_name)
    if primary_hits > 0:
        score = primary_hits * 100
        if "usd" in normalized_column_name:
            score -= 5
        return score, True, False

    secondary_hits = sum(1 for hint in SECONDARY_VALUE_HINTS if hint in normalized_column_name)
    if secondary_hits > 0:
        score = secondary_hits * 10
        for penalty_hint in SECONDARY_PENALTY_HINTS:
            if penalty_hint in normalized_column_name:
                score -= 3
        return score, False, True

    return 0, False, False


def detect_commission_in_row(row: pd.Series) -> Decimal | None:
    primary_candidates: list[tuple[int, str]] = []
    secondary_candidates: list[tuple[int, str]] = []
    for col_name, raw_value in row.items():
        norm_col = normalize_header_name(col_name)
        score, is_primary, is_secondary = _score_value_column(norm_col)
        if is_primary:
            primary_candidates.append((score, str(raw_value)))
        elif is_secondary:
            secondary_candidates.append((score, str(raw_value)))

    # If a primary commission column exists in the schema, do not fallback to generic totals.
    for _, raw_value in sorted(primary_candidates, key=lambda item: item[0], reverse=True):
        parsed = parse_decimal(raw_value)
        if parsed is not None:
            return parsed
    if primary_candidates:
        return None

    # Second pass: weaker hints only when no primary candidate exists.
    for _, raw_value in sorted(secondary_candidates, key=lambda item: item[0], reverse=True):
        parsed = parse_decimal(raw_value)
        if parsed is not None:
            return parsed
    if secondary_candidates:
        return None

    # Final fallback: if no hints exist at all, consider row-wide decimal candidates conservatively.
    parsed_values: list[Decimal] = []
    for raw_value in row.tolist():
        parsed = parse_decimal(raw_value)
        if parsed is None:
            continue
        if abs(parsed) > Decimal("1000000"):
            continue
        parsed_values.append(parsed)

    if len(parsed_values) == 1:
        return parsed_values[0]
    return None
