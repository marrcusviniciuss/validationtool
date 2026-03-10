from __future__ import annotations

import csv
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import pandas as pd

from .logger import RunLogger
from .normalize import format_money, parse_decimal

INTERNAL_EXPORT_HEADER = [
    "click_id",
    "offer_id",
    "publisher_id",
    "txn_id",
    "sub1",
    "sub2",
    "sub3",
    "sub4",
    "sale_amount",
    "revenue",
    "payout",
    "sale_currency",
    "status",
    "created",
    "conversion_id",
]

PUBLIC_EXPORT_HEADER = [column for column in INTERNAL_EXPORT_HEADER if column not in {"publisher_id", "affiliate_id"}]

INTERNAL_AUDIT_COLUMNS = [
    "advertiser_row_index",
    "extracted_click_id",
    "extracted_txn_id",
    "all_found_click_ids",
    "all_found_txn_ids",
    "non_empty_cell_count",
    "matched_master_index",
    "matched_by",
    "raw_status_detected",
    "normalized_status",
    "confidence",
    "issue_codes",
    "advertiser_commission_value",
    "master_revenue",
    "decision",
    "diagnostic_hint",
    "master_status_before",
    "matched_master_publisher_id",
]

PUBLIC_AUDIT_COLUMNS = [column for column in INTERNAL_AUDIT_COLUMNS if column != "matched_master_publisher_id"]

PUBLIC_NEEDS_REVIEW_COLUMNS = [
    "advertiser_row_number",
    "issues",
    "found_click_ids",
    "found_txn_ids",
    "detected_status",
    "detected_commission",
    "resolved_status",
    "matched_by",
    "confidence",
    "mapped_master_row_number",
    "mapped_master_click_id",
    "mapped_master_txn_id",
    "master_status_before",
    "row_snapshot_json",
]

PUBLIC_DIFF_COLUMNS = [
    "master_row_number",
    "click_id",
    "txn_id",
    "old_status",
    "new_status",
    "status_source",
    "advertiser_reference_payout",
]

_VALIDATION_OUTPUT_SCHEMAS = {
    "export_internal": INTERNAL_EXPORT_HEADER,
    "export_public": PUBLIC_EXPORT_HEADER,
    "audit_public": PUBLIC_AUDIT_COLUMNS,
    "needs_review_public": PUBLIC_NEEDS_REVIEW_COLUMNS,
    "diff_public": PUBLIC_DIFF_COLUMNS,
}

_SENSITIVE_VALIDATION_COLUMNS = {
    "publisher_id",
    "affiliate_id",
    "matched_master_publisher_id",
    "matched_master_affiliate_id",
}

DEFAULT_BALANCE_FLOOR = Decimal("1.00")
_MONEY = Decimal("0.01")
_CENT = Decimal("0.01")
_MANUAL_APPEND_SOURCE = "manual_append"


def _timestamp_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        path,
        index=False,
        encoding="utf-8",
        sep=",",
        quoting=csv.QUOTE_MINIMAL,
        na_rep="",
    )


def _sanitize_validation_output_dataframe(df: pd.DataFrame, schema_name: str) -> pd.DataFrame:
    schema = _VALIDATION_OUTPUT_SCHEMAS[schema_name]
    sanitized = df.copy()
    sanitized = sanitized.drop(
        columns=[
            column
            for column in sanitized.columns
            if column in _SENSITIVE_VALIDATION_COLUMNS and column not in schema
        ],
        errors="ignore",
    )
    for column in schema:
        if column not in sanitized.columns:
            sanitized[column] = ""
    return sanitized.loc[:, schema]


def _normalize_manual_append_dataframe(manual_append_df: pd.DataFrame | None) -> pd.DataFrame:
    if manual_append_df is None or manual_append_df.empty:
        return pd.DataFrame(columns=PUBLIC_EXPORT_HEADER)

    normalized = manual_append_df.copy().fillna("")
    for column in PUBLIC_EXPORT_HEADER:
        if column not in normalized.columns:
            normalized[column] = ""

    normalized = normalized.loc[:, PUBLIC_EXPORT_HEADER].astype(str)
    for column in ["sale_amount", "revenue", "payout"]:
        normalized[column] = [
            format_money(parsed) if (parsed := parse_decimal(value)) is not None else str(value).strip()
            for value in normalized[column].tolist()
        ]
    normalized["status"] = [str(value).strip().lower() for value in normalized["status"].tolist()]
    normalized["created"] = [str(value).strip() for value in normalized["created"].tolist()]
    normalized["conversion_id"] = [str(value).strip() for value in normalized["conversion_id"].tolist()]
    return normalized


def _append_manual_rows_to_export(
    export_df: pd.DataFrame,
    manual_append_df: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    manual_public_df = _normalize_manual_append_dataframe(manual_append_df)
    if manual_public_df.empty:
        return export_df.copy(), manual_public_df
    combined = pd.concat([export_df.copy(), manual_public_df], ignore_index=True)
    return combined.loc[:, PUBLIC_EXPORT_HEADER], manual_public_df


def _to_money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY, rounding=ROUND_HALF_UP)


def _decimal_to_cents(value: Decimal) -> int:
    return int((_to_money(value) / _CENT).to_integral_value(rounding=ROUND_HALF_UP))


def _cents_to_decimal(value: int) -> Decimal:
    return _to_money(Decimal(value) * _CENT)


def build_export_dataframe(
    master_df: pd.DataFrame,
    export_positions: list[int],
    final_status: pd.Series,
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for pos in export_positions:
        row = {column: "" for column in INTERNAL_EXPORT_HEADER}
        row["click_id"] = master_df.iloc[pos]["click_id"]
        row["publisher_id"] = str(master_df.iloc[pos].get("publisher_id", "")).strip()
        row["txn_id"] = master_df.iloc[pos]["txn_id"]
        payout_raw = parse_decimal(master_df.iloc[pos]["real_revenue"])
        row["payout"] = format_money(payout_raw) if payout_raw is not None else ""
        row["sale_currency"] = master_df.iloc[pos]["commission_currency"]
        status_value = str(final_status.iloc[pos]).strip().lower()
        row["status"] = "approved" if status_value in {"approved", "ready_to_pay"} else status_value
        rows.append(row)
    return pd.DataFrame(rows, columns=INTERNAL_EXPORT_HEADER)


def build_match_audit_dataframe(audit_rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(audit_rows, columns=INTERNAL_AUDIT_COLUMNS)


def _balance_payouts_safe(
    payouts: list[Decimal],
    target_total: Decimal,
    floor: Decimal = DEFAULT_BALANCE_FLOOR,
    preferred_flags: list[bool] | None = None,
    priority_pct: Decimal = Decimal("0"),
) -> tuple[list[Decimal], bool, Decimal]:
    """
    Redistribute payouts safely while keeping every output >= floor.

    Downward balancing uses a descending water-fill strategy:
    larger payouts are reduced first and smaller payouts only move when the
    higher tiers are no longer sufficient. Upward balancing uses proportional
    allocation with deterministic residual-cent distribution.
    """
    row_count = len(payouts)
    if row_count == 0:
        return [], True, Decimal("0.00")

    floor_value = _to_money(floor if floor > Decimal("0") else DEFAULT_BALANCE_FLOOR)
    target_value = _to_money(target_total)

    current_values = [max(_to_money(value), floor_value) for value in payouts]
    current_cents = [_decimal_to_cents(value) for value in current_values]
    floor_cents = _decimal_to_cents(floor_value)
    target_cents = _decimal_to_cents(target_value)
    current_total_cents = sum(current_cents)

    if current_total_cents == target_cents:
        return current_values, True, _cents_to_decimal(current_total_cents)

    min_total_cents = floor_cents * row_count
    if target_cents < min_total_cents:
        floored_values = [_cents_to_decimal(floor_cents)] * row_count
        return floored_values, False, _cents_to_decimal(min_total_cents)

    if target_cents > current_total_cents:
        increase_cents = target_cents - current_total_cents
        weights = list(current_cents)
        boost = max(priority_pct, Decimal("0")) / Decimal("100")
        if preferred_flags and len(preferred_flags) == row_count and boost > Decimal("0"):
            weighted: list[int] = []
            for idx, base_weight in enumerate(weights):
                if preferred_flags[idx]:
                    adjusted = Decimal(base_weight if base_weight > 0 else 1) * (Decimal("1") + boost)
                else:
                    adjusted = Decimal(base_weight if base_weight > 0 else 1)
                weighted.append(int((adjusted * Decimal("100")).to_integral_value(rounding=ROUND_HALF_UP)))
            weights = weighted
        weight_total = sum(weights)
        if weight_total <= 0:
            weights = [1] * row_count
            weight_total = row_count

        increments = [0] * row_count
        distributed = 0
        remainders: list[tuple[Decimal, int, int]] = []
        for idx, weight in enumerate(weights):
            raw_increment = Decimal(increase_cents) * Decimal(weight) / Decimal(weight_total)
            base_increment = int(raw_increment)
            increments[idx] = base_increment
            distributed += base_increment
            remainders.append((raw_increment - Decimal(base_increment), -current_cents[idx], idx))

        residual = increase_cents - distributed
        for _, _, idx in sorted(remainders, reverse=True)[:residual]:
            increments[idx] += 1

        raised_cents = [current_cents[idx] + increments[idx] for idx in range(row_count)]
        actual_total = _cents_to_decimal(sum(raised_cents))
        return [_cents_to_decimal(value) for value in raised_cents], True, actual_total

    reduce_cents = current_total_cents - target_cents
    new_cents = list(current_cents)
    order = sorted(range(row_count), key=lambda idx: (-current_cents[idx], idx))
    remaining = reduce_cents

    for top_count in range(1, row_count + 1):
        if remaining <= 0:
            break

        current_level = new_cents[order[top_count - 1]]
        next_level = max(new_cents[order[top_count]], floor_cents) if top_count < row_count else floor_cents
        if current_level <= next_level:
            continue

        capacity = (current_level - next_level) * top_count
        group = order[:top_count]
        if remaining >= capacity:
            for idx in group:
                new_cents[idx] = next_level
            remaining -= capacity
            continue

        base_drop = remaining // top_count
        residual = remaining % top_count
        for idx in group:
            new_cents[idx] -= base_drop
        for idx in group[:residual]:
            if new_cents[idx] > floor_cents:
                new_cents[idx] -= 1
        remaining = 0

    if remaining > 0:
        for idx in order:
            if remaining <= 0:
                break
            available = new_cents[idx] - floor_cents
            if available <= 0:
                continue
            take = min(available, remaining)
            new_cents[idx] -= take
            remaining -= take

    actual_total = _cents_to_decimal(sum(new_cents))
    exact_reached = remaining == 0 and actual_total == target_value
    return [_cents_to_decimal(value) for value in new_cents], exact_reached, actual_total


def _sum_payout_column(df: pd.DataFrame) -> Decimal:
    total = Decimal("0")
    if "payout" not in df.columns:
        return total
    for value in df["payout"].tolist():
        parsed = parse_decimal(value)
        if parsed is not None:
            total += parsed
    return total


def _sum_decimal_like_values(values: list[object]) -> Decimal:
    total = Decimal("0")
    for value in values:
        parsed = parse_decimal(value)
        if parsed is not None:
            total += parsed
    return total


def build_balanced_export_dataframe(
    export_df: pd.DataFrame,
    difference_delta: Decimal,
    floor: Decimal = DEFAULT_BALANCE_FLOOR,
    priority_publisher_id: str | None = None,
    priority_pct: Decimal = Decimal("0"),
    manual_append_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str, bool, str, str]:
    if export_df.empty or difference_delta == Decimal("0"):
        total = _sum_payout_column(export_df)
        sanitized = _sanitize_validation_output_dataframe(export_df.copy(), "export_public")
        combined_df, _ = _append_manual_rows_to_export(sanitized, manual_append_df)
        return (
            combined_df,
            "0.00",
            True,
            format_money(total),
            format_money(total),
        )

    current_payouts = [parse_decimal(value) or Decimal("0") for value in export_df["payout"].tolist()]
    current_total = _sum_payout_column(export_df)
    target_total = current_total + difference_delta

    preferred_flags: list[bool] | None = None
    normalized_priority = str(priority_publisher_id or "").strip().lower()
    if normalized_priority and "publisher_id" in export_df.columns:
        preferred_flags = [
            str(value).strip().lower() == normalized_priority
            for value in export_df["publisher_id"].tolist()
        ]

    new_payouts, exact_reached, actual_total = _balance_payouts_safe(
        current_payouts,
        target_total,
        floor,
        preferred_flags=preferred_flags,
        priority_pct=priority_pct,
    )

    balanced = export_df.copy()
    balanced["payout"] = [format_money(value) for value in new_payouts]
    if "revenue" in balanced.columns and balanced["revenue"].astype(str).str.strip().ne("").any():
        balanced["revenue"] = [format_money(value) for value in new_payouts]

    average_delta = (actual_total - current_total) / len(new_payouts) if new_payouts else Decimal("0")
    sanitized = _sanitize_validation_output_dataframe(balanced, "export_public")
    combined_df, _ = _append_manual_rows_to_export(sanitized, manual_append_df)
    return (
        combined_df,
        format_money(average_delta),
        exact_reached,
        format_money(actual_total),
        format_money(target_total),
    )


def persist_balanced_export(
    export_df: pd.DataFrame,
    difference_delta: Decimal,
    output_dir: Path,
    floor: Decimal = DEFAULT_BALANCE_FLOOR,
    priority_publisher_id: str | None = None,
    priority_pct: Decimal = Decimal("0"),
    manual_append_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_now()
    balanced_df, average_delta, exact_reached, actual_total, target_total = build_balanced_export_dataframe(
        export_df,
        difference_delta,
        floor,
        priority_publisher_id=priority_publisher_id,
        priority_pct=priority_pct,
        manual_append_df=manual_append_df,
    )
    balanced_path = output_dir / f"validated_export_balanced_{timestamp}.csv"
    _write_csv(balanced_df, balanced_path)
    return {
        "path": str(balanced_path),
        "df": balanced_df,
        "average_delta": average_delta,
        "exact_reached": exact_reached,
        "actual_total": actual_total,
        "target_total": target_total,
        "floor": format_money(floor),
        "row_count": int(len(balanced_df)),
        "manual_append_count": int(len(_normalize_manual_append_dataframe(manual_append_df))),
        "priority_publisher_id": str(priority_publisher_id or "").strip(),
        "priority_pct": format_money(priority_pct),
    }


def build_payout_adjusted_dataframe(
    export_df: pd.DataFrame,
    adjustment_mode: str,
    adjustment_value: Decimal,
    floor: Decimal = DEFAULT_BALANCE_FLOOR,
    manual_append_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str, str, bool]:
    if export_df.empty or adjustment_mode == "none":
        total = _sum_payout_column(export_df)
        sanitized = _sanitize_validation_output_dataframe(export_df.copy(), "export_public")
        combined_df, _ = _append_manual_rows_to_export(sanitized, manual_append_df)
        return (
            combined_df,
            format_money(total),
            "0.00",
            True,
        )

    current_total = _sum_payout_column(export_df)

    if adjustment_mode == "subtract_fixed":
        target_total = current_total - adjustment_value
    elif adjustment_mode == "subtract_pct":
        target_total = current_total * (Decimal("1") - (adjustment_value / Decimal("100")))
    elif adjustment_mode == "set_target":
        target_total = adjustment_value
    else:
        return (
            _sanitize_validation_output_dataframe(export_df.copy(), "export_public"),
            format_money(current_total),
            "0.00",
            True,
        )

    current_payouts = [parse_decimal(value) or Decimal("0") for value in export_df["payout"].tolist()]
    new_payouts, exact_reached, actual_total = _balance_payouts_safe(
        current_payouts,
        target_total,
        floor,
    )

    adjusted = export_df.copy()
    adjusted["payout"] = [format_money(value) for value in new_payouts]
    if "revenue" in adjusted.columns and adjusted["revenue"].astype(str).str.strip().ne("").any():
        adjusted["revenue"] = [format_money(value) for value in new_payouts]

    average_delta = (actual_total - current_total) / len(new_payouts) if new_payouts else Decimal("0")
    sanitized = _sanitize_validation_output_dataframe(adjusted, "export_public")
    combined_df, _ = _append_manual_rows_to_export(sanitized, manual_append_df)
    return (
        combined_df,
        format_money(target_total),
        format_money(average_delta),
        exact_reached,
    )


def persist_payout_adjusted_export(
    export_df: pd.DataFrame,
    adjustment_mode: str,
    adjustment_value: Decimal,
    output_dir: Path,
    floor: Decimal = DEFAULT_BALANCE_FLOOR,
    manual_append_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_now()
    adjusted_df, target_total, average_delta, exact_reached = build_payout_adjusted_dataframe(
        export_df,
        adjustment_mode,
        adjustment_value,
        floor,
        manual_append_df=manual_append_df,
    )
    path = output_dir / f"validated_export_payout_adjusted_{timestamp}.csv"
    _write_csv(adjusted_df, path)
    return {
        "path": str(path),
        "df": adjusted_df,
        "target_total": target_total,
        "average_delta": average_delta,
        "exact_reached": exact_reached,
        "manual_append_count": int(len(_normalize_manual_append_dataframe(manual_append_df))),
    }


def persist_outputs(
    master_df: pd.DataFrame,
    match_result: dict[str, Any],
    logger: RunLogger,
    output_dir: Path,
    manual_append_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_now()

    export_internal_df = build_export_dataframe(
        master_df,
        match_result["export_positions"],
        match_result["final_status"],
    )
    export_base_df = _sanitize_validation_output_dataframe(export_internal_df, "export_public")
    export_df, manual_public_df = _append_manual_rows_to_export(export_base_df, manual_append_df)
    diff_df = _sanitize_validation_output_dataframe(match_result["diff_df"], "diff_public")
    needs_review_df = _sanitize_validation_output_dataframe(match_result["needs_review_df"], "needs_review_public")
    audit_df = _sanitize_validation_output_dataframe(
        build_match_audit_dataframe(match_result.get("audit_rows", [])),
        "audit_public",
    )

    master_total_export_scope = _sum_payout_column(export_internal_df)
    master_total_file_scope = _sum_decimal_like_values(
        master_df.get("real_revenue", pd.Series(dtype=str)).tolist()
    )
    advertiser_total_export_scope = parse_decimal(match_result.get("advertiser_approved_total")) or Decimal("0")
    advertiser_total_file_scope = parse_decimal(match_result.get("advertiser_detected_total_all_rows")) or Decimal("0")
    difference = advertiser_total_export_scope - master_total_export_scope

    export_path = output_dir / f"validated_export_{timestamp}.csv"
    diff_path = output_dir / f"diff_{timestamp}.csv"
    needs_review_path = output_dir / f"needs_review_{timestamp}.csv"
    audit_path = output_dir / f"match_audit_{timestamp}.csv"

    _write_csv(export_df, export_path)
    _write_csv(diff_df, diff_path)
    _write_csv(needs_review_df, needs_review_path)
    _write_csv(audit_df, audit_path)

    logger.info(
        "EXPORT_SUMMARY",
        "Arquivo de exportacao gerado.",
        {
            "export_path": str(export_path),
            "exported_rows_count": int(len(export_df)),
            "first_5_click_id_values_exported": export_df["click_id"].head(5).tolist() if not export_df.empty else [],
            "master_revenue_total": format_money(master_total_export_scope),
            "advertiser_total_reference": format_money(advertiser_total_export_scope),
            "difference_advertiser_minus_master": format_money(difference),
            "master_revenue_total_file_scope": format_money(master_total_file_scope),
            "advertiser_total_detected_file_scope": format_money(advertiser_total_file_scope),
            "manual_append_rows": int(len(manual_public_df)),
            "manual_append_source": _MANUAL_APPEND_SOURCE if not manual_public_df.empty else "",
        },
    )

    comparison = {
        "master_revenue_total": format_money(master_total_export_scope),
        "advertiser_total_reference": format_money(advertiser_total_export_scope),
        "difference_advertiser_minus_master": format_money(difference),
        "master_revenue_total_file_scope": format_money(master_total_file_scope),
        "advertiser_total_detected_file_scope": format_money(advertiser_total_file_scope),
        "advertiser_detected_count_all_rows": int(match_result.get("advertiser_detected_count_all_rows", 0)),
        "needs_balance_up": difference > 0,
        "needs_balance_down": difference < 0,
        "master_total_rows": int(len(master_df)),
        "newly_approved_rows": int(len(match_result.get("newly_approved_positions", []))),
        "carry_forward_rows": int(len(match_result.get("carry_forward_positions", []))),
        "paid_excluded_rows": int(len(match_result.get("paid_excluded_positions", []))),
        "manual_append_rows": int(len(manual_public_df)),
    }

    log_paths = logger.save(
        output_dir,
        timestamp,
        metadata={
            "export_path": str(export_path),
            "diff_path": str(diff_path),
            "needs_review_path": str(needs_review_path),
            "audit_path": str(audit_path),
            "metrics": match_result["metrics"],
            "comparison": comparison,
        },
    )

    return {
        "timestamp": timestamp,
        "export_internal_df": export_internal_df,
        "export_base_df": export_base_df,
        "export_df": export_df,
        "diff_df": diff_df,
        "needs_review_df": needs_review_df,
        "audit_df": audit_df,
        "manual_append_df": manual_public_df,
        "manual_append_count": int(len(manual_public_df)),
        "manual_append_source": _MANUAL_APPEND_SOURCE if not manual_public_df.empty else "",
        "metrics": match_result["metrics"],
        "comparison": comparison,
        "paths": {
            "export": str(export_path),
            "diff": str(diff_path),
            "needs_review": str(needs_review_path),
            "audit": str(audit_path),
            "log_txt": log_paths["txt"],
            "log_json": log_paths["json"],
        },
    }
