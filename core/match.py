from __future__ import annotations

import json
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any

import pandas as pd

from .extract import detect_commission_in_row, detect_status_in_row, extract_ids_from_row
from .logger import RunLogger
from .normalize import format_money, normalize_identifier, parse_decimal

# ---------------------------------------------------------------------------
# Issue codes
# ---------------------------------------------------------------------------

ISSUE_AMBIGUOUS_CLICK = "AMBIGUOUS_MULTIPLE_CLICK_IDS"
ISSUE_AMBIGUOUS_TXN = "AMBIGUOUS_MULTIPLE_TXN_IDS"

ISSUE_CLICK_ID_ONLY = "CLICK_ID_ONLY_MATCH"
ISSUE_CLICK_AMBIGUOUS_RESOLVED = "CLICK_ID_AMBIGUOUS_RESOLVED_BY_TXN"
ISSUE_CLICK_AMBIGUOUS_UNRESOLVED = "CLICK_ID_AMBIGUOUS_UNRESOLVED"
ISSUE_CLICK_TXN_CONFLICT = "CLICK_TXN_CONFLICT"
ISSUE_TXN_ONLY_MATCH = "TXN_ONLY_MATCH"
ISSUE_TXN_ONLY_DISABLED = "TXN_ONLY_MATCH_DISABLED"

ISSUE_MISSING_STATUS_REVIEW = "MISSING_STATUS_NEEDS_REVIEW"
ISSUE_MISSING_STATUS_IMPLICIT = "MISSING_STATUS_IMPLICIT_APPROVED"
ISSUE_UNKNOWN_STATUS = "UNKNOWN_STATUS_VALUE"

ISSUE_DUP_MASTER_CLICK = "DUPLICATE_MASTER_CLICK_ID"
ISSUE_DUP_MASTER_TXN = "DUPLICATE_MASTER_TXN_ID"
ISSUE_MULTI_ADV_PER_MASTER = "MULTIPLE_ADVERTISER_ROWS_FOR_MASTER"

ISSUE_NO_IDENTIFIERS = "NO_MATCHING_IDENTIFIERS"

ISSUE_ALREADY_APPROVED_IN_MASTER = "ALREADY_APPROVED_IN_MASTER"
ISSUE_MASTER_ALREADY_PAID_EXCLUDED = "MASTER_ALREADY_PAID_EXCLUDED"
ISSUE_MASTER_STATUS_NOT_ELIGIBLE = "MASTER_STATUS_NOT_ELIGIBLE_FOR_MATCH"
ISSUE_NEWLY_APPROVED = "NEWLY_APPROVED_FROM_PENDING_OR_DECLINED"

ACTIONABLE_REVIEW_ISSUES = {
    ISSUE_AMBIGUOUS_CLICK,
    ISSUE_AMBIGUOUS_TXN,
    ISSUE_CLICK_AMBIGUOUS_UNRESOLVED,
    ISSUE_CLICK_TXN_CONFLICT,
    ISSUE_TXN_ONLY_DISABLED,
    ISSUE_MISSING_STATUS_REVIEW,
    ISSUE_UNKNOWN_STATUS,
    ISSUE_NO_IDENTIFIERS,
}

STATUS_PRIORITY: dict[str, int] = {
    "approved": 3,
    "ready_to_pay": 3,
    "pending": 2,
    "declined": 1,
    "unknown": 0,
}

CONFIDENCE_SCORE: dict[str, int] = {
    "click_id+txn_id": 3,
    "duplicate_click_resolved_by_txn": 2,
    "click_id": 2,
    "txn_id": 1,
}


def _build_master_lookup(
    master_df: pd.DataFrame,
    logger: RunLogger,
    positions: list[int],
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    click_to_positions: dict[str, list[int]] = defaultdict(list)
    txn_to_positions: dict[str, list[int]] = defaultdict(list)

    for pos in positions:
        row = master_df.iloc[pos]
        click_id = row["click_id"]
        txn_id = row["txn_id"]
        if click_id:
            click_to_positions[click_id].append(pos)
        if txn_id:
            txn_to_positions[txn_id].append(pos)

    for click_id, mapped_positions in click_to_positions.items():
        if len(mapped_positions) > 1:
            logger.warn(
                ISSUE_DUP_MASTER_CLICK,
                "ID de clique duplicado no subconjunto elegivel do MASTER.",
                {"click_id": click_id, "master_positions": mapped_positions},
            )
    for txn_id, mapped_positions in txn_to_positions.items():
        if len(mapped_positions) > 1:
            logger.warn(
                ISSUE_DUP_MASTER_TXN,
                "ID de transacao duplicado no subconjunto elegivel do MASTER.",
                {"txn_id": txn_id, "master_positions": mapped_positions},
            )

    return click_to_positions, txn_to_positions


def _status_priority(status: str | None) -> int:
    if status is None:
        return -1
    return STATUS_PRIORITY.get(str(status).strip().lower(), -1)


def _should_replace_decision(
    existing_status: str | None,
    existing_payout: Decimal | None,
    new_status: str | None,
    new_payout: Decimal | None,
    existing_confidence: int,
    new_confidence: int,
    existing_priority_match: bool,
    new_priority_match: bool,
) -> bool:
    if new_confidence > existing_confidence:
        return True
    if new_confidence < existing_confidence:
        return False

    if new_priority_match and not existing_priority_match:
        return True
    if existing_priority_match and not new_priority_match:
        return False

    existing_priority = _status_priority(existing_status)
    new_priority = _status_priority(new_status)
    if new_priority > existing_priority:
        return True
    if new_priority < existing_priority:
        return False

    if new_priority >= STATUS_PRIORITY.get("approved", 3):
        if new_payout is None:
            return False
        if existing_payout is None:
            return True
        return new_payout > existing_payout
    return False


def _diagnostic_hint(
    issues: list[str],
    found_click_ids: list[str],
    found_txn_ids: list[str],
    non_empty_cell_count: int,
    master_status: str = "",
) -> str:
    if ISSUE_MASTER_ALREADY_PAID_EXCLUDED in issues:
        return "A linha apontou para um registro do MASTER ja pago. Esse registro fica fora desta validacao."
    if ISSUE_ALREADY_APPROVED_IN_MASTER in issues:
        return "A linha apontou para um registro do MASTER ja aprovado. Ele segue como carry-forward no export final."
    if ISSUE_MASTER_STATUS_NOT_ELIGIBLE in issues and master_status:
        return f"O status atual no MASTER e '{master_status}' e esse registro nao entra como candidato ativo para match."
    if ISSUE_NO_IDENTIFIERS in issues:
        if non_empty_cell_count == 0:
            return "Linha vazia ou sem dados."
        return (
            "Nenhum click_id ou txn_id do MASTER foi encontrado nesta linha. "
            "Verifique o formato dos IDs."
        )
    if ISSUE_MISSING_STATUS_REVIEW in issues:
        return (
            "Status nao detectado. Ative a opcao de aprovacao implicita apenas se o arquivo do anunciante nao tiver status."
        )
    if ISSUE_CLICK_TXN_CONFLICT in issues:
        return "click_id e txn_id apontam para linhas diferentes no MASTER."
    if ISSUE_CLICK_AMBIGUOUS_UNRESOLVED in issues:
        cid = found_click_ids[0] if found_click_ids else ""
        return f"click_id '{cid}' aparece em multiplas linhas elegiveis do MASTER e o txn_id nao resolveu a ambiguidade."
    if ISSUE_TXN_ONLY_DISABLED in issues:
        return "Apenas txn_id encontrado, mas o fallback por txn_id esta desligado."
    if ISSUE_UNKNOWN_STATUS in issues:
        return "Status detectado no anunciante, mas nao reconhecido como aprovado, pendente ou recusado."
    if ISSUE_AMBIGUOUS_CLICK in issues or ISSUE_AMBIGUOUS_TXN in issues:
        return "Multiplos IDs foram encontrados na mesma linha do anunciante."
    return ""


def _resolve_match_from_lookup(
    click_id: str | None,
    txn_id: str | None,
    click_lookup: dict[str, list[int]],
    txn_lookup: dict[str, list[int]],
    allow_txn_only_match: bool,
) -> dict[str, Any]:
    issues: list[str] = []
    mapped_master_pos: int | None = None
    matched_by = ""
    confidence = "nenhuma"

    click_positions = click_lookup.get(click_id, []) if click_id else []
    txn_pos: int | None = None
    if txn_id:
        txn_positions = txn_lookup.get(txn_id, [])
        if len(txn_positions) == 1:
            txn_pos = txn_positions[0]
        elif len(txn_positions) > 1:
            issues.append(ISSUE_DUP_MASTER_TXN)

    if click_id and len(click_positions) == 1:
        click_pos = click_positions[0]
        if txn_id:
            if txn_pos is not None:
                if txn_pos == click_pos:
                    mapped_master_pos = click_pos
                    matched_by = "click_id+txn_id"
                    confidence = "alta"
                else:
                    issues.append(ISSUE_CLICK_TXN_CONFLICT)
            else:
                mapped_master_pos = click_pos
                matched_by = "click_id"
                confidence = "alta"
        else:
            mapped_master_pos = click_pos
            matched_by = "click_id"
            confidence = "alta"
            issues.append(ISSUE_CLICK_ID_ONLY)
    elif click_id and len(click_positions) > 1:
        if txn_pos is not None and txn_pos in click_positions:
            mapped_master_pos = txn_pos
            matched_by = "duplicate_click_resolved_by_txn"
            confidence = "media"
            issues.append(ISSUE_CLICK_AMBIGUOUS_RESOLVED)
        else:
            issues.append(ISSUE_CLICK_AMBIGUOUS_UNRESOLVED)
    elif not click_id and txn_id:
        if txn_pos is not None and allow_txn_only_match:
            mapped_master_pos = txn_pos
            matched_by = "txn_id"
            confidence = "baixa"
            issues.append(ISSUE_TXN_ONLY_MATCH)
        elif txn_pos is not None and not allow_txn_only_match:
            issues.append(ISSUE_TXN_ONLY_DISABLED)
    return {
        "mapped_master_pos": mapped_master_pos,
        "matched_by": matched_by,
        "confidence": confidence,
        "issues": issues,
    }


def _sort_positions_for_export(
    master_df: pd.DataFrame,
    positions: list[int],
    priority_publisher_norm: str | None,
) -> list[int]:
    if not priority_publisher_norm:
        return sorted(positions)
    return sorted(
        positions,
        key=lambda pos: (
            0 if str(master_df.iloc[pos].get("publisher_id_norm", "")).strip() == priority_publisher_norm else 1,
            pos,
        ),
    )


def run_matching(
    master_df: pd.DataFrame,
    advertiser_df: pd.DataFrame,
    logger: RunLogger,
    allow_txn_only_match: bool = True,
    implicit_approved_when_no_status: bool = False,
    status_keywords: dict[str, list[str]] | None = None,
    priority_publisher_id: str | None = None,
) -> dict[str, Any]:
    priority_publisher_norm = normalize_identifier(priority_publisher_id) if priority_publisher_id else None

    master_statuses = master_df["status_norm"].map(lambda value: str(value).strip().lower())
    pending_positions = [int(pos) for pos in master_df.index if master_statuses.iloc[pos] == "pending"]
    declined_positions = [int(pos) for pos in master_df.index if master_statuses.iloc[pos] == "declined"]
    carry_forward_positions = [int(pos) for pos in master_df.index if master_statuses.iloc[pos] == "approved"]
    paid_positions = [int(pos) for pos in master_df.index if master_statuses.iloc[pos] == "paid"]
    other_ineligible_positions = [
        int(pos) for pos in master_df.index
        if master_statuses.iloc[pos] not in {"pending", "declined", "approved", "paid"}
    ]
    active_candidate_positions = pending_positions + declined_positions

    active_click_lookup, active_txn_lookup = _build_master_lookup(master_df, logger, active_candidate_positions)
    approved_click_lookup, approved_txn_lookup = _build_master_lookup(master_df, logger, carry_forward_positions)
    paid_click_lookup, paid_txn_lookup = _build_master_lookup(master_df, logger, paid_positions)
    other_click_lookup, other_txn_lookup = _build_master_lookup(master_df, logger, other_ineligible_positions)

    click_set = set(active_click_lookup.keys()) | set(approved_click_lookup.keys()) | set(paid_click_lookup.keys()) | set(other_click_lookup.keys())
    txn_set = set(active_txn_lookup.keys()) | set(approved_txn_lookup.keys()) | set(paid_txn_lookup.keys()) | set(other_txn_lookup.keys())

    final_status = master_statuses.copy()
    final_advertiser_payout = pd.Series([""] * len(master_df), index=master_df.index, dtype=str)
    final_status_source = {int(pos): "master_original" for pos in master_df.index}

    touched_active_positions: set[int] = set()
    newly_approved_positions: set[int] = set()

    issue_counts: Counter[str] = Counter()
    row_logs: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    needs_review_rows: list[dict[str, Any]] = []
    advertiser_detected_total_all_rows = Decimal("0")
    advertiser_detected_count_all_rows = 0

    decision_state: dict[int, dict[str, Any]] = {}

    for adv_idx, adv_row in advertiser_df.iterrows():
        row_number = int(adv_idx) + 2
        row_values = adv_row.tolist()
        extracted = extract_ids_from_row(row_values, click_set, txn_set)
        found_click_ids = extracted["click_ids"]
        found_txn_ids = extracted["txn_ids"]
        click_id = found_click_ids[0] if len(found_click_ids) == 1 else None
        txn_id = found_txn_ids[0] if len(found_txn_ids) == 1 else None

        detected_status = detect_status_in_row(row_values, status_keywords=status_keywords)
        detected_commission = detect_commission_in_row(adv_row)
        if detected_commission is not None:
            advertiser_detected_total_all_rows += detected_commission
            advertiser_detected_count_all_rows += 1

        issues: list[str] = []
        mapped_master_pos: int | None = None
        resolved_status: str | None = None
        matched_by = ""
        confidence = "nenhuma"
        master_bucket = ""
        master_status_before = ""

        if len(found_click_ids) > 1:
            issues.append(ISSUE_AMBIGUOUS_CLICK)
        if len(found_txn_ids) > 1:
            issues.append(ISSUE_AMBIGUOUS_TXN)

        if ISSUE_AMBIGUOUS_CLICK not in issues and ISSUE_AMBIGUOUS_TXN not in issues:
            if not click_id and not txn_id:
                issues.append(ISSUE_NO_IDENTIFIERS)
            else:
                active_result = _resolve_match_from_lookup(
                    click_id,
                    txn_id,
                    active_click_lookup,
                    active_txn_lookup,
                    allow_txn_only_match,
                )
                if active_result["mapped_master_pos"] is not None:
                    mapped_master_pos = int(active_result["mapped_master_pos"])
                    matched_by = str(active_result["matched_by"])
                    confidence = str(active_result["confidence"])
                    issues.extend(active_result["issues"])
                    master_bucket = "active"
                    master_status_before = str(master_df.iloc[mapped_master_pos]["status_norm"]).strip().lower()
                else:
                    issues.extend(active_result["issues"])
                    approved_result = _resolve_match_from_lookup(
                        click_id,
                        txn_id,
                        approved_click_lookup,
                        approved_txn_lookup,
                        True,
                    )
                    paid_result = _resolve_match_from_lookup(
                        click_id,
                        txn_id,
                        paid_click_lookup,
                        paid_txn_lookup,
                        True,
                    )
                    other_result = _resolve_match_from_lookup(
                        click_id,
                        txn_id,
                        other_click_lookup,
                        other_txn_lookup,
                        True,
                    )

                    if approved_result["mapped_master_pos"] is not None:
                        mapped_master_pos = int(approved_result["mapped_master_pos"])
                        matched_by = str(approved_result["matched_by"])
                        confidence = str(approved_result["confidence"])
                        issues.extend(approved_result["issues"])
                        issues.append(ISSUE_ALREADY_APPROVED_IN_MASTER)
                        issues.append(ISSUE_MASTER_STATUS_NOT_ELIGIBLE)
                        master_bucket = "approved"
                        master_status_before = "approved"
                        resolved_status = "approved"
                    elif paid_result["mapped_master_pos"] is not None:
                        mapped_master_pos = int(paid_result["mapped_master_pos"])
                        matched_by = str(paid_result["matched_by"])
                        confidence = str(paid_result["confidence"])
                        issues.extend(paid_result["issues"])
                        issues.append(ISSUE_MASTER_ALREADY_PAID_EXCLUDED)
                        issues.append(ISSUE_MASTER_STATUS_NOT_ELIGIBLE)
                        master_bucket = "paid"
                        master_status_before = "paid"
                        resolved_status = "paid"
                    elif other_result["mapped_master_pos"] is not None:
                        mapped_master_pos = int(other_result["mapped_master_pos"])
                        matched_by = str(other_result["matched_by"])
                        confidence = str(other_result["confidence"])
                        issues.extend(other_result["issues"])
                        issues.append(ISSUE_MASTER_STATUS_NOT_ELIGIBLE)
                        master_bucket = "other"
                        master_status_before = str(master_df.iloc[mapped_master_pos]["status_norm"]).strip().lower()
                        resolved_status = master_status_before
                    elif not issues:
                        issues.append(ISSUE_NO_IDENTIFIERS)

        if master_bucket == "active" and mapped_master_pos is not None:
            if detected_status is None:
                if implicit_approved_when_no_status:
                    resolved_status = "approved"
                    issues.append(ISSUE_MISSING_STATUS_IMPLICIT)
                else:
                    issues.append(ISSUE_MISSING_STATUS_REVIEW)
                    mapped_master_pos = None
            elif detected_status == "unknown":
                resolved_status = "unknown"
                issues.append(ISSUE_UNKNOWN_STATUS)
                mapped_master_pos = None
            else:
                resolved_status = detected_status

        if master_bucket == "active" and mapped_master_pos is not None and resolved_status is not None:
            is_priority_match = (
                priority_publisher_norm is not None
                and str(master_df.iloc[mapped_master_pos].get("publisher_id_norm", "")).strip() == priority_publisher_norm
            )

            if mapped_master_pos in touched_active_positions:
                issues.append(ISSUE_MULTI_ADV_PER_MASTER)
            touched_active_positions.add(mapped_master_pos)

            existing = decision_state.get(mapped_master_pos)
            existing_conf_score = CONFIDENCE_SCORE.get(existing["matched_by"], 0) if existing else -1
            new_conf_score = CONFIDENCE_SCORE.get(matched_by, 0)

            replace = _should_replace_decision(
                existing["status"] if existing else None,
                existing["payout_decimal"] if existing else None,
                resolved_status,
                detected_commission,
                existing_conf_score,
                new_conf_score,
                existing["priority_match"] if existing else False,
                is_priority_match,
            )
            if existing is None or replace:
                final_status.iloc[mapped_master_pos] = resolved_status
                final_advertiser_payout.iloc[mapped_master_pos] = format_money(detected_commission)
                final_status_source[mapped_master_pos] = f"advertiser_row_{row_number}_{matched_by}"
                decision_state[mapped_master_pos] = {
                    "status": resolved_status,
                    "payout_decimal": detected_commission,
                    "matched_by": matched_by,
                    "priority_match": is_priority_match,
                }

        if master_bucket == "active" and mapped_master_pos is not None and str(final_status.iloc[mapped_master_pos]).strip().lower() in {"approved", "ready_to_pay"}:
            newly_approved_positions.add(mapped_master_pos)
            if ISSUE_NEWLY_APPROVED not in issues:
                issues.append(ISSUE_NEWLY_APPROVED)

        non_empty_cell_count = sum(1 for value in row_values if str(value).strip())
        matched_master_row_number = (
            int(master_df.iloc[mapped_master_pos]["master_source_row_number"])
            if mapped_master_pos is not None else ""
        )
        matched_master_click_id = master_df.iloc[mapped_master_pos]["click_id"] if mapped_master_pos is not None else ""
        matched_master_txn_id = master_df.iloc[mapped_master_pos]["txn_id"] if mapped_master_pos is not None else ""
        matched_master_publisher_id = (
            str(master_df.iloc[mapped_master_pos].get("publisher_id", "")).strip()
            if mapped_master_pos is not None else ""
        )
        matched_master_publisher_id_norm = (
            str(master_df.iloc[mapped_master_pos].get("publisher_id_norm", "")).strip()
            if mapped_master_pos is not None else ""
        )
        row_snapshot = {str(col): str(val) for col, val in adv_row.items() if str(val).strip()}

        if master_bucket == "approved":
            decision = "carry_forward_approved"
        elif master_bucket == "paid":
            decision = "excluded_paid"
        elif master_bucket == "active" and mapped_master_pos is not None and resolved_status in {"approved", "ready_to_pay"}:
            decision = "newly_approved"
        elif master_bucket == "active" and mapped_master_pos is not None:
            decision = "mapped_not_approved"
        elif master_bucket == "other":
            decision = "master_status_not_eligible"
        else:
            decision = "not_matched"

        audit_rows.append({
            "advertiser_row_index": row_number,
            "extracted_click_id": click_id or "",
            "extracted_txn_id": txn_id or "",
            "all_found_click_ids": "|".join(found_click_ids),
            "all_found_txn_ids": "|".join(found_txn_ids),
            "non_empty_cell_count": non_empty_cell_count,
            "matched_master_index": matched_master_row_number,
            "matched_by": matched_by,
            "raw_status_detected": detected_status or "",
            "normalized_status": resolved_status or "",
            "confidence": confidence,
            "issue_codes": "|".join(issues),
            "advertiser_commission_value": format_money(detected_commission),
            "master_revenue": (
                format_money(master_df.iloc[mapped_master_pos]["real_revenue"])
                if mapped_master_pos is not None else ""
            ),
            "decision": decision,
            "diagnostic_hint": _diagnostic_hint(
                issues,
                found_click_ids,
                found_txn_ids,
                non_empty_cell_count,
                master_status_before,
            ),
            "master_status_before": master_status_before,
            "matched_master_publisher_id": matched_master_publisher_id,
        })

        row_logs.append({
            "advertiser_row_number": row_number,
            "found_click_ids": found_click_ids,
            "found_txn_ids": found_txn_ids,
            "detected_status": detected_status or "",
            "detected_commission": format_money(detected_commission),
            "resolved_status": resolved_status or "",
            "matched_by": matched_by,
            "confidence": confidence,
            "decision": decision,
            "issues": issues,
            "row_snapshot": row_snapshot,
        })

        for issue in set(issues):
            issue_counts[issue] += 1
            logger.warn(issue, "Problema ou informacao registrada para linha do anunciante.", {"row_number": row_number})

        if any(issue in ACTIONABLE_REVIEW_ISSUES for issue in issues):
            needs_review_rows.append({
                "advertiser_row_number": row_number,
                "issues": "|".join(issues),
                "found_click_ids": "|".join(found_click_ids),
                "found_txn_ids": "|".join(found_txn_ids),
                "detected_status": detected_status or "",
                "detected_commission": format_money(detected_commission),
                "resolved_status": resolved_status or "",
                "matched_by": matched_by,
                "confidence": confidence,
                "mapped_master_row_number": matched_master_row_number,
                "mapped_master_click_id": matched_master_click_id,
                "mapped_master_txn_id": matched_master_txn_id,
                "matched_master_publisher_id": matched_master_publisher_id,
                "master_status_before": master_status_before,
                "row_snapshot_json": json.dumps(row_snapshot, ensure_ascii=False),
                "priority_rank": 0 if priority_publisher_norm and matched_master_publisher_id_norm == priority_publisher_norm else 1,
            })

    ordered_newly_approved_positions = _sort_positions_for_export(
        master_df,
        [
            pos for pos in newly_approved_positions
            if str(final_status.iloc[pos]).strip().lower() in {"approved", "ready_to_pay"}
        ],
        priority_publisher_norm,
    )
    ordered_carry_forward_positions = _sort_positions_for_export(
        master_df,
        carry_forward_positions,
        priority_publisher_norm,
    )
    export_positions = ordered_newly_approved_positions + ordered_carry_forward_positions

    diff_rows: list[dict[str, Any]] = []
    for pos in sorted(touched_active_positions):
        old_status = str(master_df.iloc[pos]["status_norm"]).strip().lower()
        new_status = str(final_status.iloc[pos]).strip().lower()
        advertiser_reference_payout = str(final_advertiser_payout.iloc[pos]).strip()
        if old_status != new_status:
            diff_rows.append({
                "master_row_number": int(master_df.iloc[pos]["master_source_row_number"]),
                "click_id": master_df.iloc[pos]["click_id"],
                "txn_id": master_df.iloc[pos]["txn_id"],
                "old_status": old_status,
                "new_status": new_status,
                "status_source": final_status_source[pos],
                "advertiser_reference_payout": advertiser_reference_payout,
                "publisher_id": str(master_df.iloc[pos].get("publisher_id", "")).strip(),
            })

    needs_review_df = pd.DataFrame(
        needs_review_rows,
        columns=[
            "advertiser_row_number", "issues", "found_click_ids", "found_txn_ids",
            "detected_status", "detected_commission", "resolved_status",
            "matched_by", "confidence",
            "mapped_master_row_number", "mapped_master_click_id", "mapped_master_txn_id",
            "matched_master_publisher_id", "master_status_before", "row_snapshot_json", "priority_rank",
        ],
    )
    if not needs_review_df.empty:
        needs_review_df = needs_review_df.sort_values(by=["priority_rank", "advertiser_row_number"], kind="stable").drop(columns=["priority_rank"])

    diff_df = pd.DataFrame(
        diff_rows,
        columns=[
            "master_row_number", "click_id", "txn_id",
            "old_status", "new_status", "status_source", "advertiser_reference_payout", "publisher_id",
        ],
    )

    advertiser_approved_total = Decimal("0")
    for pos in ordered_newly_approved_positions:
        parsed = parse_decimal(final_advertiser_payout.iloc[pos])
        if parsed is not None:
            advertiser_approved_total += parsed

    metrics = {
        "total_advertiser_rows": int(len(advertiser_df)),
        "master_total_rows": int(len(master_df)),
        "master_pending_count": int(len(pending_positions)),
        "master_declined_count": int(len(declined_positions)),
        "master_already_approved_count": int(len(carry_forward_positions)),
        "master_already_paid_count": int(len(paid_positions)),
        "master_status_not_eligible_count": int(len(paid_positions) + len(other_ineligible_positions) + len(carry_forward_positions)),
        "excluded_paid_count": int(len(paid_positions)),
        "newly_approved_count": int(len(ordered_newly_approved_positions)),
        "final_export_count": int(len(export_positions)),
        "needs_review_count": int(len(needs_review_df)),
        "missing_txn_for_click_count": 0,
        "missing_click_for_txn_count": int(issue_counts[ISSUE_TXN_ONLY_MATCH]),
        "missing_status_count": int(issue_counts[ISSUE_MISSING_STATUS_REVIEW] + issue_counts[ISSUE_MISSING_STATUS_IMPLICIT]),
        "pair_mismatch_count": int(issue_counts[ISSUE_CLICK_TXN_CONFLICT]),
        "click_id_only_count": int(issue_counts[ISSUE_CLICK_ID_ONLY]),
        "ambiguous_unresolved_count": int(issue_counts[ISSUE_CLICK_AMBIGUOUS_UNRESOLVED]),
        "already_approved_seen_in_advertiser_count": int(issue_counts[ISSUE_ALREADY_APPROVED_IN_MASTER]),
        "already_paid_seen_in_advertiser_count": int(issue_counts[ISSUE_MASTER_ALREADY_PAID_EXCLUDED]),
    }

    logger.info(
        "MATCHING_FINISHED",
        "Matching concluido com gating por status do MASTER.",
        {
            "total_advertiser_rows": metrics["total_advertiser_rows"],
            "newly_approved_count": metrics["newly_approved_count"],
            "carry_forward_approved_count": metrics["master_already_approved_count"],
            "final_export_count": metrics["final_export_count"],
            "needs_review_count": metrics["needs_review_count"],
            "paid_excluded_count": metrics["excluded_paid_count"],
        },
    )

    return {
        "final_status": final_status,
        "final_advertiser_payout": final_advertiser_payout,
        "advertiser_approved_total": format_money(advertiser_approved_total),
        "advertiser_detected_total_all_rows": format_money(advertiser_detected_total_all_rows),
        "advertiser_detected_count_all_rows": int(advertiser_detected_count_all_rows),
        "touched_master_positions": sorted(touched_active_positions),
        "newly_approved_positions": ordered_newly_approved_positions,
        "carry_forward_positions": ordered_carry_forward_positions,
        "paid_excluded_positions": sorted(paid_positions),
        "export_positions": export_positions,
        "row_logs": row_logs,
        "audit_rows": audit_rows,
        "needs_review_df": needs_review_df,
        "diff_df": diff_df,
        "metrics": metrics,
    }
