from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pandas as pd

from .normalize import (
    MASTER_SYNONYMS,
    REQUIRED_MASTER_FIELDS,
    decimal_to_string,
    normalize_currency,
    normalize_header_name,
    normalize_identifier,
    normalize_master_status_value,
    parse_decimal,
)

MASTER_REVENUE_SYNONYMS = ["revenue", "receita", "valor_real", "valor_liquido"]
MASTER_PUBLISHER_SYNONYMS = ["publisher_id", "publisher", "affiliate_id", "partner_id", "afp", "publisherid"]


def _to_clean_string(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def read_table(uploaded_file: Any) -> pd.DataFrame:
    filename = getattr(uploaded_file, "name", "uploaded.csv")
    suffix = Path(filename).suffix.lower()
    raw_bytes = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else Path(uploaded_file).read_bytes()

    if suffix == ".csv":
        dataframe = None
        last_error: Exception | None = None
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                dataframe = pd.read_csv(
                    io.BytesIO(raw_bytes),
                    dtype=str,
                    keep_default_na=False,
                    sep=None,
                    engine="python",
                    encoding=encoding,
                )
                break
            except Exception as exc:
                last_error = exc
        if dataframe is None:
            raise ValueError(f"Could not read CSV file: {last_error}") from last_error
        # Fallback for semicolon-delimited files that the sniffer may read as a single-column CSV.
        if len(dataframe.columns) == 1 and isinstance(dataframe.columns[0], str) and ";" in dataframe.columns[0]:
            dataframe = None
            for encoding in ("utf-8-sig", "utf-8", "latin-1"):
                try:
                    dataframe = pd.read_csv(
                        io.BytesIO(raw_bytes),
                        dtype=str,
                        keep_default_na=False,
                        sep=";",
                        quotechar='"',
                        encoding=encoding,
                    )
                    break
                except Exception:
                    continue
            if dataframe is None:
                raise ValueError("Could not parse semicolon CSV file.")
    elif suffix in {".xlsx", ".xlsm", ".xls"}:
        try:
            dataframe = pd.read_excel(io.BytesIO(raw_bytes), dtype=str, engine="openpyxl")
        except Exception as exc:
            raise ValueError(
                "Could not read Excel file. If this is .xls, please save it as .xlsx and try again."
            ) from exc
    else:
        raise ValueError("Unsupported file type. Please upload CSV or Excel (.xlsx/.xlsm/.xls).")

    dataframe = dataframe.copy()
    dataframe.columns = [str(col).strip() for col in dataframe.columns]
    dataframe = dataframe.fillna("")
    return dataframe


def auto_detect_master_mapping(columns: list[str]) -> tuple[dict[str, str], list[str]]:
    normalized_columns: dict[str, str] = {}
    for column in columns:
        key = normalize_header_name(column)
        if key and key not in normalized_columns:
            normalized_columns[key] = column

    mapping: dict[str, str] = {}
    for canonical_field, synonyms in MASTER_SYNONYMS.items():
        for synonym in synonyms:
            synonym_key = normalize_header_name(synonym)
            if synonym_key in normalized_columns:
                mapping[canonical_field] = normalized_columns[synonym_key]
                break

    missing = [field for field in REQUIRED_MASTER_FIELDS if not mapping.get(field)]
    return mapping, missing


def validate_master_mapping(mapping: dict[str, str]) -> tuple[bool, list[str]]:
    missing = [field for field in REQUIRED_MASTER_FIELDS if not mapping.get(field)]
    if missing:
        return False, missing
    selected = [mapping[field] for field in REQUIRED_MASTER_FIELDS]
    if len(set(selected)) != len(selected):
        return False, ["duplicate_column_mapping"]
    return True, []


def normalize_master_dataframe(master_df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    valid, errors = validate_master_mapping(mapping)
    if not valid:
        if errors == ["duplicate_column_mapping"]:
            raise ValueError("Invalid MASTER mapping: the same column was selected for multiple required fields.")
        raise ValueError(f"Invalid MASTER mapping. Missing: {', '.join(errors)}")

    for field in REQUIRED_MASTER_FIELDS:
        if mapping[field] not in master_df.columns:
            raise ValueError(f"Mapped MASTER column not found for {field}: {mapping[field]}")

    normalized = pd.DataFrame()
    normalized["master_row_pos"] = range(len(master_df))
    normalized["master_source_row_number"] = [row_idx + 2 for row_idx in range(len(master_df))]

    normalized["click_id_original"] = master_df[mapping["click_id"]].map(_to_clean_string)
    normalized["txn_id_original"] = master_df[mapping["txn_id"]].map(_to_clean_string)
    normalized["status_original"] = master_df[mapping["status"]].map(_to_clean_string)
    normalized["commission_value_original"] = master_df[mapping["commission_value"]].map(_to_clean_string)
    normalized["commission_currency_original"] = master_df[mapping["commission_currency"]].map(_to_clean_string)

    normalized["click_id"] = normalized["click_id_original"].map(normalize_identifier).fillna("")
    normalized["txn_id"] = normalized["txn_id_original"].map(normalize_identifier).fillna("")
    normalized["status_norm"] = normalized["status_original"].map(normalize_master_status_value)

    decimals = normalized["commission_value_original"].map(parse_decimal)
    normalized["commission_value_decimal"] = decimals
    normalized["commission_value"] = decimals.map(decimal_to_string)
    normalized["commission_currency"] = normalized["commission_currency_original"].map(normalize_currency)

    revenue_column: str | None = None
    normalized_column_map = {normalize_header_name(column): column for column in master_df.columns}
    for synonym in MASTER_REVENUE_SYNONYMS:
        synonym_key = normalize_header_name(synonym)
        if synonym_key in normalized_column_map:
            revenue_column = normalized_column_map[synonym_key]
            break

    if revenue_column is not None:
        normalized["real_revenue_original"] = master_df[revenue_column].map(_to_clean_string)
        normalized["real_revenue_decimal"] = normalized["real_revenue_original"].map(parse_decimal)
        normalized["real_revenue_source"] = "master_revenue_column"
    else:
        normalized["real_revenue_original"] = normalized["commission_value_original"]
        normalized["real_revenue_decimal"] = normalized["commission_value_decimal"]
        normalized["real_revenue_source"] = "commission_value_fallback"

    normalized["real_revenue"] = normalized["real_revenue_decimal"].map(decimal_to_string)

    publisher_column: str | None = None
    for synonym in MASTER_PUBLISHER_SYNONYMS:
        synonym_key = normalize_header_name(synonym)
        if synonym_key in normalized_column_map:
            publisher_column = normalized_column_map[synonym_key]
            break

    if publisher_column is not None:
        normalized["publisher_id_original"] = master_df[publisher_column].map(_to_clean_string)
    else:
        normalized["publisher_id_original"] = ""
    normalized["publisher_id"] = normalized["publisher_id_original"].map(_to_clean_string)
    normalized["publisher_id_norm"] = normalized["publisher_id_original"].map(normalize_identifier).fillna("")

    return normalized
