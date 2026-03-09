from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote_plus, unquote_plus, urlsplit, urlunsplit

import pandas as pd

from .normalize import normalize_header_name

POSTBACK_FINAL_COLUMN = "POSTBACK_FINAL"
POSTBACK_STATUS_COLUMN = "POSTBACK_STATUS"
POSTBACK_WARNINGS_COLUMN = "POSTBACK_WARNINGS"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WRAPPED_PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}|\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}")


def _to_clean_string(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value)


def detect_postback_template_column(columns: list[str]) -> str | None:
    for column in columns:
        if normalize_header_name(column) == "postback":
            return column
    return None


def _build_normalized_column_map(columns: list[str]) -> dict[str, str]:
    normalized_map: dict[str, str] = {}
    for column in columns:
        key = normalize_header_name(column)
        if key and key not in normalized_map:
            normalized_map[key] = column
    return normalized_map


def _looks_like_explicit_placeholder(token: str) -> bool:
    token = token.strip()
    return bool(_IDENTIFIER_RE.fullmatch(token)) and any(ch.isupper() for ch in token)


def _resolve_token_value(
    token: str,
    row: pd.Series,
    normalized_column_map: dict[str, str],
) -> tuple[str, bool, str | None]:
    normalized_token = normalize_header_name(token)
    column_name = normalized_column_map.get(normalized_token)
    if column_name is not None:
        return _to_clean_string(row.get(column_name, "")), True, None
    if _looks_like_explicit_placeholder(token):
        return token, False, token
    return token, False, None


def _replace_wrapped_placeholders(
    template: str,
    row: pd.Series,
    normalized_column_map: dict[str, str],
) -> tuple[str, set[str]]:
    missing_tokens: set[str] = set()

    def _wrapped_replacer(match: re.Match[str]) -> str:
        token = match.group(1) or match.group(2) or ""
        replacement, replaced, missing = _resolve_token_value(token, row, normalized_column_map)
        if missing:
            missing_tokens.add(missing)
        return replacement if replaced else match.group(0)

    return _WRAPPED_PLACEHOLDER_RE.sub(_wrapped_replacer, template), missing_tokens


def _replace_query_value_placeholders(
    template: str,
    row: pd.Series,
    normalized_column_map: dict[str, str],
) -> tuple[str, set[str]]:
    parsed = urlsplit(template)
    if not parsed.query:
        return template, set()

    updated_parts: list[str] = []
    missing_tokens: set[str] = set()
    for chunk in parsed.query.split("&"):
        if not chunk:
            updated_parts.append(chunk)
            continue
        key, separator, value = chunk.partition("=")
        decoded_value = unquote_plus(value)
        replacement, replaced, missing = _resolve_token_value(decoded_value, row, normalized_column_map)
        if missing:
            missing_tokens.add(missing)
        encoded_value = quote_plus(replacement, safe=":/") if replaced else value
        updated_parts.append(f"{key}{separator}{encoded_value}" if separator else chunk)

    rebuilt = urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            "&".join(updated_parts),
            parsed.fragment,
        )
    )
    return rebuilt, missing_tokens


def fill_postback_template(
    template: Any,
    row: pd.Series,
    normalized_column_map: dict[str, str],
) -> dict[str, str]:
    template_text = _to_clean_string(template).strip()
    if not template_text:
        return {
            POSTBACK_FINAL_COLUMN: "",
            POSTBACK_STATUS_COLUMN: "TEMPLATE_VAZIO",
            POSTBACK_WARNINGS_COLUMN: "Template de postback vazio nesta linha.",
        }

    try:
        wrapped_replaced, wrapped_missing = _replace_wrapped_placeholders(
            template_text, row, normalized_column_map
        )
        final_postback, query_missing = _replace_query_value_placeholders(
            wrapped_replaced, row, normalized_column_map
        )
        missing_tokens = sorted(wrapped_missing | query_missing)
        if missing_tokens:
            return {
                POSTBACK_FINAL_COLUMN: final_postback,
                POSTBACK_STATUS_COLUMN: "PLACEHOLDER_SEM_COLUNA",
                POSTBACK_WARNINGS_COLUMN: "Placeholders sem coluna correspondente: "
                + ", ".join(missing_tokens),
            }
        return {
            POSTBACK_FINAL_COLUMN: final_postback,
            POSTBACK_STATUS_COLUMN: "OK",
            POSTBACK_WARNINGS_COLUMN: "",
        }
    except Exception as exc:
        return {
            POSTBACK_FINAL_COLUMN: template_text,
            POSTBACK_STATUS_COLUMN: "ERRO_DE_PROCESSAMENTO",
            POSTBACK_WARNINGS_COLUMN: f"Falha ao processar a linha: {exc}",
        }


def process_postback_dataframe(
    dataframe: pd.DataFrame,
    template_mode: str,
    template_column: str | None = None,
    shared_template: str = "",
) -> dict[str, Any]:
    if template_mode not in {"row", "single"}:
        raise ValueError("Modo de template invalido. Use 'row' ou 'single'.")
    if template_mode == "row" and not template_column:
        raise ValueError("Informe a coluna de template quando o modo for por linha.")
    if template_mode == "row" and template_column not in dataframe.columns:
        raise ValueError(f"Coluna de template nao encontrada: {template_column}")

    working_df = dataframe.copy()
    normalized_column_map = _build_normalized_column_map(working_df.columns.tolist())

    results: list[dict[str, str]] = []
    for _, row in working_df.iterrows():
        template_value = shared_template if template_mode == "single" else row.get(template_column, "")
        results.append(fill_postback_template(template_value, row, normalized_column_map))

    working_df[POSTBACK_FINAL_COLUMN] = [item[POSTBACK_FINAL_COLUMN] for item in results]
    working_df[POSTBACK_STATUS_COLUMN] = [item[POSTBACK_STATUS_COLUMN] for item in results]
    working_df[POSTBACK_WARNINGS_COLUMN] = [item[POSTBACK_WARNINGS_COLUMN] for item in results]

    status_counts = working_df[POSTBACK_STATUS_COLUMN].value_counts(dropna=False).to_dict()
    generated_postbacks = [
        str(value).strip()
        for value in working_df[POSTBACK_FINAL_COLUMN].tolist()
        if str(value).strip()
    ]

    return {
        "df": working_df,
        "stats": {
            "total_rows": int(len(working_df)),
            "ok_rows": int(status_counts.get("OK", 0)),
            "warning_rows": int(len(working_df) - status_counts.get("OK", 0)),
            "template_empty_rows": int(status_counts.get("TEMPLATE_VAZIO", 0)),
            "missing_column_rows": int(status_counts.get("PLACEHOLDER_SEM_COLUNA", 0)),
            "error_rows": int(status_counts.get("ERRO_DE_PROCESSAMENTO", 0)),
        },
        "generated_postbacks": generated_postbacks,
    }
