from __future__ import annotations

import secrets
import string
from typing import Any

import pandas as pd

HEX_LOWER = "0123456789abcdef"
HEX_UPPER = "0123456789ABCDEF"
DIGITS = string.digits
LOWER = string.ascii_lowercase
UPPER = string.ascii_uppercase
ALNUM_LOWER = string.ascii_lowercase + string.digits
ALNUM_UPPER = string.ascii_uppercase + string.digits
ALNUM_MIXED = string.ascii_letters + string.digits


def _clean_ids(example_ids: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in example_ids:
        value = str(raw or "").strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    return cleaned


def _infer_pool(chars: set[str]) -> str:
    if chars and all(char.isdigit() for char in chars):
        return DIGITS
    if chars and all(char in HEX_LOWER for char in chars):
        return HEX_LOWER
    if chars and all(char in HEX_UPPER for char in chars):
        return HEX_UPPER
    if chars and all(char.islower() and char.isalpha() for char in chars):
        return LOWER
    if chars and all(char.isupper() and char.isalpha() for char in chars):
        return UPPER
    if chars and all(char.isalnum() for char in chars):
        has_lower = any(char.islower() for char in chars)
        has_upper = any(char.isupper() for char in chars)
        if has_lower and not has_upper:
            return ALNUM_LOWER
        if has_upper and not has_lower:
            return ALNUM_UPPER
        return ALNUM_MIXED
    return "".join(sorted(chars))


def _pool_mask_symbol(pool: str) -> str:
    if pool == DIGITS:
        return "9"
    if pool == HEX_LOWER:
        return "h"
    if pool == HEX_UPPER:
        return "H"
    if pool == LOWER:
        return "a"
    if pool == UPPER:
        return "A"
    if pool in {ALNUM_LOWER, ALNUM_UPPER, ALNUM_MIXED}:
        return "x"
    return "*"


def infer_id_pattern(example_ids: list[str]) -> dict[str, Any]:
    cleaned = _clean_ids(example_ids)
    if not cleaned:
        return {"ok": False, "error": "no_examples", "message": "Informe ao menos um ID exemplo."}

    lengths = {len(item) for item in cleaned}
    if len(lengths) != 1:
        return {
            "ok": False,
            "error": "length_mismatch",
            "message": "Todos os IDs exemplo precisam ter o mesmo tamanho.",
        }

    length = lengths.pop()
    specs: list[dict[str, str]] = []
    variable_positions = 0
    mask_chars: list[str] = []

    for idx in range(length):
        chars = {item[idx] for item in cleaned}
        if len(chars) == 1:
            single = next(iter(chars))
            if not single.isalnum():
                specs.append({"type": "literal", "value": single})
                mask_chars.append(single)
                continue

        pool = _infer_pool(chars)
        if len(pool) <= 1:
            return {
                "ok": False,
                "error": "unsupported_pattern",
                "message": "Nao foi possivel inferir um padrao variavel para gerar novos IDs.",
            }
        specs.append({"type": "pool", "value": pool})
        variable_positions += 1
        mask_chars.append(_pool_mask_symbol(pool))

    if variable_positions == 0:
        return {
            "ok": False,
            "error": "no_variable_positions",
            "message": "Padrao sem variacao detectada. Forneca exemplos com parte variavel.",
        }

    return {
        "ok": True,
        "specs": specs,
        "mask": "".join(mask_chars),
        "length": length,
        "examples_count": len(cleaned),
        "examples": cleaned,
    }


def _generate_one(specs: list[dict[str, str]]) -> str:
    pieces: list[str] = []
    for spec in specs:
        if spec["type"] == "literal":
            pieces.append(spec["value"])
            continue
        pieces.append(secrets.choice(spec["value"]))
    return "".join(pieces)


def generate_similar_ids(example_ids: list[str], total_ids: int = 100) -> dict[str, Any]:
    pattern = infer_id_pattern(example_ids)
    if not pattern.get("ok"):
        return pattern

    requested = max(1, int(total_ids))
    existing = set(pattern["examples"])
    generated: list[str] = []
    seen = set(existing)
    specs = pattern["specs"]
    max_attempts = requested * 500
    attempts = 0

    while len(generated) < requested and attempts < max_attempts:
        attempts += 1
        candidate = _generate_one(specs)
        if candidate in seen:
            continue
        seen.add(candidate)
        generated.append(candidate)

    warning = ""
    if len(generated) < requested:
        warning = (
            "Nao foi possivel gerar a quantidade solicitada com IDs unicos no padrao detectado. "
            f"Gerados: {len(generated)} de {requested}."
        )

    return {
        "ok": True,
        "mask": pattern["mask"],
        "length": pattern["length"],
        "examples_count": pattern["examples_count"],
        "requested_count": requested,
        "generated_count": len(generated),
        "warning": warning,
        "generated_ids": generated,
        "df": pd.DataFrame({"generated_id": generated}),
    }
