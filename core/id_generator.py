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
ALPHA_MIXED = string.ascii_letters
ALNUM_LOWER = string.ascii_lowercase + string.digits
ALNUM_UPPER = string.ascii_uppercase + string.digits
ALNUM_MIXED = string.ascii_letters + string.digits


def _clean_ids(example_ids: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in example_ids:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    return cleaned


def _ordered_unique(values: list[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return "".join(ordered)


def _infer_pool(chars: set[str]) -> tuple[str, str]:
    if chars and all(char.isdigit() for char in chars):
        return "digit", DIGITS
    if chars and all(char in HEX_LOWER for char in chars):
        return "hex_lower", HEX_LOWER
    if chars and all(char in HEX_UPPER for char in chars):
        return "hex_upper", HEX_UPPER
    if chars and all(char.islower() and char.isalpha() for char in chars):
        return "lower", LOWER
    if chars and all(char.isupper() and char.isalpha() for char in chars):
        return "upper", UPPER
    if chars and all(char.isalpha() for char in chars):
        return "alpha_mixed", ALPHA_MIXED
    if chars and all(char.isalnum() for char in chars):
        has_lower = any(char.islower() for char in chars)
        has_upper = any(char.isupper() for char in chars)
        if has_lower and not has_upper:
            return "alnum_lower", ALNUM_LOWER
        if has_upper and not has_lower:
            return "alnum_upper", ALNUM_UPPER
        return "alnum_mixed", ALNUM_MIXED
    return "literal_set", "".join(sorted(chars))


def _pool_mask_symbol(pool_name: str) -> str:
    if pool_name == "digit":
        return "9"
    if pool_name == "hex_lower":
        return "h"
    if pool_name == "hex_upper":
        return "H"
    if pool_name == "lower":
        return "a"
    if pool_name == "upper":
        return "A"
    if pool_name == "alpha_mixed":
        return "l"
    if pool_name in {"alnum_lower", "alnum_upper", "alnum_mixed"}:
        return "x"
    return "*"


def _build_run_lengths(value: str) -> list[int]:
    lengths = [1] * len(value)
    if not value:
        return lengths
    start = 0
    while start < len(value):
        end = start + 1
        while end < len(value) and value[end] == value[start]:
            end += 1
        run_length = end - start
        for index in range(start, end):
            lengths[index] = run_length
        start = end
    return lengths


def _build_alpha_run_lengths(value: str) -> list[int]:
    lengths = [0] * len(value)
    if not value:
        return lengths
    start = 0
    while start < len(value):
        if not value[start].isalpha():
            start += 1
            continue
        end = start + 1
        while end < len(value) and value[end].isalpha():
            end += 1
        run_length = end - start
        left_fixed = start == 0 or not value[start - 1].isalnum()
        right_fixed = end == len(value) or not value[end].isalnum()
        if left_fixed or right_fixed:
            for index in range(start, end):
                lengths[index] = run_length
        start = end
    return lengths


def _infer_single_example_specs(example_id: str) -> tuple[list[dict[str, str]], str, int]:
    repeated_lengths = _build_run_lengths(example_id)
    alpha_lengths = _build_alpha_run_lengths(example_id)
    specs: list[dict[str, str]] = []
    mask_chars: list[str] = []
    variable_positions = 0

    for index, char in enumerate(example_id):
        if not char.isalnum():
            specs.append({"type": "literal", "value": char})
            mask_chars.append(char)
            continue

        if repeated_lengths[index] >= 3:
            specs.append({"type": "literal", "value": char})
            mask_chars.append(char)
            continue

        if alpha_lengths[index] >= 2:
            specs.append({"type": "literal", "value": char})
            mask_chars.append(char)
            continue

        pool_name, generator_pool = _infer_pool({char})
        specs.append(
            {
                "type": "generated",
                "pool_name": pool_name,
                "pool": generator_pool,
                "observed_pool": char,
            }
        )
        variable_positions += 1
        mask_chars.append(_pool_mask_symbol(pool_name))

    return specs, "".join(mask_chars), variable_positions


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
    if len(cleaned) == 1:
        specs, mask, variable_positions = _infer_single_example_specs(cleaned[0])
        if variable_positions == 0:
            return {
                "ok": False,
                "error": "no_variable_positions",
                "message": "Padrao sem variacao detectada. Forneca exemplos com parte variavel.",
            }
        return {
            "ok": True,
            "specs": specs,
            "mask": mask,
            "length": length,
            "examples_count": 1,
            "examples": cleaned,
            "literal_positions": int(sum(1 for spec in specs if spec["type"] == "literal")),
            "variable_positions": variable_positions,
        }

    specs: list[dict[str, str]] = []
    variable_positions = 0
    literal_positions = 0
    mask_chars: list[str] = []

    for index in range(length):
        ordered_chars = [item[index] for item in cleaned]
        observed_pool = _ordered_unique(ordered_chars)
        chars = set(ordered_chars)
        if len(chars) == 1:
            literal = ordered_chars[0]
            specs.append({"type": "literal", "value": literal})
            mask_chars.append(literal)
            literal_positions += 1
            continue

        pool_name, generator_pool = _infer_pool(chars)
        if len(generator_pool) <= 1:
            return {
                "ok": False,
                "error": "unsupported_pattern",
                "message": "Nao foi possivel inferir um padrao variavel para gerar novos IDs.",
            }
        specs.append(
            {
                "type": "generated",
                "pool_name": pool_name,
                "pool": generator_pool,
                "observed_pool": observed_pool,
            }
        )
        variable_positions += 1
        mask_chars.append(_pool_mask_symbol(pool_name))

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
        "literal_positions": literal_positions,
        "variable_positions": variable_positions,
    }


def _generate_one(specs: list[dict[str, str]], *, broaden_variable_slots: bool = False) -> str:
    pieces: list[str] = []
    for spec in specs:
        if spec["type"] == "literal":
            pieces.append(spec["value"])
            continue
        observed_pool = spec.get("observed_pool", "")
        generator_pool = spec["pool"]
        if len(observed_pool) >= 2 and not broaden_variable_slots:
            pieces.append(secrets.choice(observed_pool))
            continue
        pieces.append(secrets.choice(generator_pool))
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
    max_attempts = requested * 800
    attempts = 0

    while len(generated) < requested and attempts < max_attempts:
        attempts += 1
        broaden_slots = attempts > (max_attempts // 3)
        candidate = _generate_one(specs, broaden_variable_slots=broaden_slots)
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
        "literal_positions": pattern.get("literal_positions", 0),
        "variable_positions": pattern.get("variable_positions", 0),
        "df": pd.DataFrame({"generated_id": generated}),
    }
