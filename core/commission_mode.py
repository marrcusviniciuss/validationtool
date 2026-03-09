from __future__ import annotations

import random
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import pandas as pd

from .normalize import format_money

_CENT = Decimal("0.01")


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_CENT, rounding=ROUND_HALF_UP)


def _to_cents(value: Decimal) -> int:
    quantized = _quantize_money(value)
    return int((quantized * 100).to_integral_value())


def _cents_to_decimal(value: int) -> Decimal:
    return (Decimal(value) / 100).quantize(_CENT, rounding=ROUND_HALF_UP)


def _build_result(
    *,
    values_in_cents: list[int],
    target_total_cents: int,
    quantity: int,
    mode: str,
    min_value_cents: int,
    exact_reached: bool,
    warning: str = "",
    seed: int | None = None,
) -> dict[str, Any]:
    values = [_cents_to_decimal(value) for value in values_in_cents]
    generated_total = sum(values, start=Decimal("0.00"))
    target_total = _cents_to_decimal(target_total_cents)
    difference = generated_total - target_total

    dataframe = pd.DataFrame(
        {
            "indice": list(range(1, quantity + 1)),
            "valor_gerado": [format_money(value) for value in values],
        }
    )

    return {
        "df": dataframe,
        "values": [format_money(value) for value in values],
        "mode": mode,
        "quantity": quantity,
        "target_total": format_money(target_total),
        "generated_total": format_money(generated_total),
        "difference": format_money(difference),
        "exact_reached": exact_reached and difference == Decimal("0.00"),
        "warning": warning,
        "min_value_floor": format_money(_cents_to_decimal(min_value_cents)),
        "seed": seed,
    }


def _build_floor_only_result(
    *,
    quantity: int,
    target_total_cents: int,
    mode: str,
    min_value_cents: int,
    seed: int | None = None,
) -> dict[str, Any]:
    warning = (
        "Nao foi possivel atingir o total solicitado com a quantidade informada e o valor minimo por linha. "
        "Foi gerado o menor total valido possivel respeitando o piso configurado."
    )
    return _build_result(
        values_in_cents=[min_value_cents] * quantity,
        target_total_cents=target_total_cents,
        quantity=quantity,
        mode=mode,
        min_value_cents=min_value_cents,
        exact_reached=False,
        warning=warning,
        seed=seed,
    )


def _generate_exact_values(total_cents: int, quantity: int) -> list[int]:
    base_value, remainder = divmod(total_cents, quantity)
    return [base_value + (1 if index < remainder else 0) for index in range(quantity)]


def _generate_average_values(
    total_cents: int,
    quantity: int,
    min_value_cents: int,
    seed: int,
) -> list[int]:
    remaining_cents = total_cents - (quantity * min_value_cents)
    if remaining_cents <= 0:
        return [min_value_cents] * quantity

    rng = random.Random(seed)
    weights = [max(0.25, rng.triangular(0.55, 1.45, 1.0)) for _ in range(quantity)]
    weight_sum = sum(weights) or float(quantity)

    raw_allocations = [(remaining_cents * weight) / weight_sum for weight in weights]
    allocations = [int(value) for value in raw_allocations]
    leftover = remaining_cents - sum(allocations)

    ranked_indexes = sorted(
        range(quantity),
        key=lambda index: (raw_allocations[index] - allocations[index], weights[index], -index),
        reverse=True,
    )
    for offset in range(leftover):
        allocations[ranked_indexes[offset % quantity]] += 1

    return [min_value_cents + allocation for allocation in allocations]


def generate_commission_values(
    total_amount: Decimal,
    quantity: int,
    mode: str,
    min_value: Decimal = Decimal("1.00"),
    seed: int | None = None,
) -> dict[str, Any]:
    if quantity <= 0:
        raise ValueError("A quantidade de linhas deve ser maior que zero.")

    total_cents = _to_cents(total_amount)
    min_value_cents = _to_cents(min_value)

    if total_cents <= 0:
        raise ValueError("O valor total deve ser maior que zero.")
    if min_value_cents <= 0:
        raise ValueError("O valor minimo por linha deve ser maior que zero.")
    if mode not in {"exact", "average"}:
        raise ValueError("Modo de geracao invalido. Use 'exact' ou 'average'.")

    minimum_possible_total = quantity * min_value_cents
    if total_cents < minimum_possible_total:
        return _build_floor_only_result(
            quantity=quantity,
            target_total_cents=total_cents,
            mode=mode,
            min_value_cents=min_value_cents,
            seed=seed,
        )

    if mode == "exact":
        values_in_cents = _generate_exact_values(total_cents, quantity)
        return _build_result(
            values_in_cents=values_in_cents,
            target_total_cents=total_cents,
            quantity=quantity,
            mode=mode,
            min_value_cents=min_value_cents,
            exact_reached=True,
            seed=seed,
        )

    effective_seed = seed if seed is not None else (total_cents * 131) + (quantity * 17) + min_value_cents
    values_in_cents = _generate_average_values(
        total_cents=total_cents,
        quantity=quantity,
        min_value_cents=min_value_cents,
        seed=effective_seed,
    )
    return _build_result(
        values_in_cents=values_in_cents,
        target_total_cents=total_cents,
        quantity=quantity,
        mode=mode,
        min_value_cents=min_value_cents,
        exact_reached=True,
        seed=effective_seed,
    )
