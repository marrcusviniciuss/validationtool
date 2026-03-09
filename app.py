from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from core import (
    REQUIRED_MASTER_FIELDS,
    USER_AGENT_PRESETS,
    RunLogger,
    auto_detect_master_mapping,
    build_status_keywords,
    check_public_ip,
    detect_postback_template_column,
    generate_similar_ids,
    read_table,
    run_click_checker,
)
from core.normalize import format_money, parse_decimal

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

_ADJ_MODE_MAP = {
    "Sem ajuste": "none",
    "Subtrair valor fixo": "subtract_fixed",
    "Subtrair percentual (%)": "subtract_pct",
    "Definir total alvo": "set_target",
}
_ADJ_LABELS = {
    "subtract_fixed": "Valor a subtrair",
    "subtract_pct": "Percentual a subtrair (%)",
    "set_target": "Total alvo de payout",
}
_POSTBACK_MANUAL_COLUMNS = ["VALOR", "CLICK", "TRANSACTION", "POSTBACK"]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_alias_text(text: str) -> list[str]:
    """Split a comma/newline text area value into individual alias strings."""
    parts = re.split(r"[,\n]+", text or "")
    return [p.strip() for p in parts if p.strip()]


def _read_file_bytes(path: str) -> bytes:
    return Path(path).read_bytes()


def _df_to_csv_bytes(df: Any) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _render_preview_caption(total_rows: int, preview_rows: int) -> None:
    shown_rows = min(total_rows, preview_rows)
    st.caption(
        f"Visualizacao parcial para conferencia. O arquivo baixado contem todas as linhas. "
        f"Mostrando {shown_rows} de {total_rows} linha(s)."
    )


def _load_runtime_module(module_name: str):
    module = importlib.import_module(module_name)
    return importlib.reload(module)


def _render_copy_text_button(label: str, text_to_copy: str, key: str) -> None:
    dom_key = "".join(ch if ch.isalnum() else "_" for ch in key)
    payload = json.dumps(text_to_copy)
    html = f"""
<div style="display:flex; align-items:center; gap:8px; margin: 6px 0 8px 0;">
  <button id="copy_btn_{dom_key}" style="padding:6px 12px; border-radius:6px; border:1px solid #666; cursor:pointer;">
    {label}
  </button>
  <span id="copy_status_{dom_key}" style="font-size:12px; color:#66d9ef;"></span>
</div>
<script>
  const textToCopy_{dom_key} = {payload};
  const button_{dom_key} = document.getElementById("copy_btn_{dom_key}");
  const status_{dom_key} = document.getElementById("copy_status_{dom_key}");
  if (button_{dom_key}) {{
    button_{dom_key}.onclick = async () => {{
      try {{
        await navigator.clipboard.writeText(textToCopy_{dom_key});
        status_{dom_key}.textContent = "Copiado!";
      }} catch (error) {{
        status_{dom_key}.textContent = "Falha ao copiar";
      }}
    }};
  }}
</script>
"""
    components.html(html, height=52)


def _render_copyable_text_block(
    *,
    title: str,
    helper_text: str,
    text_to_copy: str,
    button_label: str,
    key: str,
    height: int = 180,
) -> None:
    st.markdown(f"**{title}**")
    st.caption(helper_text)
    _render_copy_text_button(button_label, text_to_copy, f"{key}_button")
    st.text_area(
        title,
        value=text_to_copy,
        height=height,
        key=f"{key}_text",
        label_visibility="collapsed",
    )


def _sanitize_filename_suffix(value: str, default: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", (value or "").strip()).strip("_")
    return cleaned or default


def _build_dataframe_signature(df: Any) -> str:
    dataframe = df.fillna("").astype(str)
    digest = hashlib.sha1(dataframe.to_csv(index=False).encode("utf-8")).hexdigest()
    return digest


def _build_postback_manual_dataframe(rows: int = 5) -> pd.DataFrame:
    return pd.DataFrame([{column: "" for column in _POSTBACK_MANUAL_COLUMNS} for _ in range(rows)])


def _prepare_manual_postback_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    working_df = dataframe.copy().fillna("")
    for column in _POSTBACK_MANUAL_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = ""
    ordered_columns = [column for column in _POSTBACK_MANUAL_COLUMNS if column in working_df.columns]
    compact_df = working_df[ordered_columns].astype(str)
    non_empty_mask = compact_df.apply(lambda row: any(cell.strip() for cell in row), axis=1)
    return compact_df.loc[non_empty_mask].reset_index(drop=True)


def _render_mapping_ui(columns: list[str], auto_mapping: dict[str, str], has_missing: bool) -> dict[str, str]:
    options = [""] + columns
    mapping: dict[str, str] = {}
    with st.expander("Mapeamento de colunas do MASTER", expanded=has_missing):
        for field in REQUIRED_MASTER_FIELDS:
            suggested = auto_mapping.get(field, "")
            default_index = options.index(suggested) if suggested in options else 0
            mapping[field] = st.selectbox(
                label=field,
                options=options,
                index=default_index,
                key=f"mapping_{field}",
            )
    return mapping


# ---------------------------------------------------------------------------
# Validation pipeline
# ---------------------------------------------------------------------------

def _run_pipeline(
    master_df: Any,
    advertiser_df: Any,
    mapping: dict[str, str],
    allow_txn_only: bool,
    implicit_approved: bool,
    status_keywords: dict[str, list[str]],
    priority_publisher_id: str | None,
) -> dict[str, Any]:
    # Streamlit reruns the script in-process; reloading local business modules keeps
    # the runtime aligned with the latest code on disk during focused refinement passes.
    core_loaders_module = _load_runtime_module("core.loaders")
    core_match_module = _load_runtime_module("core.match")
    core_export_module = _load_runtime_module("core.export")

    logger = RunLogger()
    logger.info(
        "RUN_START",
        "Execucao de validacao iniciada.",
        {"master_rows": int(len(master_df)), "advertiser_rows": int(len(advertiser_df))},
    )

    master_normalized = core_loaders_module.normalize_master_dataframe(master_df, mapping)
    logger.info("MASTER_NORMALIZED", "Dados MASTER normalizados.", {"rows": int(len(master_normalized))})

    match_result = core_match_module.run_matching(
        master_normalized,
        advertiser_df,
        logger,
        allow_txn_only_match=allow_txn_only,
        implicit_approved_when_no_status=implicit_approved,
        status_keywords=status_keywords,
        priority_publisher_id=priority_publisher_id,
    )
    output_result = core_export_module.persist_outputs(master_normalized, match_result, logger, OUTPUTS_DIR)
    return output_result


# ---------------------------------------------------------------------------
# Validation tab
# ---------------------------------------------------------------------------

def _render_validation_tab() -> None:
    st.caption("Faca upload dos arquivos MASTER e ANUNCIANTE, configure as regras e execute a validacao.")

    # ---- File uploads (two columns) ----
    col_master, col_adv = st.columns(2)
    with col_master:
        st.markdown("**1) Arquivo MASTER** (CSV ou Excel)")
        master_upload = st.file_uploader(
            "MASTER",
            type=["csv", "xlsx", "xlsm", "xls"],
            key="master_file",
            label_visibility="collapsed",
        )
    with col_adv:
        st.markdown("**2) Arquivo ANUNCIANTE** (CSV ou Excel)")
        advertiser_upload = st.file_uploader(
            "ANUNCIANTE",
            type=["csv", "xlsx", "xlsm", "xls"],
            key="advertiser_file",
            label_visibility="collapsed",
        )

    master_df = None
    advertiser_df = None
    mapping: dict[str, str] = {}

    if master_upload is not None:
        try:
            master_df = read_table(master_upload)
            with col_master:
                st.caption(
                    f"Pre-visualizacao MASTER - {len(master_df)} linhas, {len(master_df.columns)} colunas"
                )
                st.dataframe(master_df.head(10), use_container_width=True, height=200)

            auto_mapping, missing = auto_detect_master_mapping(master_df.columns.tolist())
            if missing:
                st.warning(
                    "Nao foi possivel detectar automaticamente todas as colunas obrigatorias do MASTER. "
                    "Complete o mapeamento abaixo antes de executar."
                )
            else:
                st.success("Colunas obrigatorias do MASTER detectadas automaticamente.")

            mapping = _render_mapping_ui(master_df.columns.tolist(), auto_mapping, has_missing=bool(missing))
        except Exception as exc:
            st.error(f"Falha ao ler o arquivo MASTER: {exc}")

    if advertiser_upload is not None:
        try:
            advertiser_df = read_table(advertiser_upload)
            with col_adv:
                st.caption(
                    f"Pre-visualizacao ANUNCIANTE - {len(advertiser_df)} linhas, "
                    f"{len(advertiser_df.columns)} colunas"
                )
                st.dataframe(advertiser_df.head(10), use_container_width=True, height=200)
        except Exception as exc:
            st.error(f"Falha ao ler o arquivo ANUNCIANTE: {exc}")

    # ---- Matching rules expander ----
    with st.expander("Regras de correspondencia", expanded=False):
        st.markdown(
            "**Como funciona o motor de correspondencia (v2):**\n\n"
            "- `click_id` e o identificador **primario** - um click_id unico ja e suficiente para aprovar.\n"
            "- `txn_id` e o validador secundario - confirma ou desambigua a correspondencia.\n"
            "- `click_id` + `txn_id` apontando para a mesma linha = alta confianca.\n"
            "- `click_id` duplicado no MASTER: `txn_id` e usado para desambiguar.\n"
            "- `click_id` e `txn_id` apontando para linhas diferentes = conflito, nao aprovado.\n"
            "- Status nao detectado vai para revisao por padrao (configuravel abaixo)."
        )
        allow_txn_only = st.checkbox(
            "Permitir correspondencia somente por txn_id (confianca baixa)",
            value=True,
            key="allow_txn_only",
        )
        implicit_approved = st.checkbox(
            "Aprovar implicitamente linhas sem status detectado (padrao: envia para revisao)",
            value=False,
            key="implicit_approved",
        )

    # ---- Status alias expander ----
    with st.expander("Aliases de status personalizados", expanded=False):
        st.caption(
            "Adicione aliases extras separados por virgula ou linha. "
            "Serao mesclados com os aliases padrao apenas nesta execucao."
        )
        alias_col1, alias_col2, alias_col3 = st.columns(3)
        with alias_col1:
            st.markdown("**Aliases — Aprovado**")
            st.caption("Padrao: approved, confirmed, validated, paid, entregue, completed, success...")
            custom_approved_text = st.text_area(
                "Aliases aprovado",
                value="",
                height=100,
                key="custom_approved",
                label_visibility="collapsed",
                placeholder="meu_alias_1\nmeu_alias_2",
            )
        with alias_col2:
            st.markdown("**Aliases — Pendente**")
            st.caption("Padrao: pending, in_review, hold, aguardando, processando...")
            custom_pending_text = st.text_area(
                "Aliases pendente",
                value="",
                height=100,
                key="custom_pending",
                label_visibility="collapsed",
                placeholder="em_espera",
            )
        with alias_col3:
            st.markdown("**Aliases — Recusado**")
            st.caption("Padrao: declined, rejected, canceled, recusado, cancelado...")
            custom_declined_text = st.text_area(
                "Aliases recusado",
                value="",
                height=100,
                key="custom_declined",
                label_visibility="collapsed",
                placeholder="devolvido",
            )

    # ---- Financial reconciliation expander (pre-run) ----
    with st.expander("Reconciliacao financeira", expanded=False):
        st.caption(
            "Insira os totais pagos pelo anunciante por moeda e escolha a moeda de referencia ativa. "
            "O export final consolidado continua sendo gerado sem equilibrio. "
            "Use a acao pos-validacao para gerar um export equilibrado apenas se necessario."
        )
        fin_col1, fin_col2, fin_col3 = st.columns(3)
        with fin_col1:
            adv_brl = st.number_input(
                "Total pago pelo anunciante (BRL)",
                min_value=0.0, value=0.0, step=0.01, format="%.2f",
                key="fin_adv_brl",
            )
        with fin_col2:
            adv_usd = st.number_input(
                "Total pago pelo anunciante (USD)",
                min_value=0.0, value=0.0, step=0.01, format="%.2f",
                key="fin_adv_usd",
            )
        with fin_col3:
            adv_eur = st.number_input(
                "Total pago pelo anunciante (EUR)",
                min_value=0.0, value=0.0, step=0.01, format="%.2f",
                key="fin_adv_eur",
            )

        active_currency = st.radio(
            "Moeda de referencia ativa para esta execucao",
            options=["BRL", "USD", "EUR"],
            horizontal=True,
            key="fin_active_currency",
        )

        fx_col1, fx_col2 = st.columns(2)
        usd_brl_rate = fx_col1.number_input(
            "Taxa USD para BRL",
            min_value=0.0001, value=5.0, step=0.01, format="%.4f",
            key="fin_usd_brl",
        )
        eur_brl_rate = fx_col2.number_input(
            "Taxa EUR para BRL",
            min_value=0.0001, value=5.5, step=0.01, format="%.4f",
            key="fin_eur_brl",
        )
        balance_floor = st.number_input(
            "Piso minimo por linha no export equilibrado",
            min_value=0.01,
            value=1.00,
            step=0.01,
            format="%.2f",
            key="fin_balance_floor",
        )

        if active_currency == "USD" and adv_usd > 0:
            brl_equiv = adv_usd * usd_brl_rate
            st.caption(f"Equivalente BRL estimado (USD x {usd_brl_rate:.4f}): R$ {brl_equiv:,.2f}")
        elif active_currency == "EUR" and adv_eur > 0:
            brl_equiv = adv_eur * eur_brl_rate
            st.caption(f"Equivalente BRL estimado (EUR x {eur_brl_rate:.4f}): R$ {brl_equiv:,.2f}")
        else:
            st.caption(f"Piso minimo configurado para o export equilibrado: {balance_floor:.2f}")

    # ---- Payout adjustment expander (pre-run) ----
    with st.expander("Ajuste de payout (opcional)", expanded=False):
        st.caption(
            "Gera um arquivo de export separado com valores de payout ajustados ao executar a validacao. "
            "O export original validado nao e alterado."
        )
        adj_mode_label = st.radio(
            "Modo de ajuste",
            options=list(_ADJ_MODE_MAP.keys()),
            key="adj_mode",
        )
        adj_mode = _ADJ_MODE_MAP[adj_mode_label]
        if adj_mode != "none":
            st.number_input(
                _ADJ_LABELS[adj_mode],
                min_value=0.0, value=0.0, step=0.01, format="%.2f",
                key="adj_value",
            )

    with st.expander("Prioridade segura por publisher (opcional)", expanded=False):
        st.caption(
            "Nao altera a verdade da validacao. So influencia ordenacao de revisao, desempate entre linhas ja "
            "elegiveis e a distribuicao positiva do balanceamento quando houver margem real."
        )
        prio_col1, prio_col2 = st.columns(2)
        priority_publisher_id = prio_col1.text_input(
            "Publisher prioritario",
            value="",
            key="priority_publisher_id",
            placeholder="publisher_id",
        ).strip()
        priority_pct = Decimal(
            str(
                prio_col2.number_input(
                    "Percentual de prioridade",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=1.0,
                    format="%.0f",
                    key="priority_pct",
                )
            )
        )

    # ---- Run button ----
    can_run = master_df is not None and advertiser_df is not None
    run_clicked = st.button("Executar validacao", disabled=not can_run, type="primary")

    if run_clicked:
        missing_mapping = [field for field in REQUIRED_MASTER_FIELDS if not mapping.get(field)]
        if missing_mapping:
            st.error(
                "Mapeamento incompleto. Campos faltando: "
                + ", ".join(missing_mapping)
                + ". Execucao cancelada."
            )
        elif len({mapping[field] for field in REQUIRED_MASTER_FIELDS}) != len(REQUIRED_MASTER_FIELDS):
            st.error("Mapeamento invalido: cada campo obrigatorio deve ser mapeado para uma coluna diferente.")
        else:
            status_keywords = build_status_keywords(
                custom_approved=_parse_alias_text(custom_approved_text),
                custom_pending=_parse_alias_text(custom_pending_text),
                custom_declined=_parse_alias_text(custom_declined_text),
            )

            # Capture financial config from widgets at run time
            _adv_paid_map = {
                "BRL": Decimal(str(st.session_state.get("fin_adv_brl", 0.0))),
                "USD": Decimal(str(st.session_state.get("fin_adv_usd", 0.0))),
                "EUR": Decimal(str(st.session_state.get("fin_adv_eur", 0.0))),
            }
            _active_currency = str(st.session_state.get("fin_active_currency", "BRL"))
            _adv_ref = _adv_paid_map[_active_currency]
            _usd_brl = float(st.session_state.get("fin_usd_brl", 5.0))
            _eur_brl = float(st.session_state.get("fin_eur_brl", 5.5))
            _balance_floor = Decimal(str(st.session_state.get("fin_balance_floor", 1.0)))
            _priority_publisher_id = str(st.session_state.get("priority_publisher_id", "")).strip()
            _priority_pct = Decimal(str(st.session_state.get("priority_pct", 0.0)))

            # Capture payout adjustment config from widgets at run time
            _adj_mode_label = str(st.session_state.get("adj_mode", "Sem ajuste"))
            _adj_mode = _ADJ_MODE_MAP.get(_adj_mode_label, "none")
            _adj_value_raw = st.session_state.get("adj_value", 0.0) if _adj_mode != "none" else 0.0
            _adj_value_decimal = Decimal(str(_adj_value_raw))

            try:
                with st.spinner("Processando..."):
                    result = _run_pipeline(
                        master_df,
                        advertiser_df,
                        mapping,
                        allow_txn_only=st.session_state.get("allow_txn_only", True),
                        implicit_approved=st.session_state.get("implicit_approved", False),
                        status_keywords=status_keywords,
                        priority_publisher_id=_priority_publisher_id or None,
                    )

                st.session_state["run_result"] = result
                st.session_state.pop("balanced_result", None)
                st.session_state.pop("adjusted_result", None)
                st.session_state.pop("balanced_error", None)
                st.session_state.pop("adjusted_error", None)

                # Store run config for consistent post-run display
                st.session_state["run_config"] = {
                    "adv_ref": str(_adv_ref),
                    "active_currency": _active_currency,
                    "usd_brl_rate": _usd_brl,
                    "eur_brl_rate": _eur_brl,
                    "balance_floor": str(_balance_floor),
                    "priority_publisher_id": _priority_publisher_id,
                    "priority_pct": str(_priority_pct),
                    "adj_mode": _adj_mode,
                    "adj_mode_label": _adj_mode_label,
                    "adj_value_decimal": str(_adj_value_decimal),
                }

                # Auto-generate adjusted export if a mode and value were configured
                if _adj_mode != "none" and _adj_value_decimal > Decimal("0") and not result["export_internal_df"].empty:
                    try:
                        runtime_export_module = _load_runtime_module("core.export")
                        adj_result = runtime_export_module.persist_payout_adjusted_export(
                            result["export_internal_df"], _adj_mode, _adj_value_decimal, OUTPUTS_DIR
                        )
                        st.session_state["adjusted_result"] = adj_result
                    except Exception as exc:
                        st.session_state["adjusted_error"] = str(exc)

                st.success("Execucao concluida. O export final consolidado foi gerado em outputs/.")
            except Exception as exc:
                st.error(f"Falha na execucao: {exc}")

    result = st.session_state.get("run_result")
    if not result:
        return

    # ================================================================
    # Results section
    # ================================================================

    st.divider()

    # ---- Metric cards ----
    st.subheader("Metricas da execucao")
    metrics = result["metrics"]
    comparison = result["comparison"]
    metric_row_1 = st.columns(5)
    metric_row_1[0].metric("Linhas do anunciante", metrics["total_advertiser_rows"])
    metric_row_1[1].metric("Linhas do MASTER", metrics["master_total_rows"])
    metric_row_1[2].metric("Pendentes no MASTER", metrics["master_pending_count"])
    metric_row_1[3].metric("Declinadas no MASTER", metrics["master_declined_count"])
    metric_row_1[4].metric("Linhas em revisao", metrics["needs_review_count"])

    metric_row_2 = st.columns(5)
    metric_row_2[0].metric("Ja aprovadas no MASTER", metrics["master_already_approved_count"])
    metric_row_2[1].metric("Ja pagas no MASTER (fora desta validacao)", metrics["master_already_paid_count"])
    metric_row_2[2].metric("Novamente aprovadas por match", metrics["newly_approved_count"])
    metric_row_2[3].metric("Linhas exportadas finais", metrics["final_export_count"])
    metric_row_2[4].metric("Excluidas por status paid no MASTER", metrics["excluded_paid_count"])

    metric_row_3 = st.columns(5)
    metric_row_3[0].metric("Match somente por click_id", metrics.get("click_id_only_count", 0))
    metric_row_3[1].metric("Linhas com somente txn_id", metrics["missing_click_for_txn_count"])
    metric_row_3[2].metric("Conflitos click_id x txn_id", metrics["pair_mismatch_count"])
    metric_row_3[3].metric("Casos ambiguos nao resolvidos", metrics.get("ambiguous_unresolved_count", 0))
    metric_row_3[4].metric("Ja aprovadas vistas no anunciante", metrics.get("already_approved_seen_in_advertiser_count", 0))

    st.caption(
        "O export final consolidado = novamente aprovadas por match + ja aprovadas no MASTER. "
        "Linhas `paid` ficam fora do export e fora do balanceamento."
    )

    # ---- Zero-match diagnostic ----
    if metrics["newly_approved_count"] == 0:
        st.error("Nenhuma nova linha foi aprovada por match nesta execucao.")
        with st.expander("Diagnostico: por que nenhuma linha foi aprovada?", expanded=True):
            st.markdown(
                "**Verifique os seguintes pontos:**\n\n"
                "1. **Mapeamento MASTER incorreto** — confirme que `click_id`, `txn_id` e as colunas de "
                "receita estao mapeadas para as colunas corretas do seu arquivo.\n"
                "2. **Arquivo ANUNCIANTE sem coluna de status** — se o arquivo nao tem coluna de status, "
                "ative **'Aprovar implicitamente linhas sem status detectado'** em Regras de correspondencia "
                "e execute novamente.\n"
                "3. **IDs com formato diferente** — verifique se os IDs no arquivo ANUNCIANTE correspondem "
                "exatamente (ou contem) os IDs do MASTER. Maiusculas/minusculas, hifens e espacos importam.\n"
                "4. **Confira o arquivo `match_audit_<ts>.csv`** baixado abaixo — a coluna `issue_codes` "
                "indica o problema especifico de cada linha. A coluna `diagnostic_hint` oferece uma dica "
                "em texto legivel."
            )
            audit_df = result.get("audit_df")
            if audit_df is not None and not audit_df.empty and "issue_codes" in audit_df.columns:
                all_issues: list[str] = []
                for codes_str in audit_df["issue_codes"].dropna():
                    if str(codes_str).strip():
                        all_issues.extend(str(codes_str).split("|"))
                if all_issues:
                    st.markdown("**Resumo dos codigos de problema (todas as linhas do anunciante):**")
                    for code, count in Counter(all_issues).most_common():
                        st.write(f"- `{code}`: {count} linha(s)")

    # ---- Comparison ----
    run_config = st.session_state.get("run_config", {})

    st.subheader("Comparacao de totais")
    comp_left, comp_right = st.columns(2)
    with comp_left:
        st.markdown("**MASTER**")
        st.metric("Total MASTER do export final", comparison["master_revenue_total"])
        st.metric("Total MASTER do arquivo completo", comparison.get("master_revenue_total_file_scope", "0.00"))
        st.metric("Ja aprovadas carregadas no export", comparison.get("carry_forward_rows", 0))
    with comp_right:
        st.markdown("**ANUNCIANTE**")
        st.metric("Comissao do anunciante nas novas aprovacoes por match", comparison["advertiser_total_reference"])
        st.metric("Comissao detectada no arquivo completo", comparison.get("advertiser_total_detected_file_scope", "0.00"))
        st.metric("Novas aprovacoes por match", comparison.get("newly_approved_rows", 0))

    diff_value = parse_decimal(comparison["difference_advertiser_minus_master"]) or Decimal("0")
    diff_str = comparison["difference_advertiser_minus_master"]
    if diff_value > Decimal("0"):
        st.warning(
            f"Diferenca (anunciante - MASTER): **{diff_str}** "
            "O anunciante esta pagando mais do que o total exportado."
        )
    elif diff_value < Decimal("0"):
        st.warning(
            f"Diferenca (anunciante - MASTER): **{diff_str}** "
            "O total MASTER esta acima do total do anunciante."
        )
    else:
        st.success("Totais equilibrados: nao ha diferenca no escopo exportado.")

    st.caption(
        f"O export final considera {metrics['newly_approved_count']} novas aprovacoes por match "
        f"mais {metrics['master_already_approved_count']} linhas ja aprovadas no MASTER. "
        f"As {metrics['excluded_paid_count']} linhas `paid` ficam totalmente fora desta base."
    )

    # ---- Financial reconciliation results (from run-time config) ----
    adv_ref_stored = parse_decimal(run_config.get("adv_ref", "0")) or Decimal("0")
    active_currency_stored = run_config.get("active_currency", "")
    balance_floor_stored = parse_decimal(run_config.get("balance_floor", "1.00")) or Decimal("1.00")
    priority_publisher_stored = str(run_config.get("priority_publisher_id", "")).strip()
    priority_pct_stored = parse_decimal(run_config.get("priority_pct", "0")) or Decimal("0")
    export_df = result["export_df"]
    export_internal_df = result["export_internal_df"]
    if adv_ref_stored > Decimal("0"):
        master_export_total_stored = (
            parse_decimal(comparison["master_revenue_total"]) or Decimal("0")
        )
        fin_diff_stored = adv_ref_stored - master_export_total_stored

        st.subheader("Reconciliacao financeira (referencia da execucao)")
        fin_r1, fin_r2, fin_r3 = st.columns(3)
        fin_r1.metric("Total MASTER exportado", format_money(master_export_total_stored))
        fin_r2.metric(
            f"Pago pelo anunciante ({active_currency_stored})",
            format_money(adv_ref_stored),
        )
        fin_r3.metric("Diferenca (anunciante - MASTER)", format_money(fin_diff_stored))
        st.caption(f"Piso minimo considerado no export equilibrado desta execucao: {format_money(balance_floor_stored)}")
        if priority_publisher_stored and priority_pct_stored > Decimal("0"):
            st.caption(
                f"Prioridade segura ativa para publisher `{priority_publisher_stored}` em {format_money(priority_pct_stored)}%. "
                "Ela nao aprova linhas e so pode influenciar desempates/redistribuicao positiva."
            )

        if fin_diff_stored == Decimal("0"):
            st.success("Totais equilibrados para a moeda selecionada.")
        elif fin_diff_stored > Decimal("0"):
            st.warning(
                f"O anunciante esta pagando {format_money(fin_diff_stored)} "
                "a mais do que o total exportado."
            )
        else:
            st.warning(
                f"O MASTER esta {format_money(abs(fin_diff_stored))} "
                "acima do total pago pelo anunciante."
            )

        st.info("O export final consolidado e gerado sem equilibrio.")
        st.caption(
            "Use 'Gerar export equilibrado' apenas se quiser ajustar o total exportado ao valor pago pelo anunciante."
        )
        balance_disabled = export_internal_df.empty
        if st.button("Gerar export equilibrado", key="generate_balanced_export_btn", disabled=balance_disabled):
            try:
                runtime_export_module = _load_runtime_module("core.export")
                balanced = runtime_export_module.persist_balanced_export(
                    export_internal_df,
                    fin_diff_stored,
                    OUTPUTS_DIR,
                    floor=balance_floor_stored,
                    priority_publisher_id=priority_publisher_stored or None,
                    priority_pct=priority_pct_stored,
                )
                st.session_state["balanced_result"] = balanced
                st.session_state.pop("balanced_error", None)
            except Exception as exc:
                st.session_state["balanced_error"] = str(exc)
    else:
        st.info("O export final consolidado e gerado sem equilibrio.")
        st.caption(
            "Se quiser gerar um export equilibrado depois, primeiro informe o total pago pelo anunciante na reconciliacao financeira."
        )

    # ---- Balanced export result (explicit action) ----
    balanced_error = st.session_state.get("balanced_error")
    if balanced_error:
        st.error(f"Falha ao gerar export equilibrado: {balanced_error}")

    balanced_result = st.session_state.get("balanced_result")
    if balanced_result:
        with st.expander("Export equilibrado (gerado apos acao do operador)", expanded=True):
            st.caption(
                f"Total alvo: {balanced_result['target_total']} | "
                f"Total alcancado: {balanced_result['actual_total']} | "
                f"Delta medio por conversao: {balanced_result['average_delta']} | "
                f"Piso minimo: {balanced_result['floor']} | "
                f"Arquivo: {Path(balanced_result['path']).name}"
            )
            if balanced_result.get("priority_publisher_id") and parse_decimal(balanced_result.get("priority_pct", "0")):
                st.caption(
                    f"Prioridade segura aplicada ao publisher `{balanced_result['priority_publisher_id']}` "
                    f"em {balanced_result['priority_pct']}% apenas na redistribuicao positiva elegivel."
                )
            if not balanced_result.get("exact_reached", True):
                st.warning(
                    "Nao foi possivel atingir exatamente o total pago pelo anunciante sem quebrar o piso minimo "
                    f"de {balanced_result['floor']}. O arquivo foi gerado com o total mais proximo possivel: "
                    f"{balanced_result['actual_total']}."
                )
            _render_preview_caption(int(balanced_result.get("row_count", len(balanced_result["df"]))), 50)
            st.dataframe(balanced_result["df"].head(50), use_container_width=True)
            st.download_button(
                label="Baixar export equilibrado (CSV)",
                data=_read_file_bytes(balanced_result["path"]),
                file_name=Path(balanced_result["path"]).name,
                mime="text/csv",
            )

    # ---- Adjusted export result (auto-generated during run) ----
    adjusted_error = st.session_state.get("adjusted_error")
    if adjusted_error:
        st.error(f"Falha ao gerar export ajustado: {adjusted_error}")

    adjusted_result = st.session_state.get("adjusted_result")
    if adjusted_result:
        with st.expander("Export com payout ajustado (gerado automaticamente)", expanded=True):
            st.caption(
                f"Modo: {run_config.get('adj_mode_label', '')} | "
                f"Total alvo: {adjusted_result['target_total']} | "
                f"Delta medio: {adjusted_result['average_delta']} | "
                f"Arquivo: {Path(adjusted_result['path']).name}"
            )
            if not adjusted_result.get("exact_reached", True):
                st.warning(
                    "O total alvo do ajuste nao pode ser atingido exatamente sem quebrar o piso minimo por linha. "
                    "O arquivo foi salvo com o total mais proximo possivel permitido."
                )
            _render_preview_caption(len(adjusted_result["df"]), 50)
            st.dataframe(adjusted_result["df"].head(50), use_container_width=True)
            st.download_button(
                label="Baixar export ajustado (CSV)",
                data=_read_file_bytes(adjusted_result["path"]),
                file_name=Path(adjusted_result["path"]).name,
                mime="text/csv",
            )

    # ---- Preview tables ----
    st.subheader("Pre-visualizacao dos resultados")
    prev_col1, prev_col2 = st.columns(2)
    with prev_col1:
        export_df = result["export_df"]
        st.markdown(f"**Export final consolidado** ({len(export_df)} linhas)")
        _render_preview_caption(len(export_df), 30)
        st.dataframe(export_df.head(30), use_container_width=True, height=300)
    with prev_col2:
        needs_df = result["needs_review_df"]
        st.markdown(f"**Linhas em revisao** ({len(needs_df)} linhas)")
        if needs_df.empty:
            st.info("Nenhuma linha para revisao.")
        else:
            _render_preview_caption(len(needs_df), 30)
            st.dataframe(needs_df.head(30), use_container_width=True, height=300)

    with st.expander("Pre-visualizacao do audit de correspondencia", expanded=False):
        audit_df = result.get("audit_df")
        if audit_df is not None and not audit_df.empty:
            _render_preview_caption(len(audit_df), 50)
            st.dataframe(audit_df.head(50), use_container_width=True, height=400)
        else:
            st.info("Nenhum dado de auditoria disponivel.")

    # ---- Downloads ----
    st.subheader("Downloads")
    paths = result["paths"]
    balanced_result = st.session_state.get("balanced_result")
    dl_col1, dl_col2, dl_col3 = st.columns(3)
    with dl_col1:
        st.download_button(
            label="Export final consolidado (sem equilibrio) (CSV)",
            data=_read_file_bytes(paths["export"]),
            file_name=Path(paths["export"]).name,
            mime="text/csv",
        )
        if balanced_result:
            st.download_button(
                label="Export equilibrado (CSV)",
                data=_read_file_bytes(balanced_result["path"]),
                file_name=Path(balanced_result["path"]).name,
                mime="text/csv",
            )
        st.download_button(
            label="Linhas em revisao (CSV)",
            data=_read_file_bytes(paths["needs_review"]),
            file_name=Path(paths["needs_review"]).name,
            mime="text/csv",
        )
    with dl_col2:
        st.download_button(
            label="Diff de status (CSV)",
            data=_read_file_bytes(paths["diff"]),
            file_name=Path(paths["diff"]).name,
            mime="text/csv",
        )
        st.download_button(
            label="Audit de correspondencia (CSV)",
            data=_read_file_bytes(paths["audit"]),
            file_name=Path(paths["audit"]).name,
            mime="text/csv",
        )
    with dl_col3:
        st.download_button(
            label="Log TXT",
            data=_read_file_bytes(paths["log_txt"]),
            file_name=Path(paths["log_txt"]).name,
            mime="text/plain",
        )
        st.download_button(
            label="Log JSON",
            data=_read_file_bytes(paths["log_json"]),
            file_name=Path(paths["log_json"]).name,
            mime="application/json",
        )

    st.caption(
        "Arquivos gerados em: "
        + ", ".join([paths["export"], paths["diff"], paths["needs_review"], paths["audit"]])
    )


# ---------------------------------------------------------------------------
# Clicks tab
# ---------------------------------------------------------------------------

def _render_clicks_tab() -> None:
    st.caption(
        "Rastreia o caminho de redirecionamento de URLs e extrai o click_id do parametro configurado. "
        "Ideal para verificar links de afiliados e confirmar que o rastreamento esta ativo."
    )

    # ---- Primary inputs ----
    urls_text = st.text_area(
        "URLs para verificar (uma por linha)",
        height=200,
        placeholder="https://exemplo.com/clique1\nhttps://exemplo.com/clique2",
        key="clicks_urls",
    )

    primary_col1, primary_col2 = st.columns(2)
    with primary_col1:
        click_id_param = st.text_input(
            "Parametro para extrair click_id",
            value="s2s.req_id",
            placeholder="s2s.req_id",
            key="clicks_param",
        ).strip()
    with primary_col2:
        repeat_per_url = int(
            st.number_input(
                "Repeticoes por URL (max 100 no total)",
                min_value=1,
                max_value=100,
                value=1,
                step=1,
                key="clicks_repeat",
            )
        )

    ua_preset_name = st.selectbox(
        "User-Agent",
        options=list(USER_AGENT_PRESETS.keys()),
        index=0,
        key="clicks_ua_preset",
    )
    user_agent = USER_AGENT_PRESETS[ua_preset_name]

    # ---- Advanced options ----
    with st.expander("Opcoes avancadas", expanded=False):
        adv_col1, adv_col2 = st.columns(2)
        max_hops = int(
            adv_col1.number_input(
                "Max hops", min_value=1, max_value=50, value=15, step=1, key="clicks_max_hops"
            )
        )
        timeout_seconds = int(
            adv_col2.number_input(
                "Timeout (s)", min_value=5, max_value=120, value=20, step=1, key="clicks_timeout"
            )
        )
        proxy_url = st.text_input(
            "Proxy (opcional)",
            value="",
            placeholder="socks5h://usuario:senha@host:porta",
            key="clicks_proxy",
        ).strip()
        run_ip_check = st.checkbox("Verificar IP publico antes de executar", value=True, key="clicks_ip_check")

    raw_url_count = len([ln for ln in (urls_text or "").splitlines() if ln.strip()])
    if raw_url_count > 0:
        st.caption(
            f"{raw_url_count} URL(s) x {repeat_per_url} repeticao(oes) = "
            f"{min(raw_url_count * repeat_per_url, 100)} requisicoes (cap: 100)"
        )

    if st.button("Executar verificacao", key="clicks_run_btn", type="primary"):
        raw_urls = [ln.strip() for ln in (urls_text or "").splitlines() if ln.strip()]
        if not raw_urls:
            st.error("Informe ao menos uma URL.")
            return
        if not click_id_param:
            st.error("Informe o nome do parametro para extrair o click_id.")
            return

        if run_ip_check:
            with st.spinner("Verificando IP publico..."):
                ip_result = check_public_ip(
                    proxy_url=proxy_url or None, timeout_seconds=timeout_seconds
                )
            st.session_state["clicks_ip_result"] = ip_result

        with st.spinner("Verificando redirecionamentos..."):
            results_df = run_click_checker(
                raw_urls=raw_urls,
                proxy_url=proxy_url or None,
                timeout_seconds=timeout_seconds,
                max_hops=max_hops,
                click_id_param=click_id_param,
                repeat_per_url=repeat_per_url,
                user_agent=user_agent,
            )
        st.session_state["clicks_results_df"] = results_df

    # IP result
    ip_result = st.session_state.get("clicks_ip_result")
    if ip_result:
        if ip_result.get("ok"):
            st.info(f"IP visivel nesta execucao: {ip_result['ip']} ({ip_result['service']})")
        else:
            st.warning(f"Nao foi possivel verificar o IP: {ip_result.get('error', 'erro desconhecido')}")

    results_df = st.session_state.get("clicks_results_df")
    if results_df is None:
        return

    st.subheader("Resultados")
    st.dataframe(results_df, use_container_width=True, height=420, hide_index=True)

    if "click_id" in results_df.columns:
        click_ids = [str(v).strip() for v in results_df["click_id"].tolist() if str(v).strip()]
        if click_ids:
            _render_copy_text_button(
                label="Copiar coluna click_id",
                text_to_copy="\n".join(click_ids),
                key="copy_clicks_click_id",
            )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="Baixar resultado (CSV)",
        data=_df_to_csv_bytes(results_df),
        file_name=f"verificacao_redirecionamentos_{ts}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# ID tab
# ---------------------------------------------------------------------------

def _render_id_tab() -> None:
    st.caption("Gera IDs semelhantes com base no padrao dos exemplos informados.")
    examples_text = st.text_area(
        "Cole os IDs de exemplo (um por linha)",
        height=220,
        placeholder="48b7fa0f-8b47-43a8-bd65-f579d707633d\n8208e6de-331f-4416-9f19-608d573d2725",
        key="id_mode_examples",
    )
    total_ids = int(
        st.number_input(
            "Quantidade a gerar",
            min_value=1,
            max_value=1000,
            value=100,
            step=1,
            key="id_mode_count",
        )
    )

    if st.button("Gerar IDs", key="run_id_generator", type="primary"):
        raw_examples = [ln.strip() for ln in (examples_text or "").splitlines() if ln.strip()]
        if not raw_examples:
            st.error("Informe ao menos um ID de exemplo.")
            return
        result = generate_similar_ids(raw_examples, total_ids=total_ids)
        st.session_state["id_mode_result"] = result

    result = st.session_state.get("id_mode_result")
    if not result:
        return
    if not result.get("ok"):
        st.error(result.get("message", "Falha ao gerar IDs."))
        return

    st.success(f"IDs gerados: {result['generated_count']} de {result['requested_count']}.")
    st.caption(f"Mascara detectada: `{result['mask']}` | Tamanho: `{result['length']}`")

    warning = str(result.get("warning", "")).strip()
    if warning:
        st.warning(warning)

    generated_df = result["df"]
    st.subheader("Pre-visualizacao")
    st.dataframe(generated_df, use_container_width=True, height=420, hide_index=True)

    generated_ids = [str(v).strip() for v in generated_df["generated_id"].tolist() if str(v).strip()]
    if generated_ids:
        _render_copy_text_button(
            label="Copiar coluna generated_id",
            text_to_copy="\n".join(generated_ids),
            key="copy_id_mode_generated_id",
        )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="Baixar IDs gerados (CSV)",
        data=_df_to_csv_bytes(generated_df),
        file_name=f"ids_gerados_{ts}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Postback tab
# ---------------------------------------------------------------------------

def _render_postback_tab() -> None:
    st.caption(
        "Preenche templates de postback linha a linha com valores da mesma linha. "
        "Voce pode usar upload de planilha ou montar os dados manualmente na grade editavel."
    )

    st.markdown("**Modo de entrada**")
    input_mode_label = st.radio(
        "Modo de entrada",
        options=["Upload de planilha", "Editar/colar dados manualmente"],
        key="postback_input_mode",
        horizontal=True,
        label_visibility="collapsed",
    )

    input_mode = "file" if input_mode_label == "Upload de planilha" else "manual"
    source_df: pd.DataFrame | None = None
    context_signature: dict[str, Any] = {"input_mode": input_mode}

    if input_mode == "file":
        st.markdown("**Upload de planilha**")
        postback_upload = st.file_uploader(
            "Planilha para modo postback",
            type=["csv", "xlsx", "xlsm", "xls"],
            key="postback_file",
            label_visibility="collapsed",
        )

        if postback_upload is None:
            st.info("Envie uma planilha para continuar no Modo Postback.")
            return

        try:
            source_df = read_table(postback_upload)
        except Exception as exc:
            st.error(f"Falha ao ler a planilha: {exc}")
            return

        st.caption(
            f"Pre-visualizacao da planilha - {len(source_df)} linhas, {len(source_df.columns)} colunas"
        )
        st.dataframe(source_df.head(20), use_container_width=True, height=260)
        context_signature.update(
            {
                "file_name": getattr(postback_upload, "name", ""),
                "file_size": getattr(postback_upload, "size", 0),
                "source_signature": _build_dataframe_signature(source_df),
            }
        )
    else:
        st.markdown("**Montagem manual**")
        st.caption(
            "Use a grade editavel abaixo para digitar, editar celulas ou colar linhas diretamente da area de "
            "transferencia. Linhas totalmente vazias serao ignoradas na geracao."
        )
        if "postback_manual_seed_df" not in st.session_state:
            st.session_state["postback_manual_seed_df"] = _build_postback_manual_dataframe()
        manual_df = st.data_editor(
            st.session_state["postback_manual_seed_df"],
            key="postback_manual_editor",
            num_rows="dynamic",
            hide_index=True,
            use_container_width=True,
            height=320,
            column_config={
                "VALOR": st.column_config.TextColumn("VALOR"),
                "CLICK": st.column_config.TextColumn("CLICK"),
                "TRANSACTION": st.column_config.TextColumn("TRANSACTION"),
                "POSTBACK": st.column_config.TextColumn("POSTBACK"),
            },
        )
        st.caption("Grade editavel")
        source_df = _prepare_manual_postback_dataframe(manual_df)
        if source_df.empty:
            st.info("Preencha ao menos uma linha na grade editavel para gerar postbacks.")
        else:
            st.caption(f"{len(source_df)} linha(s) preenchida(s) serao processadas.")
        context_signature["source_signature"] = _build_dataframe_signature(source_df)

    st.markdown("**Modo de template**")
    template_mode_label = st.radio(
        "Modo de template",
        options=[
            "Usar coluna POSTBACK dos dados",
            "Usar template unico colado manualmente",
        ],
        key="postback_template_mode",
    )
    template_mode = "row" if template_mode_label == "Usar coluna POSTBACK dos dados" else "single"

    detected_template_column = detect_postback_template_column(source_df.columns.tolist())
    template_column = ""
    shared_template = ""

    if template_mode == "row":
        if input_mode == "manual":
            template_column = "POSTBACK"
            st.caption(
                "Cada linha da grade usara a coluna POSTBACK como template e preenchera os placeholders com os "
                "dados da mesma linha."
            )
        else:
            if detected_template_column is None:
                st.warning(
                    "Nenhuma coluna POSTBACK foi detectada automaticamente. "
                    "Selecione manualmente a coluna que contem o template por linha."
                )
            template_column = st.selectbox(
                "Coluna com o template de postback",
                options=source_df.columns.tolist(),
                index=source_df.columns.tolist().index(detected_template_column)
                if detected_template_column in source_df.columns
                else 0,
                key="postback_template_column",
            )
            st.caption(
                "Cada linha usara o valor dessa coluna como template e preenchera os placeholders com os dados "
                "da mesma linha."
            )
    else:
        shared_template = st.text_area(
            "Template unico",
            height=160,
            placeholder=(
                "https://exemplo.com/postback?click_id=CLICK&rate=VALOR&txn_id=TRANSACTION"
            ),
            key="postback_single_template",
        )
        st.caption(
            "O mesmo template sera aplicado a todas as linhas. Cada linha recebera seus proprios valores."
        )

    output_suffix = st.text_input(
        "Sufixo do arquivo de saida (opcional)",
        value="postback_preenchido",
        key="postback_output_suffix",
        help="O arquivo baixado sera gerado em CSV com timestamp.",
    ).strip()

    context_signature.update(
        {
            "mode": template_mode,
            "template_column": template_column,
            "shared_template": shared_template,
            "output_suffix": output_suffix,
        }
    )
    if st.session_state.get("postback_mode_signature") != context_signature:
        st.session_state["postback_mode_signature"] = context_signature
        st.session_state.pop("postback_mode_result", None)
        st.session_state.pop("postback_mode_error", None)

    st.markdown("**Gerar postbacks**")
    if st.button("Gerar postbacks", key="postback_generate_btn", type="primary"):
        if source_df is None or source_df.empty:
            st.error("Preencha ao menos uma linha valida ou envie uma planilha para gerar postbacks.")
        elif template_mode == "row" and not template_column:
            st.error("Selecione a coluna de template da planilha.")
        elif template_mode == "single" and not shared_template.strip():
            st.error("Cole um template unico para aplicar nas linhas.")
        else:
            try:
                runtime_postback_module = _load_runtime_module("core.postback_mode")
                result = runtime_postback_module.process_postback_dataframe(
                    source_df,
                    template_mode=template_mode,
                    template_column=template_column or None,
                    shared_template=shared_template,
                )
                file_suffix = _sanitize_filename_suffix(output_suffix, "postback_preenchido")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                result["download_name"] = f"{file_suffix}_{timestamp}.csv"
                st.session_state["postback_mode_result"] = result
                st.session_state.pop("postback_mode_error", None)
            except Exception as exc:
                st.session_state["postback_mode_error"] = str(exc)

    postback_error = st.session_state.get("postback_mode_error")
    if postback_error:
        st.error(f"Falha ao gerar postbacks: {postback_error}")

    postback_result = st.session_state.get("postback_mode_result")
    if not postback_result:
        return

    result_df = postback_result["df"]
    stats = postback_result["stats"]

    st.markdown("**Pre-visualizacao**")
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Total de linhas processadas", stats["total_rows"])
    metric_col2.metric("Linhas OK", stats["ok_rows"])
    metric_col3.metric("Linhas com aviso", stats["warning_rows"])
    metric_col4.metric("Templates vazios", stats["template_empty_rows"])

    if stats["missing_column_rows"] > 0:
        st.warning(
            f"{stats['missing_column_rows']} linha(s) ficaram com placeholder sem coluna correspondente. "
            "O token original foi preservado."
        )
    if stats["error_rows"] > 0:
        st.warning(f"{stats['error_rows']} linha(s) tiveram erro de processamento.")

    _render_preview_caption(len(result_df), 30)
    st.dataframe(result_df.head(30), use_container_width=True, height=360)

    generated_postbacks = postback_result.get("generated_postbacks", [])
    if generated_postbacks:
        all_postbacks_text = "\n".join(generated_postbacks)
        _render_copyable_text_block(
            title="Copiar coluna POSTBACK_FINAL",
            helper_text="Cada linha abaixo corresponde a um postback final. Use para colar em outra planilha.",
            text_to_copy=all_postbacks_text,
            button_label="Copiar coluna POSTBACK_FINAL",
            key="postback_generated_text",
            height=200,
        )
    else:
        st.info("Nenhum valor de POSTBACK_FINAL foi gerado para copia.")

    st.markdown("**Baixar arquivo processado**")
    st.download_button(
        label="Baixar arquivo processado (CSV)",
        data=_df_to_csv_bytes(result_df),
        file_name=postback_result["download_name"],
        mime="text/csv",
    )


def _render_commission_tab() -> None:
    st.caption(
        "Gera uma lista de valores de comissao a partir de um valor total e da quantidade de linhas desejada."
    )

    input_col1, input_col2 = st.columns(2)
    with input_col1:
        total_amount = Decimal(
            str(
                st.number_input(
                    "Valor total",
                    min_value=0.0,
                    value=1000.00,
                    step=0.01,
                    format="%.2f",
                    key="commission_total_amount",
                )
            )
        )
    with input_col2:
        quantity = int(
            st.number_input(
                "Quantidade de linhas",
                min_value=1,
                value=10,
                step=1,
                key="commission_quantity",
            )
        )

    mode_label = st.radio(
        "Modo de geracao",
        options=["Exato", "Media"],
        horizontal=True,
        key="commission_mode",
    )
    generation_mode = "exact" if mode_label == "Exato" else "average"

    options_col1, options_col2 = st.columns(2)
    with options_col1:
        min_value_floor = Decimal(
            str(
                st.number_input(
                    "Valor minimo por linha",
                    min_value=0.01,
                    value=1.00,
                    step=0.01,
                    format="%.2f",
                    key="commission_min_value_floor",
                )
            )
        )
    with options_col2:
        seed_base = int(
            st.number_input(
                "Seed da variacao",
                min_value=0,
                value=20260309,
                step=1,
                key="commission_seed_base",
            )
        )

    st.caption(
        "Piso padrao sugerido: 1.00 por linha. Se o total nao comportar esse piso para a quantidade "
        "informada, o app avisa e gera o menor total valido possivel."
    )

    generate_col1, generate_col2 = st.columns(2)
    generate_clicked = generate_col1.button("Gerar valores", key="commission_generate_btn", type="primary")
    new_variation_clicked = generate_col2.button(
        "Gerar nova variacao",
        key="commission_variation_btn",
        disabled=generation_mode != "average",
    )
    if new_variation_clicked:
        st.session_state["commission_variation_nonce"] = int(
            st.session_state.get("commission_variation_nonce", 0)
        ) + 1
        generate_clicked = True

    variation_nonce = int(st.session_state.get("commission_variation_nonce", 0))
    effective_seed = seed_base + variation_nonce

    if generation_mode == "average":
        st.caption(f"Seed efetiva da variacao atual: {effective_seed}")
    else:
        st.caption("No modo Exato, o resultado depende apenas do total, da quantidade e das casas decimais.")

    context_signature = {
        "total_amount": str(total_amount),
        "quantity": quantity,
        "generation_mode": generation_mode,
        "min_value_floor": str(min_value_floor),
        "effective_seed": effective_seed,
    }
    if st.session_state.get("commission_mode_signature") != context_signature:
        st.session_state["commission_mode_signature"] = context_signature
        st.session_state.pop("commission_mode_result", None)
        st.session_state.pop("commission_mode_error", None)

    if generate_clicked:
        try:
            runtime_commission_module = _load_runtime_module("core.commission_mode")
            result = runtime_commission_module.generate_commission_values(
                total_amount=total_amount,
                quantity=quantity,
                mode=generation_mode,
                min_value=min_value_floor,
                seed=effective_seed if generation_mode == "average" else None,
            )
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result["download_name"] = f"comissao_gerada_{timestamp}.csv"
            st.session_state["commission_mode_result"] = result
            st.session_state.pop("commission_mode_error", None)
        except Exception as exc:
            st.session_state["commission_mode_error"] = str(exc)

    commission_error = st.session_state.get("commission_mode_error")
    if commission_error:
        st.error(f"Falha ao gerar valores de comissao: {commission_error}")

    commission_result = st.session_state.get("commission_mode_result")
    if not commission_result:
        return

    if commission_result.get("warning"):
        st.warning(commission_result["warning"])

    result_df = commission_result["df"]
    st.markdown("**Pre-visualizacao**")
    metric_col1, metric_col2, metric_col3 = st.columns(3)
    metric_col1.metric("Total gerado", commission_result["generated_total"])
    metric_col2.metric("Quantidade gerada", commission_result["quantity"])
    metric_col3.metric("Diferenca vs alvo", commission_result["difference"])

    if generation_mode == "average":
        st.caption(f"Seed utilizada nesta geracao: {commission_result.get('seed')}")
    st.caption(f"Valor minimo por linha considerado: {commission_result['min_value_floor']}")

    _render_preview_caption(len(result_df), 50)
    st.dataframe(result_df.head(50), use_container_width=True, height=360, hide_index=True)

    values_block = "\n".join(commission_result["values"])
    _render_copyable_text_block(
        title="Copiar valores",
        helper_text="Cada linha abaixo corresponde a um valor gerado. Use para colar em outra planilha.",
        text_to_copy=values_block,
        button_label="Copiar valores",
        key="commission_values_copy",
        height=220,
    )

    st.download_button(
        label="Baixar CSV",
        data=_df_to_csv_bytes(result_df),
        file_name=commission_result["download_name"],
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="Ferramenta de Validacao de Pagamentos", layout="wide")
    st.title("Ferramenta de Validacao de Pagamentos")

    tab_validacao, tab_cliques, tab_id, tab_postback, tab_comissao = st.tabs(
        ["Validacao", "Modo Cliques", "Modo ID", "Modo Postback", "Modo Comissao"]
    )

    with tab_validacao:
        _render_validation_tab()

    with tab_cliques:
        _render_clicks_tab()

    with tab_id:
        _render_id_tab()

    with tab_postback:
        _render_postback_tab()

    with tab_comissao:
        _render_commission_tab()


if __name__ == "__main__":
    main()
