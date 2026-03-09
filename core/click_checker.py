from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlparse

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# User-Agent presets
# ---------------------------------------------------------------------------

USER_AGENT_PRESETS: dict[str, str] = {
    "Chrome Desktop (padrão)": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Opera Desktop": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 OPR/110.0.0.0"
    ),
    "Chrome Android": (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Mobile Safari/537.36"
    ),
    "Chrome iPhone": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "CriOS/124.0.0.0 Mobile/15E148 Safari/604.1"
    ),
}

DEFAULT_USER_AGENT = USER_AGENT_PRESETS["Chrome Desktop (padrão)"]

# Safety cap: maximum total requests per run across all URLs and repeats.
MAX_TOTAL_REQUESTS_PER_RUN = 100


@dataclass
class RedirectHop:
    request_index: int
    hop_index: int
    request_url: str
    status_code: int
    location: str
    next_url: str


def _normalize_input_url(raw_url: str) -> str:
    value = str(raw_url or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme:
        return value
    return f"http://{value}"


def _build_session(proxy_url: str | None = None, user_agent: str | None = None) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    ua = user_agent or DEFAULT_USER_AGENT
    session.headers.update({"User-Agent": ua})
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
    return session


def _extract_query_param_value(url: str, parameter_name: str) -> str:
    if not url or not parameter_name:
        return ""
    query = urlparse(url).query
    if not query:
        return ""
    values = parse_qs(query, keep_blank_values=True)
    matched = values.get(parameter_name)
    if not matched:
        return ""
    return str(matched[0]).strip()


def _build_redirect_chain(hops: list[RedirectHop], fallback_url: str) -> str:
    if not hops:
        return fallback_url or ""
    urls: list[str] = []
    for hop in hops:
        if hop.request_url:
            urls.append(hop.request_url)
        if hop.next_url:
            urls.append(hop.next_url)
    deduplicated: list[str] = []
    for url in urls:
        if not deduplicated or deduplicated[-1] != url:
            deduplicated.append(url)
    return " -> ".join(deduplicated)


def check_public_ip(proxy_url: str | None = None, timeout_seconds: int = 20) -> dict[str, Any]:
    session = _build_session(proxy_url)
    services = [
        "https://api.ipify.org?format=json",
        "https://ifconfig.me/ip",
    ]
    last_error = "unknown_error"
    for service_url in services:
        try:
            response = session.get(service_url, timeout=timeout_seconds)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()
            if "application/json" in content_type:
                payload = response.json()
                ip = str(payload.get("ip", "")).strip()
            else:
                ip = response.text.strip()
            if ip:
                return {"ok": True, "ip": ip, "service": service_url, "error": ""}
        except requests.RequestException as exc:
            last_error = str(exc)
    return {"ok": False, "ip": "", "service": "", "error": last_error}


def _trace_one(
    raw_url: str,
    seq: int,
    proxy_url: str | None,
    timeout_seconds: int,
    max_hops: int,
    click_id_param: str,
    user_agent: str,
) -> dict[str, Any]:
    """Trace redirects for a single URL and return a result row."""
    normalized_url = _normalize_input_url(raw_url)
    if not normalized_url:
        return {
            "seq": seq,
            "source_url": raw_url,
            "final_url": "",
            "click_id": "",
            "redirect_chain": "",
            "final_status": "",
            "error": "url_vazia",
            "user_agent_used": user_agent,
        }

    session = _build_session(proxy_url, user_agent)
    hops: list[RedirectHop] = []
    visited_urls: set[str] = set()
    current_url = normalized_url
    last_status = 0
    loop_detected = False
    error_message = ""

    for hop_index in range(0, max_hops + 1):
        if current_url in visited_urls:
            loop_detected = True
            error_message = "loop_detectado"
            break
        visited_urls.add(current_url)
        try:
            response = session.get(current_url, allow_redirects=False, timeout=timeout_seconds)
        except requests.RequestException as exc:
            error_message = str(exc)
            break

        status_code = int(response.status_code)
        last_status = status_code
        location = str(response.headers.get("Location", "")).strip()
        next_url = urljoin(current_url, location) if location else ""
        hops.append(
            RedirectHop(
                request_index=seq,
                hop_index=hop_index,
                request_url=current_url,
                status_code=status_code,
                location=location,
                next_url=next_url,
            )
        )
        response.close()

        if 300 <= status_code < 400 and location:
            current_url = next_url
            continue
        break

    if len(hops) > max_hops and not error_message:
        error_message = "max_hops_excedido"

    final_url = current_url if hops else normalized_url
    redirect_chain = _build_redirect_chain(hops, final_url)

    # Extract click_id from any URL in the chain
    click_id_value = ""
    all_urls = []
    for hop in hops:
        if hop.request_url:
            all_urls.append(hop.request_url)
        if hop.next_url:
            all_urls.append(hop.next_url)
    for url in all_urls:
        val = _extract_query_param_value(url, click_id_param)
        if val:
            click_id_value = val
            break

    return {
        "seq": seq,
        "source_url": raw_url,
        "final_url": final_url,
        "click_id": click_id_value,
        "redirect_chain": redirect_chain,
        "final_status": last_status if last_status else "",
        "error": error_message,
        "user_agent_used": user_agent,
    }


def run_click_checker(
    raw_urls: list[str],
    proxy_url: str | None = None,
    timeout_seconds: int = 20,
    max_hops: int = 15,
    click_id_param: str = "s2s.req_id",
    repeat_per_url: int = 1,
    user_agent: str | None = None,
) -> pd.DataFrame:
    """
    Trace redirects for each URL, optionally repeating each URL up to repeat_per_url times.

    Total requests are capped at MAX_TOTAL_REQUESTS_PER_RUN (100).
    Returns one row per execution with: seq, source_url, final_url, click_id,
    redirect_chain, final_status, error, user_agent_used.
    """
    ua = user_agent or DEFAULT_USER_AGENT
    safe_repeat = max(1, min(int(repeat_per_url), MAX_TOTAL_REQUESTS_PER_RUN))
    normalized_param = str(click_id_param or "").strip()

    rows: list[dict[str, Any]] = []
    seq = 1
    for raw_url in raw_urls:
        for _ in range(safe_repeat):
            if seq > MAX_TOTAL_REQUESTS_PER_RUN:
                break
            row = _trace_one(
                raw_url=raw_url,
                seq=seq,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                max_hops=max_hops,
                click_id_param=normalized_param,
                user_agent=ua,
            )
            rows.append(row)
            seq += 1
        if seq > MAX_TOTAL_REQUESTS_PER_RUN:
            break

    return pd.DataFrame(
        rows,
        columns=[
            "seq",
            "source_url",
            "final_url",
            "click_id",
            "redirect_chain",
            "final_status",
            "error",
            "user_agent_used",
        ],
    )


# ---------------------------------------------------------------------------
# QA clicks (kept for backward compatibility; not exposed in the new UI)
# ---------------------------------------------------------------------------

QA_MAX_CLICKS_PER_RUN = 10
QA_MIN_INTERVAL_SECONDS = 2.0


def _append_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urlparse(url)
    current = parse_qsl(parsed.query, keep_blank_values=True)
    cleaned = [(key, value) for key, value in current if key not in params]
    for key, value in params.items():
        cleaned.append((key, value))
    new_query = urlencode(cleaned, doseq=True)
    return parsed._replace(query=new_query).geturl()


def run_qa_clicks(
    raw_urls: list[str],
    total_clicks: int = 5,
    interval_seconds: float = 5.0,
    proxy_url: str | None = None,
    timeout_seconds: int = 20,
    qa_tag_param: str = "qa_test",
    qa_tag_value: str = "true",
) -> pd.DataFrame:
    """Legacy QA click mode. Kept for backward compatibility."""
    normalized_urls = [_normalize_input_url(url) for url in raw_urls if str(url or "").strip()]
    normalized_urls = [url for url in normalized_urls if url]
    if not normalized_urls:
        return pd.DataFrame(columns=["click_seq", "request_url", "status_code", "clicked_at_utc", "error"])

    safe_total_clicks = max(1, min(int(total_clicks), QA_MAX_CLICKS_PER_RUN))
    safe_interval_seconds = max(float(interval_seconds), QA_MIN_INTERVAL_SECONDS)
    safe_param = str(qa_tag_param or "").strip() or "qa_test"
    safe_value = str(qa_tag_value or "").strip() or "true"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    session = _build_session(proxy_url)
    rows: list[dict[str, Any]] = []
    try:
        for idx in range(safe_total_clicks):
            base_url = normalized_urls[idx % len(normalized_urls)]
            request_url = _append_query_params(
                base_url,
                {safe_param: safe_value, "qa_run_id": run_id, "qa_seq": str(idx + 1)},
            )
            status_code: int | str = ""
            error_message = ""
            try:
                response = session.get(request_url, allow_redirects=False, timeout=timeout_seconds)
                status_code = int(response.status_code)
                response.close()
            except requests.RequestException as exc:
                error_message = str(exc)

            rows.append({
                "click_seq": idx + 1,
                "request_url": request_url,
                "status_code": status_code,
                "clicked_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "error": error_message,
            })
            if idx < safe_total_clicks - 1:
                time.sleep(safe_interval_seconds)
    finally:
        session.close()

    return pd.DataFrame(rows, columns=["click_seq", "request_url", "status_code", "clicked_at_utc", "error"])
