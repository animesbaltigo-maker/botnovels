from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from config import (
    CAKTO_PLAN_BRONZE_URL,
    CAKTO_PLAN_DIAMANTE_URL,
    CAKTO_PLAN_OURO_URL,
    CAKTO_PLAN_RUBI_URL,
)
from services.offline_access import (
    get_offline_access,
    grant_offline_access,
    normalize_plan,
    plan_label,
    revoke_offline_access,
)

APPROVAL_EVENTS = {
    "purchase_approved",
    "subscription_renewed",
}

CONDITIONAL_APPROVAL_EVENTS = {
    "subscription_created",
}

REVOCATION_EVENTS = {
    "refund",
    "chargeback",
    "subscription_canceled",
    "subscription_renewal_refused",
}

IGNORED_EVENTS = {
    "test",
    "initiate_checkout",
    "checkout_abandonment",
    "purchase_refused",
    "pix_gerado",
    "boleto_gerado",
    "picpay_gerado",
    "openfinance_nubank_gerado",
}

APPROVED_STATUSES = {
    "approved",
    "aprovado",
    "paid",
    "pago",
    "completed",
    "complete",
    "active",
    "ativo",
    "authorized",
}

REVOKED_STATUSES = {
    "refunded",
    "reembolsado",
    "chargeback",
    "canceled",
    "cancelled",
    "cancelado",
    "refused",
    "recusado",
    "failed",
    "falhou",
}

PLAN_CHECKOUTS = (
    ("bronze", "🥉 Bronze semanal - R$ 7,99", CAKTO_PLAN_BRONZE_URL),
    ("ouro", "🏆 Ouro mensal - R$ 17,99", CAKTO_PLAN_OURO_URL),
    ("diamante", "💎 Diamante anual - R$ 79,99", CAKTO_PLAN_DIAMANTE_URL),
    ("rubi", "♦️ Rubi vitalício - R$ 249,00", CAKTO_PLAN_RUBI_URL),
)

_TRACKING_RE = re.compile(
    r"(?:^|[^a-z0-9])(?:tg|telegram)[_-]?(\d{4,})(?:[_-]plan[_-]?([a-z0-9_]+))?",
    re.IGNORECASE,
)


def _plain(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _normalize_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "tg://")):
        return url
    return f"https://{url}"


def _append_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: value for key, value in params.items() if value})
    return urlunparse(parsed._replace(query=urlencode(query)))


def build_checkout_url(base_url: str, user_id: int | str, plan: str) -> str:
    plan_key = normalize_plan(plan)
    url = _normalize_url(base_url)
    if not url or not plan_key:
        return ""

    uid = str(int(user_id)).strip()
    return _append_query_params(
        url,
        {
            "src": f"tg_{uid}",
            "sck": f"tg_{uid}_plan_{plan_key}",
            "utm_source": "telegram",
            "utm_medium": "bot",
            "utm_campaign": "offline_novel",
            "utm_content": plan_key,
        },
    )


def get_checkout_options(user_id: int | str | None) -> list[dict[str, str]]:
    if user_id is None:
        return []

    options = []
    for plan, label, base_url in PLAN_CHECKOUTS:
        url = build_checkout_url(base_url, user_id, plan)
        if url:
            options.append({"plan": plan, "label": label, "url": url})
    return options


def _iter_nodes(value: Any, path: tuple[str, ...] = ()):
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = (*path, key_text)
            yield child_path, key_text, child
            yield from _iter_nodes(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_nodes(child, (*path, str(index)))


def _scalar_text(value: Any) -> str:
    if isinstance(value, (str, int, float, bool)):
        return str(value).strip()
    return ""


def _all_scalar_strings(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for _path, _key, value in _iter_nodes(payload):
        text = _scalar_text(value)
        if text:
            values.append(text)
    return values


def _values_for_keys(payload: dict[str, Any], keys: set[str]) -> list[str]:
    values = []
    for _path, key, value in _iter_nodes(payload):
        if _plain(key) in keys:
            text = _scalar_text(value)
            if text:
                values.append(text)
    return values


def _event_from_text(value: str) -> str:
    token = _plain(value)
    aliases = {
        "compra_aprovada": "purchase_approved",
        "venda_aprovada": "purchase_approved",
        "purchase_approved": "purchase_approved",
        "assinatura_renovada": "subscription_renewed",
        "subscription_renewed": "subscription_renewed",
        "assinatura_criada": "subscription_created",
        "subscription_created": "subscription_created",
        "reembolso": "refund",
        "refund": "refund",
        "chargeback": "chargeback",
        "assinatura_cancelada": "subscription_canceled",
        "subscription_canceled": "subscription_canceled",
        "renovacao_de_assinatura_recusada": "subscription_renewal_refused",
        "subscription_renewal_refused": "subscription_renewal_refused",
        "compra_recusada": "purchase_refused",
        "purchase_refused": "purchase_refused",
        "teste": "test",
        "test": "test",
        "inicio_de_checkout": "initiate_checkout",
        "initiate_checkout": "initiate_checkout",
        "abandono_de_checkout": "checkout_abandonment",
        "checkout_abandonment": "checkout_abandonment",
        "pix_gerado": "pix_gerado",
        "boleto_gerado": "boleto_gerado",
        "picpay_gerado": "picpay_gerado",
        "openfinance_nubank_gerado": "openfinance_nubank_gerado",
    }
    if token in aliases:
        return aliases[token]

    for alias, event in aliases.items():
        if alias and alias in token:
            return event
    return ""


def extract_event_type(payload: dict[str, Any]) -> str:
    event_keys = {"event", "event_type", "event_name", "type", "custom_id", "customid"}
    for value in _values_for_keys(payload, event_keys):
        event = _event_from_text(value)
        if event:
            return event

    for value in _all_scalar_strings(payload):
        event = _event_from_text(value)
        if event:
            return event
    return ""


def extract_status(payload: dict[str, Any]) -> str:
    status_keys = {"status", "payment_status", "order_status", "transaction_status", "subscription_status"}
    deferred: list[str] = []
    for path, key, value in _iter_nodes(payload):
        if _plain(key) not in status_keys:
            continue
        token = _plain(_scalar_text(value))
        if not token:
            continue
        path_text = "_".join(_plain(item) for item in path)
        if any(item in path_text for item in ("payment", "transaction", "order", "purchase", "sale", "subscription")):
            return token
        deferred.append(token)

    for token in deferred:
        if token in APPROVED_STATUSES or token in REVOKED_STATUSES:
            return token
    return ""


def _tracking_candidate_strings(payload: dict[str, Any]) -> list[str]:
    hints = {
        "src",
        "sck",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "metadata",
        "meta",
        "tracking",
        "external_id",
        "external_reference",
        "reference",
    }
    values: list[str] = []
    for path, key, value in _iter_nodes(payload):
        path_text = "_".join(_plain(item) for item in path)
        if _plain(key) in hints or any(hint in path_text for hint in hints):
            text = _scalar_text(value)
            if text:
                values.append(text)
    values.extend(_all_scalar_strings(payload))
    return values


def extract_access_target(payload: dict[str, Any]) -> dict[str, Any]:
    user_id: int | None = None
    plan = ""
    tracking = ""

    for value in _tracking_candidate_strings(payload):
        match = _TRACKING_RE.search(value)
        if not match:
            continue
        user_id = int(match.group(1))
        tracking = value
        plan = normalize_plan(match.group(2) or "")
        break

    if user_id is None:
        telegram_keys = {"telegram_id", "telegram_user_id", "telegramid", "telegramuserid"}
        for value in _values_for_keys(payload, telegram_keys):
            if value.isdigit():
                user_id = int(value)
                break

    if not plan:
        plan_keys = {"plan", "plano", "utm_content", "sck"}
        for path, key, value in _iter_nodes(payload):
            text = _scalar_text(value)
            if not text:
                continue
            path_text = "_".join(_plain(item) for item in path)
            key_text = _plain(key)
            if key_text in plan_keys or any(item in path_text for item in ("plan", "plano", "offer", "oferta", "product", "produto")):
                plan = normalize_plan(text)
                if plan:
                    break

    if not plan and user_id is not None:
        access = get_offline_access(user_id)
        if access and access.get("plan"):
            plan = normalize_plan(access["plan"])

    return {
        "user_id": user_id,
        "plan": plan,
        "tracking": tracking,
    }


def extract_webhook_secret_values(payload: dict[str, Any]) -> list[str]:
    return _values_for_keys(payload, {"secret", "webhook_secret", "webhooksecret", "cakto_secret", "caktosecret"})


def _first_event_id_candidate(payload: dict[str, Any]) -> str:
    priority = {
        "event_id",
        "webhook_event_id",
        "webhookeventid",
        "transaction_id",
        "transactionid",
        "order_id",
        "orderid",
        "sale_id",
        "saleid",
        "purchase_id",
        "purchaseid",
        "payment_id",
        "paymentid",
        "invoice_id",
        "invoiceid",
        "subscription_id",
        "subscriptionid",
    }
    for value in _values_for_keys(payload, priority):
        if value:
            return value

    for path, key, value in _iter_nodes(payload):
        if _plain(key) != "id":
            continue
        path_text = "_".join(_plain(item) for item in path)
        if any(item in path_text for item in ("event", "transaction", "order", "sale", "purchase", "payment", "invoice", "subscription")):
            text = _scalar_text(value)
            if text:
                return text

    root_id = payload.get("id")
    return _scalar_text(root_id)


def extract_event_id(payload: dict[str, Any], event_type: str, user_id: int | None) -> str:
    candidate = _first_event_id_candidate(payload)
    if candidate:
        return f"cakto:{event_type or 'event'}:{candidate}:{user_id or 'unknown'}"

    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"cakto:{event_type or 'event'}:{digest}"


def _is_revocation_event(event_type: str, status: str) -> bool:
    return event_type in REVOCATION_EVENTS or status in REVOKED_STATUSES


def _is_approval_event(event_type: str, status: str) -> bool:
    if event_type in APPROVAL_EVENTS:
        return True
    if event_type in CONDITIONAL_APPROVAL_EVENTS and status in APPROVED_STATUSES:
        return True
    if event_type in REVOCATION_EVENTS or event_type in IGNORED_EVENTS:
        return False
    return status in APPROVED_STATUSES


def process_cakto_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    event_type = extract_event_type(payload)
    status = extract_status(payload)
    target = extract_access_target(payload)
    user_id = target.get("user_id")
    plan = normalize_plan(target.get("plan") or "")
    event_id = extract_event_id(payload, event_type, user_id)

    base = {
        "ok": True,
        "gateway": "cakto",
        "event_type": event_type,
        "status": status,
        "user_id": user_id,
        "plan": plan,
        "plan_label": plan_label(plan),
        "event_id": event_id,
    }

    if _is_revocation_event(event_type, status):
        if not user_id:
            return {**base, "action": "ignored", "reason": "missing_telegram_id"}
        access = revoke_offline_access(
            user_id,
            event_id=event_id,
            event_type=event_type or status or "cakto_revocation",
            reason="cakto",
            payload=payload,
        )
        return {**base, "action": "revoked", "access": access}

    if _is_approval_event(event_type, status):
        if not user_id:
            return {**base, "action": "ignored", "reason": "missing_telegram_id"}
        if not plan:
            return {**base, "action": "ignored", "reason": "missing_plan"}

        access = grant_offline_access(
            user_id,
            plan,
            event_id=event_id,
            event_type=event_type or status or "cakto_approval",
            source="cakto",
            payload=payload,
        )
        return {**base, "action": "granted", "access": access}

    return {**base, "action": "ignored", "reason": "event_not_handled"}
