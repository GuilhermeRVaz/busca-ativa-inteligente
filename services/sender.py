import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from core.logging import get_logger


logger = get_logger(__name__)

EVOLUTION_ENDPOINT = "http://localhost:8080/message/sendText/escola-decia"
EVOLUTION_HEADERS = {
    "apikey": "escola123",
    "Content-Type": "application/json",
}
REQUEST_TIMEOUT_SECONDS = 30


def send_campaign(campaign: list[dict[str, Any]]) -> list[dict[str, Any]]:
    updated_campaign: list[dict[str, Any]] = []
    total_items = len(campaign)

    for index, item in enumerate(campaign, start=1):
        updated_item = dict(item)
        attempt_number = int(updated_item.get("attempt_number") or 0) + 1
        sent_at = datetime.now().isoformat(timespec="seconds")
        normalized_phone = _normalize_phone(updated_item.get("phone"))

        updated_item["attempt_number"] = attempt_number
        updated_item["sent_at"] = sent_at
        updated_item["provider"] = "evolution"
        updated_item["provider_message_id"] = None

        validation_error = _validate_item(updated_item, normalized_phone)
        if validation_error:
            updated_item["status"] = "failed"
            logger.info(
                "ENVIO EVOLUTION | campaign_id=%s | phone=%s | status=failed | attempt_number=%s | motivo=%s",
                updated_item.get("campaign_id", ""),
                normalized_phone or updated_item.get("phone", ""),
                attempt_number,
                validation_error,
            )
        else:
            success, provider_message_id, used_fallback = _send_to_evolution(
                normalized_phone,
                str(updated_item.get("message", "")).strip(),
            )
            updated_item["phone"] = normalized_phone
            updated_item["provider_message_id"] = provider_message_id
            updated_item["status"] = "sent" if success else "failed"
            logger.info(
                "ENVIO EVOLUTION | campaign_id=%s | phone=%s | status=%s | attempt_number=%s | fallback=%s",
                updated_item.get("campaign_id", ""),
                normalized_phone,
                updated_item["status"],
                attempt_number,
                "sim" if used_fallback else "nao",
            )

        updated_campaign.append(updated_item)

        if index < total_items:
            _sleep_between_messages(index)

    return updated_campaign


def save_sent_campaign_to_json(
    sent_campaign: list[dict[str, Any]],
    campaign_type: str,
    day: int | None,
    output_dir: str | Path,
) -> str:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    if campaign_type == "faltas" and day is not None:
        file_name = f"campaign_faltas_dia_{day}_sent.json"
    else:
        file_name = f"campaign_{campaign_type}_sent.json"

    file_path = directory / file_name
    file_path.write_text(
        json.dumps(sent_campaign, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(file_path)


def _normalize_phone(value: Any) -> str:
    digits = "".join(char for char in str(value or "") if char.isdigit())
    if not digits:
        return ""

    if len(digits) < 10:
        return ""

    if not digits.startswith("55"):
        digits = f"55{digits}"

    if len(digits) < 12:
        return ""

    return f"{digits}@s.whatsapp.net"


def _validate_item(item: dict[str, Any], normalized_phone: str) -> str | None:
    if item.get("status") != "pending":
        return "status_invalido"

    message = str(item.get("message", "")).strip()
    if not normalized_phone:
        return "telefone_invalido"

    if not message:
        return "mensagem_vazia"

    return None


def _send_to_evolution(phone: str, message: str) -> tuple[bool, str | None, bool]:
    primary_payload = {
        "number": phone,
        "text": message,
    }
    success, provider_message_id, fallback_needed = _perform_send(primary_payload)
    if success or not fallback_needed:
        return success, provider_message_id, False

    fallback_payload = {
        "number": phone,
        "textMessage": {
            "text": message,
        },
    }
    fallback_success, fallback_message_id, _ = _perform_send(fallback_payload)
    return fallback_success, fallback_message_id, True


def _perform_send(payload: dict[str, Any]) -> tuple[bool, str | None, bool]:
    try:
        response = requests.post(
            EVOLUTION_ENDPOINT,
            headers=EVOLUTION_HEADERS,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("Falha de conexao com Evolution API: %s", exc)
        return False, None, False

    response_data = _parse_response_json(response)
    success = _is_successful_response(response)
    provider_message_id = _extract_provider_message_id(response_data) if success else None
    fallback_needed = _should_retry_with_fallback(response, response_data, payload)
    return success, provider_message_id, fallback_needed


def _parse_response_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {}


def _is_successful_response(response: requests.Response) -> bool:
    return (
        response.status_code in [200, 201]
        and "error" not in response.text.lower()
    )


def _should_retry_with_fallback(
    response: requests.Response,
    response_data: Any,
    payload: dict[str, Any],
) -> bool:
    if "text" not in payload:
        return False

    if _is_successful_response(response):
        return False

    return True


def _extract_provider_message_id(response_data: Any) -> str | None:
    if not isinstance(response_data, dict):
        return None

    provider_message_id = (
        response_data.get("key", {}).get("id")
        or response_data.get("id")
        or response_data.get("messageId")
    )
    return str(provider_message_id) if provider_message_id else None


def _sleep_between_messages(processed_items: int) -> None:
    delay_seconds = random.randint(20, 40)
    if processed_items % 10 == 0:
        delay_seconds += random.randint(60, 120)
    time.sleep(delay_seconds)
