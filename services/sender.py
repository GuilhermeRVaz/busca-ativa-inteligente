import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from core.config import settings
from core.logging import get_logger
from data.repository import repository
from services.evolution_api import evolution_api_service


logger = get_logger(__name__)


def send_campaign(
    campaign: list[dict[str, Any]],
    *,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    updated_campaign: list[dict[str, Any]] = []
    total_items = len(campaign)

    if not dry_run:
        evolution_api_service.validate_configuration()

    for index, item in enumerate(campaign, start=1):
        updated_item = dict(item)
        attempt_number = int(updated_item.get("attempt_number") or 0) + 1
        sent_at = datetime.now().isoformat(timespec="seconds")
        normalized_phone = _normalize_phone(updated_item.get("phone"))

        updated_item["attempt_number"] = attempt_number
        updated_item["sent_at"] = sent_at
        updated_item["provider"] = "evolution"
        updated_item["provider_message_id"] = None
        updated_item["dry_run"] = dry_run
        updated_item["failure_reason"] = None
        updated_item["used_fallback"] = False

        validation_error = _validate_item(updated_item, normalized_phone)
        if validation_error:
            updated_item["status"] = "failed"
            updated_item["failure_reason"] = validation_error
            _log_send_result(updated_item, normalized_phone or updated_item.get("phone", ""))
        else:
            send_result = evolution_api_service.send_text_message(
                normalized_phone,
                str(updated_item.get("message", "")).strip(),
                dry_run=dry_run,
            )
            updated_item["phone"] = normalized_phone
            updated_item["provider_message_id"] = send_result.get("provider_message_id")
            updated_item["used_fallback"] = bool(send_result.get("used_fallback"))
            updated_item["status"] = "sent" if send_result.get("success") else "failed"
            updated_item["failure_reason"] = send_result.get("error")
            _log_send_result(updated_item, normalized_phone)
            repository.save_message(
                conversation_id=normalized_phone,
                direction="outbound",
                text=str(updated_item.get("message", "")).strip(),
                metadata={
                    "campaign_id": updated_item.get("campaign_id", ""),
                    "student_name": updated_item.get("student_name", ""),
                    "class_name": updated_item.get("class_name", ""),
                    "provider_message_id": updated_item.get("provider_message_id"),
                    "status": updated_item.get("status"),
                    "failure_reason": updated_item.get("failure_reason"),
                    "dry_run": dry_run,
                },
            )

        updated_campaign.append(updated_item)

        if index < total_items and not dry_run:
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


def _log_send_result(item: dict[str, Any], phone: str) -> None:
    logger.info(
        "ENVIO EVOLUTION | campaign_id=%s | phone=%s | status=%s | attempt_number=%s | fallback=%s | provider_message_id=%s | motivo_falha=%s | dry_run=%s",
        item.get("campaign_id", ""),
        phone,
        item.get("status", ""),
        item.get("attempt_number", 0),
        "sim" if item.get("used_fallback") else "nao",
        item.get("provider_message_id"),
        item.get("failure_reason"),
        "sim" if item.get("dry_run") else "nao",
    )


def _sleep_between_messages(processed_items: int) -> None:
    delay_seconds = random.randint(
        settings.send_min_delay_seconds,
        settings.send_max_delay_seconds,
    )
    if (
        settings.send_batch_extra_every > 0
        and processed_items % settings.send_batch_extra_every == 0
    ):
        delay_seconds += random.randint(
            settings.send_batch_extra_delay_min_seconds,
            settings.send_batch_extra_delay_max_seconds,
        )
    time.sleep(delay_seconds)
