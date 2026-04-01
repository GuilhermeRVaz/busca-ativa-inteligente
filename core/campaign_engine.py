import json
from datetime import datetime
from pathlib import Path

from core.config import settings
from core.message_catalog import generate_message


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def _normalize_phone(value: str | None) -> str:
    if not value:
        return ""

    digits = "".join(char for char in str(value) if char.isdigit())
    if len(digits) < 10:
        return ""

    return digits


def _build_contacts_index(contacts: list[dict]) -> dict[str, dict]:
    index: dict[str, dict] = {}
    for contact in contacts:
        student_name = _normalize_text(contact.get("student_name"))
        if student_name:
            index[student_name] = contact
    return index


def generate_campaign(
    absences: list[dict],
    contacts: list[dict],
    campaign_type: str = "faltas",
    *,
    campaign_id: str | None = None,
    created_at: str | None = None,
    school_name: str = "Escola",
) -> list[dict]:
    campaign: list[dict] = []
    contacts_index = _build_contacts_index(contacts)
    resolved_campaign_id = campaign_id or f"campaign_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    resolved_created_at = created_at or datetime.now().isoformat(timespec="seconds")

    for absence in absences:
        student_name = absence.get("student_name", "").strip()
        class_name = absence.get("class_name", "").strip()
        absence_days = str(absence.get("absence_days", "")).strip()
        normalized_name = _normalize_text(student_name)
        contact = contacts_index.get(normalized_name)

        if not contact:
            continue

        for phone_field in ("phone1", "phone2", "phone3"):
            phone = _normalize_phone(contact.get(phone_field))
            if not phone:
                continue

            message_payload = generate_message(
                student_name=student_name,
                class_name=class_name or str(contact.get("class_name", "")).strip(),
                campaign_type=campaign_type,
                school_name=school_name,
                absence_days=absence_days,
                unique_key=f"{campaign_type}|{student_name}|{phone_field}|{phone}",
            )

            campaign.append(
                {
                    "campaign_id": resolved_campaign_id,
                    "created_at": resolved_created_at,
                    "student_name": student_name,
                    "class_name": class_name or str(contact.get("class_name", "")).strip(),
                    "phone": phone,
                    "message": message_payload["message"],
                    "status": "pending",
                    "template_id": message_payload["template_id"],
                }
            )

    return campaign


def save_campaign_to_json(campaign: list[dict], filename: str | None = None) -> str:
    campaigns_dir = Path(settings.data_dir) / "campaigns"
    campaigns_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = filename or f"campaign_{timestamp}.json"
    if not file_name.endswith(".json"):
        file_name = f"{file_name}_{timestamp}.json"

    file_path = campaigns_dir / file_name
    file_path.write_text(
        json.dumps(campaign, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(file_path)
