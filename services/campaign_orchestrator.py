from datetime import datetime
from pathlib import Path

from core.campaign_engine import generate_campaign, save_campaign_to_json
from core.config import settings
from providers.absences_provider import ConsolidatedAbsencesProvider
from providers.contacts_provider import GoogleSheetsContactsProvider


class CampaignOrchestrator:
    def __init__(
        self,
        absences_provider: ConsolidatedAbsencesProvider | None = None,
        contacts_provider: GoogleSheetsContactsProvider | None = None,
    ) -> None:
        self.absences_provider = absences_provider or ConsolidatedAbsencesProvider()
        self.contacts_provider = contacts_provider or GoogleSheetsContactsProvider()

    def run(
        self,
        campaign_type: str,
        day: int | None = None,
        report_path: str | Path | None = None,
    ) -> dict:
        contacts = self.contacts_provider.fetch_contacts()
        if campaign_type == "faltas":
            if day is None:
                raise ValueError("Informe --dia para gerar campanha de faltas.")
            absences = self.absences_provider.fetch_absences(
                report_path=report_path or settings.consolidated_report_path,
                day=day,
            )
        elif campaign_type == "reuniao":
            absences = self._build_meeting_audience(contacts)
        else:
            raise ValueError(f"Tipo de campanha nao suportado: {campaign_type}")

        campaign_id = self._build_campaign_id(campaign_type, day)
        created_at = datetime.now().isoformat(timespec="seconds")
        campaign = generate_campaign(
            absences,
            contacts,
            campaign_type=campaign_type,
            campaign_id=campaign_id,
            created_at=created_at,
            school_name=settings.school_name,
        )
        file_path = save_campaign_to_json(
            campaign,
            filename=self._build_filename(campaign_type, day),
        )

        processed_students = len(absences)
        students_with_campaign = {item["student_name"] for item in campaign}
        return {
            "campaign_type": campaign_type,
            "target_day": day,
            "processed_students": processed_students,
            "generated_items": len(campaign),
            "students_without_contact": max(processed_students - len(students_with_campaign), 0),
            "file_path": file_path,
            "campaign": campaign,
            "campaign_id": campaign_id,
            "created_at": created_at,
        }

    @staticmethod
    def _build_meeting_audience(contacts: list[dict]) -> list[dict]:
        audience: list[dict] = []
        seen_students: set[str] = set()
        for contact in contacts:
            student_name = str(contact.get("student_name", "")).strip()
            if not student_name or student_name in seen_students:
                continue
            seen_students.add(student_name)
            audience.append(
                {
                    "student_name": student_name,
                    "class_name": str(contact.get("class_name", "")).strip(),
                    "absence_days": "",
                }
            )
        return audience

    @staticmethod
    def _build_filename(campaign_type: str, day: int | None) -> str:
        if campaign_type == "faltas" and day is not None:
            return f"campaign_{campaign_type}_dia_{day}.json"
        return f"campaign_{campaign_type}.json"

    @staticmethod
    def _build_campaign_id(campaign_type: str, day: int | None) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if campaign_type == "faltas" and day is not None:
            return f"campaign_{campaign_type}_dia_{day}_{timestamp}"
        return f"campaign_{campaign_type}_{timestamp}"
