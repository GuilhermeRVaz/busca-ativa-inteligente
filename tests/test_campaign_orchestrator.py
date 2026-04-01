from pathlib import Path

from services.campaign_orchestrator import CampaignOrchestrator


class StubAbsencesProvider:
    def fetch_absences(self, report_path, day):
        return [
            {
                "student_name": "Joao Silva",
                "class_name": "7A",
                "absence_days": str(day),
            },
            {
                "student_name": "Aluno Sem Contato",
                "class_name": "8B",
                "absence_days": str(day),
            },
        ]


class StubContactsProvider:
    def fetch_contacts(self):
        return [
            {
                "student_name": "Joao Silva",
                "class_name": "7A",
                "phone1": "18999991111",
                "phone2": "",
                "phone3": "",
            }
        ]


def test_orchestrator_generates_json_and_summary(tmp_path: Path, monkeypatch) -> None:
    from services import campaign_orchestrator

    monkeypatch.setattr(campaign_orchestrator.settings, "data_dir", tmp_path)
    monkeypatch.setattr(
        campaign_orchestrator.settings,
        "consolidated_report_path",
        tmp_path / "consolidado.xlsx",
    )
    monkeypatch.setattr(campaign_orchestrator.settings, "school_name", "Escola Teste")

    orchestrator = CampaignOrchestrator(
        absences_provider=StubAbsencesProvider(),
        contacts_provider=StubContactsProvider(),
    )

    result = orchestrator.run(campaign_type="faltas", day=25)

    assert result["generated_items"] == 1
    assert result["processed_students"] == 2
    assert result["students_without_contact"] == 1
    assert result["campaign_id"].startswith("campaign_faltas_dia_25_")
    assert result["created_at"]
    assert Path(result["file_path"]).exists()
    assert result["campaign"][0]["campaign_id"] == result["campaign_id"]
    assert result["campaign"][0]["created_at"] == result["created_at"]
