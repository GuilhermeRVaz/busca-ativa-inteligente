import json
from pathlib import Path

from core import campaign_engine


def test_generate_campaign_with_single_phone() -> None:
    absences = [{"student_name": "Joao Silva", "class_name": "7A", "absence_days": "25"}]
    contacts = [
        {
            "student_name": " joao silva ",
            "phone1": "(18) 99999-1111",
            "phone2": "",
            "phone3": None,
        }
    ]

    campaign = campaign_engine.generate_campaign(absences, contacts, school_name="Escola Teste")

    assert len(campaign) == 1
    assert campaign[0]["student_name"] == "Joao Silva"
    assert campaign[0]["class_name"] == "7A"
    assert campaign[0]["phone"] == "18999991111"
    assert campaign[0]["status"] == "pending"
    assert campaign[0]["message"]
    assert campaign[0]["template_id"]


def test_generate_campaign_with_multiple_phones() -> None:
    absences = [{"student_name": "Maria Souza", "class_name": "8B", "absence_days": "25"}]
    contacts = [
        {
            "student_name": "Maria Souza",
            "phone1": "18999990001",
            "phone2": "18999990002",
            "phone3": "18999990003",
        }
    ]

    campaign = campaign_engine.generate_campaign(absences, contacts)

    assert len(campaign) == 3
    assert {item["phone"] for item in campaign} == {
        "18999990001",
        "18999990002",
        "18999990003",
    }


def test_generate_campaign_ignores_invalid_phones() -> None:
    absences = [{"student_name": "Ana Costa", "class_name": "6C", "absence_days": "25"}]
    contacts = [
        {
            "student_name": "Ana Costa",
            "phone1": "123",
            "phone2": "",
            "phone3": "18 99999-2222",
        }
    ]

    campaign = campaign_engine.generate_campaign(absences, contacts)

    assert len(campaign) == 1
    assert campaign[0]["phone"] == "18999992222"


def test_generate_campaign_without_contact_does_not_break() -> None:
    absences = [{"student_name": "Aluno Sem Contato", "class_name": "9A", "absence_days": "25"}]

    campaign = campaign_engine.generate_campaign(absences, contacts=[])

    assert campaign == []


def test_save_campaign_to_json(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(campaign_engine.settings, "data_dir", tmp_path)
    campaign = [
        {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone": "18999991111",
            "message": "Teste",
            "status": "pending",
            "template_id": "absence_followup_1",
        }
    ]

    file_path = campaign_engine.save_campaign_to_json(campaign, filename="daily")

    saved_path = Path(file_path)
    assert saved_path.exists()
    assert saved_path.parent == tmp_path / "campaigns"

    saved_campaign = json.loads(saved_path.read_text(encoding="utf-8"))
    assert saved_campaign == campaign
