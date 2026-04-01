import json
from pathlib import Path

from services.sender import save_sent_campaign_to_json


def test_save_sent_campaign_to_json_uses_expected_name_and_content(tmp_path: Path) -> None:
    sent_campaign = [
        {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone": "5518999991111@s.whatsapp.net",
            "message": "Teste",
            "status": "sent",
            "sent_at": "2026-03-29T12:00:00",
            "attempt_number": 1,
            "provider": "evolution",
            "provider_message_id": "msg-123",
        }
    ]

    file_path = save_sent_campaign_to_json(
        sent_campaign,
        campaign_type="faltas",
        day=25,
        output_dir=tmp_path,
    )

    saved_path = Path(file_path)
    assert saved_path.name == "campaign_faltas_dia_25_sent.json"
    assert saved_path.exists()

    payload = json.loads(saved_path.read_text(encoding="utf-8"))
    assert payload[0]["status"] == "sent"
    assert payload[0]["sent_at"] == "2026-03-29T12:00:00"
    assert payload[0]["attempt_number"] == 1
    assert payload[0]["provider"] == "evolution"
    assert payload[0]["provider_message_id"] == "msg-123"
