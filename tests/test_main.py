from argparse import Namespace
from pathlib import Path

import main


class StubOrchestrator:
    def run(self, campaign_type, day, report_path):
        return {
            "campaign_type": campaign_type,
            "file_path": "data/storage/campaigns/test.json",
            "processed_students": 2,
            "generated_items": 3,
            "students_without_contact": 1,
            "campaign": [
                {"status": "pending"},
                {"status": "pending"},
                {"status": "pending"},
            ],
        }


def test_run_campaign_prints_sender_summary(monkeypatch, capsys) -> None:
    monkeypatch.setattr(main, "CampaignOrchestrator", StubOrchestrator)
    monkeypatch.setattr(
        main,
        "send_campaign",
        lambda campaign: [
            {"status": "sent"},
            {"status": "sent"},
            {"status": "failed"},
        ],
    )
    monkeypatch.setattr(
        main,
        "save_sent_campaign_to_json",
        lambda sent_campaign, campaign_type, day, output_dir: str(
            Path(output_dir) / "campaign_faltas_dia_25_sent.json"
        ),
    )

    result = main.run_campaign(Namespace(tipo="faltas", dia=25, report_path=None))

    output = capsys.readouterr().out
    assert result.endswith("campaign_faltas_dia_25_sent.json")
    assert "Arquivo enviado salvo:" in output
    assert "Total de itens: 3" in output
    assert "Enviados: 2" in output
    assert "Falharam: 1" in output
