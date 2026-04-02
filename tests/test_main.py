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
    monkeypatch.setattr(main.evolution_api_service, "get_instance_name", lambda: "instancia-teste")
    monkeypatch.setattr(
        main,
        "send_campaign",
        lambda campaign, dry_run: [
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

    result = main.run_campaign(
        Namespace(
            tipo="faltas",
            dia=25,
            report_path=None,
            dry_run=False,
            max_items=None,
            offset_items=0,
        )
    )

    output = capsys.readouterr().out
    assert result.endswith("campaign_faltas_dia_25_sent.json")
    assert "Modo de envio: real" in output
    assert "Instancia Evolution: instancia-teste" in output
    assert "Arquivo enviado salvo:" in output
    assert "Total de itens: 3" in output
    assert "Enviados: 2" in output
    assert "Falharam: 1" in output


def test_run_campaign_applies_max_items(monkeypatch, capsys) -> None:
    monkeypatch.setattr(main, "CampaignOrchestrator", StubOrchestrator)
    monkeypatch.setattr(main.evolution_api_service, "get_instance_name", lambda: "instancia-teste")
    captured = {}

    def fake_send_campaign(campaign, dry_run):
        captured["campaign"] = campaign
        captured["dry_run"] = dry_run
        return [{"status": "sent"} for _ in campaign]

    monkeypatch.setattr(main, "send_campaign", fake_send_campaign)
    monkeypatch.setattr(
        main,
        "save_sent_campaign_to_json",
        lambda sent_campaign, campaign_type, day, output_dir: str(
            Path(output_dir) / "campaign_reuniao_sent.json"
        ),
    )

    main.run_campaign(
        Namespace(
            tipo="reuniao",
            dia=None,
            report_path=None,
            dry_run=True,
            max_items=2,
            offset_items=0,
        )
    )

    output = capsys.readouterr().out
    assert len(captured["campaign"]) == 2
    assert captured["dry_run"] is True
    assert "Modo de envio: dry-run" in output
    assert "Itens planejados: 3" in output
    assert "Itens pulados: 0" in output
    assert "Itens a enviar: 2" in output


def test_run_campaign_applies_offset_items(monkeypatch, capsys) -> None:
    monkeypatch.setattr(main, "CampaignOrchestrator", StubOrchestrator)
    monkeypatch.setattr(main.evolution_api_service, "get_instance_name", lambda: "instancia-teste")
    captured = {}

    def fake_send_campaign(campaign, dry_run):
        captured["campaign"] = campaign
        return [{"status": "sent"} for _ in campaign]

    monkeypatch.setattr(main, "send_campaign", fake_send_campaign)
    monkeypatch.setattr(
        main,
        "save_sent_campaign_to_json",
        lambda sent_campaign, campaign_type, day, output_dir: str(
            Path(output_dir) / "campaign_reuniao_sent.json"
        ),
    )

    main.run_campaign(
        Namespace(
            tipo="reuniao",
            dia=None,
            report_path=None,
            dry_run=True,
            max_items=1,
            offset_items=1,
        )
    )

    output = capsys.readouterr().out
    assert len(captured["campaign"]) == 1
    assert "Itens pulados: 1" in output
    assert "Itens a enviar: 1" in output
