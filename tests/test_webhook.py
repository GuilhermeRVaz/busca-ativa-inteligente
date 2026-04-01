import json
from pathlib import Path

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from ai import classifier
from app.main import app
from core.config import settings
from data.repository import repository
from services import webhook_service as webhook_module


client = TestClient(app)


def _evolution_payload(message: str | None = "Quero voltar a estudar") -> dict:
    payload = {
        "event": "messages.upsert",
        "data": {
            "key": {
                "remoteJid": "+5514999999999@s.whatsapp.net",
                "fromMe": False,
                "id": "ABC123",
            },
            "message": {},
        },
    }
    if message is not None:
        payload["data"]["message"]["conversation"] = message
    return payload


def test_webhook_processes_complete_payload(monkeypatch) -> None:
    saved: dict = {}

    monkeypatch.setattr(
        webhook_module,
        "classificar_mensagem",
        lambda mensagem: {
            "intencao": "RETORNAR",
            "motivo": "OUTRO",
            "observacao": "aluno quer retornar",
        },
    )
    monkeypatch.setattr(repository, "salvar_interacao", lambda data: saved.update(data))

    response = client.post("/webhook/messages", json=_evolution_payload("acho que volto sim"))

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["classification"] == "RETORNAR"
    assert response.json()["telefone"] == "5514999999999"
    assert saved["telefone"] == "5514999999999"
    assert saved["mensagem"] == "acho que volto sim"
    assert saved["classificacao"] == "RETORNAR"
    assert saved["intencao"] == "RETORNAR"
    assert saved["motivo"] == "OUTRO"
    assert saved["observacao"] == "aluno quer retornar"
    assert saved["campaign_id"] == ""
    assert saved["origem"] == "whatsapp"
    assert saved["data_hora"]


def test_webhook_handles_missing_conversation_with_safe_fallback(monkeypatch) -> None:
    saved: dict = {}

    monkeypatch.setattr(
        webhook_module,
        "classificar_mensagem",
        lambda mensagem: {
            "intencao": "DUVIDA",
            "motivo": "OUTRO",
            "observacao": "mensagem vaga",
        },
    )
    monkeypatch.setattr(repository, "salvar_interacao", lambda data: saved.update(data))

    response = client.post("/webhook/messages", json=_evolution_payload(message=None))

    assert response.status_code == 200
    assert response.json()["classification"] == "DUVIDA"
    assert saved["mensagem"] == ""
    assert saved["telefone"] == "5514999999999"
    assert saved["campaign_id"] == ""
    assert saved["origem"] == "whatsapp"


def test_classifier_falls_back_to_duvida_on_openai_failure(monkeypatch) -> None:
    class FailingCompletions:
        def create(self, *args, **kwargs):
            raise RuntimeError("boom")

    class FailingChat:
        def __init__(self) -> None:
            self.completions = FailingCompletions()

    class FailingClient:
        def __init__(self, *args, **kwargs) -> None:
            self.chat = FailingChat()

    monkeypatch.setattr(classifier, "OpenAI", FailingClient)
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-4o-mini")

    result = classifier.classificar_mensagem("acho que volto sim")

    assert result["intencao"] == "DUVIDA"


def test_repository_keeps_local_backup_when_google_sheets_fails(
    monkeypatch, tmp_path: Path
) -> None:
    incoming_file = tmp_path / "incoming_messages.json"
    service_account_file = tmp_path / "service-account.json"
    service_account_file.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(repository, "incoming_messages_file", incoming_file)
    monkeypatch.setattr(settings, "google_sheet_dados_url", "https://example.com/sheet")
    monkeypatch.setattr(settings, "google_sheet_dados_worksheet", "Interacoes")
    monkeypatch.setattr(settings, "google_service_account_file", service_account_file)

    def failing_service_account(*args, **kwargs):
        raise RuntimeError("sheet down")

    monkeypatch.setattr("data.repository.gspread.service_account", failing_service_account)

    repository.salvar_interacao(
        {
            "telefone": "5514999999999",
            "mensagem": "acho que volto sim",
            "classificacao": "RETORNAR",
            "intencao": "RETORNAR",
            "motivo": "OUTRO",
            "observacao": "teste",
            "data_hora": "2026-04-01T10:00:00",
            "campaign_id": "",
            "origem": "whatsapp",
            "raw_payload": {"event": "messages.upsert"},
        }
    )

    saved = json.loads(incoming_file.read_text(encoding="utf-8"))
    assert len(saved) == 1
    assert saved[0]["telefone"] == "5514999999999"
    assert saved[0]["classificacao"] == "RETORNAR"
    assert saved[0]["origem"] == "whatsapp"


def test_repository_salvar_interacao_appends_expected_columns(monkeypatch, tmp_path: Path) -> None:
    incoming_file = tmp_path / "incoming_messages.json"
    service_account_file = tmp_path / "service-account.json"
    service_account_file.write_text("{}", encoding="utf-8")
    appended_rows: list[list[str]] = []

    class FakeWorksheet:
        def append_row(self, row):
            appended_rows.append(row)

    class FakeSpreadsheet:
        def worksheet(self, name: str):
            assert name == "Interacoes"
            return FakeWorksheet()

    class FakeClient:
        def open_by_url(self, url: str):
            assert url == "https://example.com/dados"
            return FakeSpreadsheet()

    monkeypatch.setattr(repository, "incoming_messages_file", incoming_file)
    monkeypatch.setattr(settings, "google_sheet_dados_url", "https://example.com/dados")
    monkeypatch.setattr(settings, "google_sheet_dados_worksheet", "Interacoes")
    monkeypatch.setattr(settings, "google_service_account_file", service_account_file)
    monkeypatch.setattr("data.repository.gspread.service_account", lambda **kwargs: FakeClient())

    repository.salvar_interacao(
        {
            "telefone": "5514999999999",
            "mensagem": "acho que volto sim",
            "classificacao": "RETORNAR",
            "intencao": "RETORNAR",
            "motivo": "OUTRO",
            "observacao": "teste",
            "data_hora": "2026-04-01T10:00:00",
            "campaign_id": "campaign-123",
            "origem": "whatsapp",
            "raw_payload": {"event": "messages.upsert"},
        }
    )

    assert appended_rows == [
        [
            "2026-04-01T10:00:00",
            "5514999999999",
            "acho que volto sim",
            "RETORNAR",
            "OUTRO",
            "teste",
            "campaign-123",
            "whatsapp",
        ]
    ]


def test_repository_carregar_contatos_uses_contact_sheet_settings(monkeypatch, tmp_path: Path) -> None:
    service_account_file = tmp_path / "service-account.json"
    service_account_file.write_text("{}", encoding="utf-8")

    class FakeWorksheet:
        def get_all_records(self):
            return [
                {
                    "Nome do Aluno": "Joao Silva",
                    "Turma": "7A",
                    "Telefone 1": "(18) 99999-1111",
                    "Telefone 2": "123",
                    "Celular 3": "18 99999-2222",
                }
            ]

    class FakeSpreadsheet:
        def worksheet(self, name: str):
            assert name == "Contatos"
            return FakeWorksheet()

    class FakeClient:
        def open_by_url(self, url: str):
            assert url == "https://example.com/contatos"
            return FakeSpreadsheet()

    monkeypatch.setattr(settings, "google_sheet_contatos_url", "https://example.com/contatos")
    monkeypatch.setattr(settings, "google_sheet_contatos_worksheet", "Contatos")
    monkeypatch.setattr(settings, "google_service_account_file", service_account_file)
    monkeypatch.setattr("data.repository.gspread.service_account", lambda **kwargs: FakeClient())

    contacts = repository.carregar_contatos()

    assert contacts == [
        {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "phone1": "18999991111",
            "phone2": "18999992222",
            "phone3": "",
        }
    ]


def test_app_has_single_webhook_route() -> None:
    webhook_routes = [
        route
        for route in app.routes
        if isinstance(route, APIRoute)
        and route.path == "/webhook/messages"
        and "POST" in route.methods
    ]

    assert len(webhook_routes) == 1
