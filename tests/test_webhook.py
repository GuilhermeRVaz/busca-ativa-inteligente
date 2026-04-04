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
    monkeypatch.setattr(
        repository,
        "resolver_contexto_aluno",
        lambda telefone, student_name="", push_name="", data_hora="": {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "ra": "000123456789-1/SP",
            "tipo_responsavel": "mae",
            "numero_chamado": "5514999999999",
        },
    )
    monkeypatch.setattr(repository, "salvar_interacao", lambda data: saved.update(data))
    monkeypatch.setattr(repository, "save_message", lambda **kwargs: None)

    response = client.post("/webhook/messages", json=_evolution_payload("acho que volto sim"))

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["classification"] == "RETORNAR"
    assert response.json()["telefone"] == "5514999999999"
    assert response.json()["student_name"] == "Joao Silva"
    assert response.json()["class_name"] == "7A"
    assert response.json()["ra"] == "000123456789-1/SP"
    assert response.json()["tipo_responsavel"] == "mae"
    assert response.json()["numero_chamado"] == "5514999999999"
    assert response.json()["identificador_remetente"] == "5514999999999"
    assert saved["telefone"] == "5514999999999"
    assert saved["mensagem"] == "acho que volto sim"
    assert saved["classificacao"] == "RETORNAR"
    assert saved["intencao"] == "RETORNAR"
    assert saved["motivo"] == "OUTRO"
    assert saved["observacao"] == "aluno quer retornar"
    assert saved["student_name"] == "Joao Silva"
    assert saved["class_name"] == "7A"
    assert saved["ra"] == "000123456789-1/SP"
    assert saved["tipo_responsavel"] == "mae"
    assert saved["numero_chamado"] == "5514999999999"
    assert saved["identificador_remetente"] == "5514999999999"
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
    monkeypatch.setattr(
        repository,
        "resolver_contexto_aluno",
        lambda telefone, student_name="", push_name="", data_hora="": {
            "student_name": "Joao Silva",
            "class_name": "7A",
            "ra": "000123456789-1/SP",
            "tipo_responsavel": "mae",
            "numero_chamado": "5514999999999",
        },
    )
    monkeypatch.setattr(repository, "salvar_interacao", lambda data: saved.update(data))
    monkeypatch.setattr(repository, "save_message", lambda **kwargs: None)

    response = client.post("/webhook/messages", json=_evolution_payload(message=None))

    assert response.status_code == 200
    assert response.json()["classification"] == "DUVIDA"
    assert saved["mensagem"] == ""
    assert saved["telefone"] == "5514999999999"
    assert saved["class_name"] == "7A"
    assert saved["ra"] == "000123456789-1/SP"
    assert saved["tipo_responsavel"] == "mae"
    assert saved["numero_chamado"] == "5514999999999"
    assert saved["identificador_remetente"] == "5514999999999"
    assert saved["campaign_id"] == ""
    assert saved["origem"] == "whatsapp"


def test_webhook_ignores_send_message_events(monkeypatch) -> None:
    saved: dict = {}

    monkeypatch.setattr(repository, "salvar_interacao", lambda data: saved.update(data))
    monkeypatch.setattr(repository, "save_message", lambda **kwargs: None)

    response = client.post(
        "/webhook/messages",
        json={
            "event": "send.message",
            "data": {
                "key": {
                    "remoteJid": "5514999999999@s.whatsapp.net",
                    "fromMe": True,
                    "id": "ABC123",
                },
                "message": {"conversation": "mensagem enviada"},
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["classification"] == "IGNORADO"
    assert saved == {}


def test_webhook_ignores_duplicate_messages(monkeypatch) -> None:
    saved: dict = {}

    monkeypatch.setattr(repository, "interacao_ja_registrada", lambda payload: True)
    monkeypatch.setattr(repository, "salvar_interacao", lambda data: saved.update(data))
    monkeypatch.setattr(repository, "save_message", lambda **kwargs: None)

    response = client.post("/webhook/messages", json=_evolution_payload("mensagem repetida"))

    assert response.status_code == 200
    assert response.json()["classification"] == "IGNORADO"
    assert saved == {}


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
        def row_values(self, index: int):
            assert index == 1
            return [
                "data_hora",
                "student_name",
                "class_name",
                "ra",
                "tipo_responsavel",
                "numero_chamado",
                "identificador_remetente",
                "mensagem",
                "intencao",
                "motivo",
                "observacao",
                "campaign_id",
                "origem",
            ]

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
            "",
            "",
            "",
            "",
            "5514999999999",
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
                    "RA": 123456789,
                    "Dig. RA": 1,
                    "UF RA": "SP",
                    "Turma": "7A",
                    "responsável 1": "mae",
                    "Telefone 1": "(18) 99999-1111",
                    "responsavel 2": "pai",
                    "Telefone 2": "123",
                    "responsavel 3": "avo",
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
            "ra": "000123456789-1/SP",
            "phone1": "18999991111",
            "phone2": "",
            "phone3": "18999992222",
            "responsible_type1": "mae",
            "responsible_type2": "pai",
            "responsible_type3": "avo",
        }
    ]


def test_repository_resolver_nome_aluno_returns_empty_without_confident_match(
    monkeypatch, tmp_path: Path
) -> None:
    campaigns_dir = tmp_path / "campaigns"
    campaigns_dir.mkdir()
    sent_file = campaigns_dir / "campaign_faltas_dia_2_sent.json"
    sent_file.write_text(
        json.dumps(
            [
                {
                    "student_name": "BRYAN ENZO SILVA CAVALCANTI",
                    "status": "sent",
                    "phone": "5514981324832@s.whatsapp.net",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(repository, "base_path", tmp_path)
    monkeypatch.setattr(repository, "carregar_contatos", lambda: [])

    student_name = repository.resolver_nome_aluno("149357571661905@lid", push_name="")

    assert student_name == ""


def test_repository_resolver_nome_aluno_uses_phone_match_from_sent_campaign(
    monkeypatch, tmp_path: Path
) -> None:
    campaigns_dir = tmp_path / "campaigns"
    campaigns_dir.mkdir()
    sent_file = campaigns_dir / "campaign_faltas_dia_2_sent.json"
    sent_file.write_text(
        json.dumps(
            [
                {
                    "student_name": "BRYAN ENZO SILVA CAVALCANTI",
                    "status": "sent",
                    "phone": "5514981324832@s.whatsapp.net",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(repository, "base_path", tmp_path)
    monkeypatch.setattr(repository, "carregar_contatos", lambda: [])

    student_name = repository.resolver_nome_aluno("5514981324832@s.whatsapp.net", push_name="")

    assert student_name == "BRYAN ENZO SILVA CAVALCANTI"


def test_repository_resolver_contexto_aluno_by_phone_enriches_ra_and_tipo(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        repository,
        "carregar_contatos",
        lambda: [
            {
                "student_name": "BRYAN ENZO SILVA CAVALCANTI",
                "class_name": "",
                "ra": "000115238654-2/SP",
                "phone1": "14997906412",
                "phone2": "",
                "phone3": "",
                "responsible_type1": "mae",
                "responsible_type2": "",
                "responsible_type3": "",
            }
        ],
    )
    monkeypatch.setattr(
        repository,
        "_find_student_context_in_consolidated",
        lambda student_name, ra: {
            "student_name": "BRYAN ENZO SILVA CAVALCANTI",
            "class_name": "6 ANO 6A INTEGRAL 9H ANUAL",
            "ra": "000115238654-2/SP",
        },
    )

    context = repository.resolver_contexto_aluno("5514997906412@s.whatsapp.net")

    assert context == {
        "student_name": "BRYAN ENZO SILVA CAVALCANTI",
        "class_name": "6 ANO 6A INTEGRAL 9H ANUAL",
        "ra": "000115238654-2/SP",
        "tipo_responsavel": "mae",
        "numero_chamado": "14997906412",
    }


def test_repository_resolver_contexto_aluno_uses_recent_outbound_for_lid_reply(
    monkeypatch, tmp_path: Path
) -> None:
    incoming_file = tmp_path / "incoming_messages.json"
    incoming_file.write_text(
        json.dumps(
            [
                {
                    "data_hora": "2026-04-02T17:07:44",
                    "student_name": "Guilherme Usando Teste",
                    "class_name": "6 ANO 6A INTEGRAL 9H ANUAL",
                    "ra": "000112554122-4/SP",
                    "tipo_responsavel": "doce",
                    "numero_chamado": "5514981324832",
                    "telefone": "5514981324832@s.whatsapp.net",
                    "mensagem": "Teste",
                    "origem": "whatsapp_outbound",
                    "raw_payload": {"event": "send.message"},
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(repository, "incoming_messages_file", incoming_file)
    monkeypatch.setattr(repository, "carregar_contatos", lambda: [])
    monkeypatch.setattr(
        repository,
        "_find_student_context_in_consolidated",
        lambda student_name, ra: {
            "student_name": "Guilherme Usando Teste",
            "class_name": "6 ANO 6A INTEGRAL 9H ANUAL",
            "ra": "000112554122-4/SP",
        },
    )

    context = repository.resolver_contexto_aluno(
        "22153491648743@lid",
        push_name="Guilherme",
        data_hora="2026-04-02T17:09:03.619665",
    )

    assert context == {
        "student_name": "Guilherme Usando Teste",
        "class_name": "6 ANO 6A INTEGRAL 9H ANUAL",
        "ra": "000112554122-4/SP",
        "tipo_responsavel": "doce",
        "numero_chamado": "5514981324832",
    }


def test_app_has_single_webhook_route() -> None:
    webhook_routes = [
        route
        for route in app.routes
        if isinstance(route, APIRoute)
        and route.path == "/webhook/messages"
        and "POST" in route.methods
    ]

    assert len(webhook_routes) == 1
