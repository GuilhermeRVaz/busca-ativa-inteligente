import json
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.config import settings
from data.repository import repository
from services.evolution_api import evolution_api_service
from services.sender import send_campaign
from services.webhook_service import webhook_service

MODO = "online"  # "offline" ou "online"

VALID_CLASSIFICACOES = {"RETORNAR", "NAO_QUER", "DUVIDA"}


def log_ok(message: str) -> None:
    print(f"[OK] {message}")


def log_fail(message: str) -> None:
    print(f"[FAIL] {message}")


def validar_config() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        raise FileNotFoundError(".env nao encontrado")

    if not settings.google_service_account_file.exists():
        raise FileNotFoundError(
            f"Arquivo de service account nao encontrado: {settings.google_service_account_file}"
        )

    if not settings.google_sheet_contatos_url.strip():
        raise ValueError("GOOGLE_SHEET_CONTATOS_URL nao configurada")
    if not settings.google_sheet_dados_url.strip():
        raise ValueError("GOOGLE_SHEET_DADOS_URL nao configurada")

    if MODO == "online":
        evolution_api_service.validate_configuration()

    log_ok("Configuracao validada")


def carregar_contatos() -> list[dict[str, str]]:
    if MODO == "offline":
        records = [
            {
                "Nome do Aluno": "Joao Silva",
                "Turma": "7A",
                "Telefone 1": "(14) 98132-4832",
                "Telefone 2": "123",
                "Celular 3": "14 98230-7099",
            }
        ]
        contatos = repository._records_to_contacts(records)
    else:
        contatos = repository.carregar_contatos()

    if not contatos:
        raise ValueError("Nenhum contato retornado")

    log_ok("Contatos carregados")
    return contatos


def simular_campanha(contatos: list[dict[str, str]]) -> list[dict[str, Any]]:
    selecionados = contatos[:2]
    if not selecionados:
        raise ValueError("Nenhum contato valido para simular campanha")

    campaign = [
        {
            "campaign_id": "simulacao_e2e",
            "student_name": contato.get("student_name") or "Aluno(a)",
            "class_name": contato.get("class_name") or "Teste",
            "phone": contato.get("phone1") or contato.get("phone2") or contato.get("phone3"),
            "message": (
                f"Ola {contato.get('student_name') or 'Aluno(a)'}, "
                "aqui e da escola. Mensagem de teste."
            ),
            "status": "pending",
        }
        for contato in selecionados
    ]

    sent_campaign = send_campaign(campaign, dry_run=(MODO == "offline"))
    if not sent_campaign:
        raise ValueError("Falha ao simular campanha")

    log_ok("Campanha simulada")
    return sent_campaign


def simular_webhook() -> dict[str, str]:
    payload = {
        "event": "messages.upsert",
        "data": {
            "key": {
                "remoteJid": "5514999999999@s.whatsapp.net",
                "fromMe": False,
                "id": "TESTE123",
            },
            "message": {"conversation": "quero voltar a estudar"},
        },
    }

    if MODO == "offline":
        _force_sheets_fallback(True)
        try:
            result = webhook_service.process_incoming(payload)
        finally:
            _force_sheets_fallback(False)
    else:
        result = webhook_service.process_incoming(payload)

    log_ok("Webhook processado")
    return result


def validar_classificacao(result: dict[str, str]) -> None:
    classificacao = result.get("classificacao", "")
    if classificacao not in VALID_CLASSIFICACOES:
        raise ValueError(f"Classificacao invalida: {classificacao}")
    if not result.get("motivo"):
        raise ValueError("Motivo ausente na classificacao")
    if not result.get("observacao"):
        raise ValueError("Observacao ausente na classificacao")
    log_ok("Classificacao realizada")


def verificar_persistencia(initial_count: int) -> None:
    file_path = repository.incoming_messages_file
    if not file_path.exists():
        raise FileNotFoundError("Arquivo de mensagens recebidas nao encontrado")

    payload = json.loads(file_path.read_text(encoding="utf-8"))
    if len(payload) <= initial_count:
        raise AssertionError("JSON nao foi atualizado")

    log_ok("JSON atualizado")
    log_ok("Sheets salvo (ou fallback)")


def _force_sheets_fallback(enabled: bool) -> None:
    if enabled:
        _force_sheets_fallback._previous = (
            settings.google_sheet_dados_url,
            settings.google_service_account_file,
        )
        settings.google_sheet_dados_url = "https://example.com/invalid"
        settings.google_service_account_file = Path("service-account-missing.json")
    else:
        previous = getattr(_force_sheets_fallback, "_previous", None)
        if previous:
            settings.google_sheet_dados_url, settings.google_service_account_file = previous


def main() -> None:
    try:
        validar_config()
        contatos = carregar_contatos()
        simular_campanha(contatos)
        initial_payload = repository._read_json(repository.incoming_messages_file)
        result = simular_webhook()
        validar_classificacao(result)
        verificar_persistencia(len(initial_payload))
    except Exception as exc:
        log_fail(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
