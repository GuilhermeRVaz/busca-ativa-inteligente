"""
Camada de persistência Supabase para o sistema de Busca Ativa Inteligente.

Regras:
- Se SUPABASE_URL não estiver configurada → módulo inteiro desativado (noop).
- Toda operação é envolta em try/except → loga erro, NÃO quebra o fluxo.
- Inserções são idempotentes (verifica duplicata antes de inserir).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Singleton do cliente Supabase
# ---------------------------------------------------------------------------
_client = None
_initialized = False


def _get_client():
    """Retorna o cliente Supabase (singleton). None se desativado."""
    global _client, _initialized

    if _initialized:
        return _client

    _initialized = True

    from core.config import settings

    url = getattr(settings, "supabase_url", "")
    key = getattr(settings, "supabase_key", "")

    if not url or not key:
        logger.info("supabase desativado | SUPABASE_URL ou SUPABASE_KEY nao configurada")
        return None

    try:
        from supabase import create_client

        _client = create_client(url, key)
        logger.info("supabase conectado | url=%s", url)
    except Exception as exc:
        logger.error("falha ao conectar supabase | erro=%s", exc)
        _client = None

    return _client


# ---------------------------------------------------------------------------
# Normalização de telefone
# ---------------------------------------------------------------------------

def normalize_phone(phone: Any) -> str:
    """
    Normaliza telefone para formato apenas dígitos.
    - Remove @s.whatsapp.net
    - Ignora @lid (retorna vazio)
    - Retorna apenas dígitos no formato 5514...
    """
    text = str(phone or "").strip()

    if "@lid" in text:
        return ""

    text = text.replace("@s.whatsapp.net", "").replace("+", "").strip()
    digits = "".join(char for char in text if char.isdigit())

    if len(digits) < 10:
        return ""

    if not digits.startswith("55"):
        digits = f"55{digits}"

    return digits


# ---------------------------------------------------------------------------
# Operações com tabela messages
# ---------------------------------------------------------------------------

def salvar_mensagem(data: dict[str, Any]) -> bool:
    """
    Insere uma mensagem na tabela 'messages'.
    Idempotente: ignora se message_id já existe.
    Retorna True se salvou, False caso contrário.
    """
    client = _get_client()
    if client is None:
        return False

    message_id = str(data.get("id", "") or data.get("message_id", "")).strip()
    if not message_id:
        logger.warning("supabase | mensagem sem id, ignorando")
        return False

    # Validar telefone antes de salvar
    telefone_raw = data.get("telefone", "")
    telefone_check = normalize_phone(telefone_raw)
    if not telefone_check:
        logger.warning("supabase | telefone invalido | id=%s | raw=%s | ignorando", message_id, telefone_raw)
        return False

    try:
        # Verificar duplicata
        existing = (
            client.table("messages")
            .select("id")
            .eq("id", message_id)
            .execute()
        )
        if existing.data:
            logger.info("supabase | mensagem duplicada ignorada | id=%s", message_id)
            return False

        telefone = normalize_phone(data.get("telefone", ""))
        row = {
            "id": message_id,
            "telefone": telefone,
            "ra": str(data.get("ra", "") or "").strip(),
            "nome_aluno": str(data.get("nome_aluno", "") or data.get("student_name", "")).strip(),
            "turma": str(data.get("turma", "") or data.get("class_name", "")).strip(),
            "direcao": str(data.get("direcao", "") or data.get("direction", "")).strip() or None,
            "tipo": _resolve_tipo(data),
            "mensagem": str(data.get("mensagem", "") or data.get("text", "")).strip(),
            "tipo_resposta": _resolve_tipo_resposta(data),
            "motivo": _resolve_motivo(data),
            "campaign_id": str(data.get("campaign_id", "")).strip() or None,
        }

        client.table("messages").insert(row).execute()
        logger.info("salvo no supabase | tabela=messages | id=%s | telefone=%s", message_id, telefone)
        return True

    except Exception as exc:
        logger.error("erro ao salvar no supabase | tabela=messages | id=%s | erro=%s", message_id, exc)
        return False


# ---------------------------------------------------------------------------
# Operações com tabela students
# ---------------------------------------------------------------------------

def atualizar_student(data: dict[str, Any]) -> bool:
    """
    Upsert na tabela 'students' usando telefone como chave primária.
    Incrementa total_interacoes.
    Retorna True se atualizou, False caso contrário.
    """
    client = _get_client()
    if client is None:
        return False

    telefone = normalize_phone(data.get("telefone", ""))
    if not telefone:
        logger.warning("supabase | telefone invalido ou vazio | ignorando student upsert")
        return False

    try:
        # Verificar se já existe para saber total_interacoes atual
        existing = (
            client.table("students")
            .select("total_interacoes")
            .eq("telefone", telefone)
            .execute()
        )

        current_total = 0
        if existing.data:
            current_total = int(existing.data[0].get("total_interacoes", 0) or 0)

        # Resolver status a partir da classificação/intenção
        status = _resolve_student_status(data)

        row = {
            "telefone": telefone,
            "nome": str(data.get("nome_aluno", "") or data.get("student_name", "")).strip() or None,
            "turma": str(data.get("turma", "") or data.get("class_name", "")).strip() or None,
            "ra": str(data.get("ra", "")).strip() or None,
            "status": status,
            "ultima_interacao": datetime.now().isoformat(),
            "total_interacoes": current_total + 1,
        }

        client.table("students").upsert(row, on_conflict="telefone").execute()  # noqa: typo-fixed
        logger.info(
            "salvo no supabase | tabela=students | telefone=%s | status=%s | total=%s",
            telefone,
            status,
            current_total + 1,
        )
        return True

    except Exception as exc:
        logger.error("erro ao salvar no supabase | tabela=students | telefone=%s | erro=%s", telefone, exc)
        return False


# ---------------------------------------------------------------------------
# Operações com tabela campaigns
# ---------------------------------------------------------------------------

def registrar_campaign(data: dict[str, Any]) -> bool:
    """
    Insere ou atualiza uma campanha na tabela 'campaigns'.
    Retorna True se salvou, False caso contrário.
    """
    client = _get_client()
    if client is None:
        return False

    campaign_id = str(data.get("campaign_id", "")).strip()
    if not campaign_id:
        return False

    try:
        row = {
            "campaign_id": campaign_id,
            "tipo": str(data.get("tipo", "") or data.get("event_type", "")).strip() or None,
            "total_enviados": int(data.get("total_enviados", 0) or 0),
            "total_respostas": int(data.get("total_respostas", 0) or 0),
            "total_justificativas": int(data.get("total_justificativas", 0) or 0),
        }

        client.table("campaigns").upsert(row, on_conflict="campaign_id").execute()
        logger.info("salvo no supabase | tabela=campaigns | campaign_id=%s", campaign_id)
        return True

    except Exception as exc:
        logger.error(
            "erro ao salvar no supabase | tabela=campaigns | campaign_id=%s | erro=%s",
            campaign_id,
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

_VALID_TIPO_RESPOSTA = {
    "JUSTIFICOU",
    "VAI_REGULARIZAR",
    "DUVIDA",
    "RESISTENCIA",
    "NAO_IDENTIFICADO",
    "OUTBOUND",  # usado para mensagens de saída (campanha)
}

_VALID_MOTIVO = {
    "SAUDE",
    "TRANSPORTE",
    "FAMILIAR",
    "ESCOLAR",
    "LOGISTICA",
    "OUTROS",
}

_VALID_STUDENT_STATUS = {
    "EM_RISCO",
    "CONTATADO",
    "JUSTIFICOU",
    "VAI_RETORNAR",
    "RESISTENCIA",
}


def _resolve_tipo(data: dict[str, Any]) -> str | None:
    """Resolve o tipo da mensagem (texto, audio, imagem, sistema)."""
    tipo = str(data.get("tipo", "")).strip().lower()
    if tipo in ("texto", "audio", "imagem", "sistema"):
        return tipo
    # Se não fornecido, inferir de 'mensagem' existente
    return "texto"


def _resolve_tipo_resposta(data: dict[str, Any]) -> str | None:
    """Resolve tipo_resposta a partir de intencao/classificacao."""
    # Primeiro tenta o campo direto
    tipo_resposta = str(data.get("tipo_resposta", "")).strip().upper()
    if tipo_resposta in _VALID_TIPO_RESPOSTA:
        return tipo_resposta

    # Tenta mapear da intenção/classificação
    intencao = str(data.get("intencao", "") or data.get("classificacao", "")).strip().upper()
    if intencao in _VALID_TIPO_RESPOSTA:
        return intencao

    # Se for outbound
    direcao = str(data.get("direcao", "") or data.get("direction", "") or data.get("origem", "")).strip().lower()
    if "outbound" in direcao:
        return "OUTBOUND"

    return "NAO_IDENTIFICADO"


def _resolve_motivo(data: dict[str, Any]) -> str | None:
    """Resolve motivo validando contra o CHECK constraint."""
    motivo = str(data.get("motivo", "")).strip().upper()
    if motivo in _VALID_MOTIVO:
        return motivo

    # Mapear "OUTRO" → "OUTROS" (nome do campo no DB)
    if motivo in ("OUTRO", ""):
        return "OUTROS"

    return "OUTROS"


def _resolve_student_status(data: dict[str, Any]) -> str:
    """Resolve o status do aluno baseado na interação."""
    intencao = str(data.get("intencao", "") or data.get("classificacao", "")).strip().upper()

    mapping = {
        "JUSTIFICOU": "JUSTIFICOU",
        "VAI_REGULARIZAR": "VAI_RETORNAR",
        "RESISTENCIA": "RESISTENCIA",
        "DUVIDA": "CONTATADO",
        "NAO_IDENTIFICADO": "CONTATADO",
        "OUTBOUND": "CONTATADO",
    }

    return mapping.get(intencao, "CONTATADO")


def list_contacts() -> list[dict[str, Any]]:
    """
    Busca todos os contatos da tabela 'contacts' no Supabase.
    Mapeia para o formato esperado pelo campaign_engine (student_name, phone1, etc).
    """
    client = _get_client()
    if client is None:
        logger.warning("supabase_repository.list_contacts | cliente não inicializado, retornando lista vazia")
        return []

    try:
        response = client.table("contacts").select("*").execute()
        raw_contacts = response.data or []
        
        normalized_contacts = []
        for rc in raw_contacts:
            normalized_contacts.append({
                "student_name": rc.get("nome_aluno", ""),
                "ra": rc.get("ra", ""),
                "class_name": rc.get("turma", ""),
                "phone1": rc.get("telefone_1", ""),
                "phone2": rc.get("telefone_2", ""),
                "phone3": rc.get("telefone_3", ""),
                "responsavel_1": rc.get("responsavel_1", ""),
                "responsavel_2": rc.get("responsavel_2", ""),
                "responsavel_3": rc.get("responsavel_3", ""),
            })
            
        logger.info(f"supabase_repository.list_contacts | carregou {len(normalized_contacts)} contatos")
        return normalized_contacts
    except Exception as exc:
        logger.error(f"Erro ao listar contatos no supabase | erro={exc}")
        return []

