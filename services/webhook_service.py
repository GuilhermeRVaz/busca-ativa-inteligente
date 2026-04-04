"""
Serviço de processamento de webhooks do WhatsApp (Evolution API).

Regras de validação (aplicadas ANTES de qualquer processamento):
  1. Payload deve ser do tipo messages.upsert e não fromMe
  2. Telefone com @lid → ignorado (grupo/evento interno)
  3. Telefone vazio ou inválido → ignorado
  4. Mensagem duplicada (message_id já registrado) → ignorada

Fluxo de dados (UMA classificação, replicada para todos os destinos):
  payload → normaliza_phone → classifica → salvar_interacao (JSON + Sheets + Supabase)
"""

from datetime import datetime
from typing import Any

from ai.classifier import classificar_mensagem
from core.logging import get_logger
from data.repository import repository


logger = get_logger(__name__)

_IGNORED_RESULT_TEMPLATE: dict[str, str] = {
    "telefone": "",
    "mensagem": "",
    "classificacao": "IGNORADO",
    "intencao": "IGNORADO",
    "motivo": "IGNORADO",
    "observacao": "",
    "student_name": "",
    "class_name": "",
    "ra": "",
    "tipo_responsavel": "",
    "campaign_id": "",
    "origem": "whatsapp",
}


def _make_ignored(observacao: str) -> dict[str, str]:
    result = dict(_IGNORED_RESULT_TEMPLATE)
    result["observacao"] = observacao
    result["data_hora"] = datetime.now().isoformat()
    return result


class WebhookService:
    def process_incoming(self, payload: dict[str, Any]) -> dict[str, str]:
        logger.info("Webhook recebido")

        # ── Filtro 1: evento elegível? ──────────────────────────────────────
        if not self._should_process_payload(payload):
            logger.info("Webhook ignorado | evento nao elegivel para interacao")
            return _make_ignored("evento ignorado")

        # ── Filtro 2: validar telefone ──────────────────────────────────────
        telefone = self._normalize_and_validate_phone(payload)
        if telefone is None:
            # log já foi emitido dentro do método
            return _make_ignored("telefone invalido ou grupo")

        # ── Filtro 3: idempotência ──────────────────────────────────────────
        if repository.interacao_ja_registrada(payload):
            logger.info("Webhook ignorado | mensagem duplicada | telefone=%s", telefone)
            return _make_ignored("mensagem duplicada")

        # ── Extrair dados ───────────────────────────────────────────────────
        mensagem = self._extract_message(payload)
        campaign_id = self._extract_campaign_id(payload)
        push_name = self._extract_push_name(payload)
        data_hora = datetime.now().isoformat()

        context = repository.resolver_contexto_aluno(
            telefone,
            student_name="",
            push_name=push_name,
            data_hora=data_hora,
        )
        student_name = context.get("student_name", "")
        class_name = context.get("class_name", "")
        ra = context.get("ra", "")
        tipo_responsavel = context.get("tipo_responsavel", "")
        numero_chamado = context.get("numero_chamado", "")

        if not mensagem:
            logger.warning("Webhook recebido sem mensagem textual | telefone=%s", telefone)

        if not student_name:
            logger.warning(
                "nome_aluno_nao_resolvido | telefone=%s | push_name=%s",
                telefone,
                push_name,
            )

        # ── Classificação (UMA vez, resultado é a fonte única de verdade) ───
        classificacao = classificar_mensagem(mensagem)
        intencao = classificacao["intencao"]
        motivo = classificacao["motivo"]
        observacao = classificacao["observacao"]

        # ── Persistir (Supabase + Sheets + JSON via repositório) ─────────────
        repository.salvar_interacao(
            {
                "numero_chamado": numero_chamado,
                "identificador_remetente": telefone,
                "telefone": telefone,
                "mensagem": mensagem,
                "classificacao": intencao,
                "intencao": intencao,
                "motivo": motivo,
                "observacao": observacao,
                "student_name": student_name,
                "class_name": class_name,
                "ra": ra,
                "tipo_responsavel": tipo_responsavel,
                "data_hora": data_hora,
                "campaign_id": campaign_id,
                "origem": "whatsapp",
                "raw_payload": payload,
            }
        )

        logger.info(
            "Webhook processado | telefone=%s | aluno=%s | intencao=%s | motivo=%s | campaign=%s",
            telefone,
            student_name or "(desconhecido)",
            intencao,
            motivo,
            campaign_id or "(sem campanha)",
        )

        return {
            "telefone": telefone,
            "mensagem": mensagem,
            "classificacao": intencao,
            "intencao": intencao,
            "motivo": motivo,
            "observacao": observacao,
            "student_name": student_name,
            "class_name": class_name,
            "ra": ra,
            "tipo_responsavel": tipo_responsavel,
            "numero_chamado": numero_chamado,
            "identificador_remetente": telefone,
            "campaign_id": campaign_id,
            "origem": "whatsapp",
            "data_hora": data_hora,
        }

    # ── Helpers privados ────────────────────────────────────────────────────

    def _normalize_and_validate_phone(self, payload: dict[str, Any]) -> str | None:
        """
        Normaliza e valida o telefone do payload.
        Retorna o telefone limpo (apenas dígitos, com 55...) ou None se inválido.

        Regras:
          - @lid → None (grupo / evento interno)
          - @s.whatsapp.net → remove sufixo
          - resultado com < 10 dígitos → None (inválido)
          - adiciona prefixo 55 se ausente
        """
        raw = (
            payload.get("data", {}).get("key", {}).get("remoteJid")
            or payload.get("phone")
            or payload.get("from")
            or ""
        )
        raw = str(raw).strip()

        # Grupos e eventos internos da Evolution API
        if "@lid" in raw:
            logger.info("telefone ignorado (grupo) | raw=%s", raw)
            return None

        # Remover sufixo WhatsApp e caracteres especiais
        cleaned = raw.replace("@s.whatsapp.net", "").replace("+", "").strip()
        digits = "".join(ch for ch in cleaned if ch.isdigit())

        if len(digits) < 10:
            logger.warning("telefone invalido | raw=%s | digits=%s", raw, digits)
            return None

        # Garantir prefixo Brasil
        if not digits.startswith("55"):
            digits = f"55{digits}"

        return digits

    def _extract_message(self, payload: dict[str, Any]) -> str:
        message = payload.get("data", {}).get("message", {})
        if isinstance(message, dict):
            conversation = message.get("conversation")
            if conversation is not None:
                return str(conversation)
            # Suporte a outros tipos de mensagem (extendedTextMessage, etc.)
            extended = message.get("extendedTextMessage", {})
            if isinstance(extended, dict) and extended.get("text"):
                return str(extended["text"])
        return str(payload.get("text") or payload.get("message") or "")

    def _extract_campaign_id(self, payload: dict[str, Any]) -> str:
        data = payload.get("data", {})
        message = data.get("message", {})
        candidates = [
            payload.get("campaign_id"),
            payload.get("campaignId"),
            data.get("campaign_id"),
            data.get("campaignId"),
            message.get("campaign_id") if isinstance(message, dict) else None,
            message.get("campaignId") if isinstance(message, dict) else None,
        ]
        for value in candidates:
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def _extract_push_name(self, payload: dict[str, Any]) -> str:
        return str(payload.get("data", {}).get("pushName", "")).strip()

    def _should_process_payload(self, payload: dict[str, Any]) -> bool:
        return repository._should_persist_incoming_payload(payload)


webhook_service = WebhookService()
