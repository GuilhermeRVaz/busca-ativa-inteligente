from datetime import datetime
from typing import Any

from ai.classifier import classificar_mensagem
from core.logging import get_logger
from data.repository import repository


logger = get_logger(__name__)


class WebhookService:
    def process_incoming(self, payload: dict[str, Any]) -> dict[str, str]:
        logger.info("Webhook recebido")

        telefone = self._extract_phone(payload)
        mensagem = self._extract_message(payload)
        campaign_id = self._extract_campaign_id(payload)
        data_hora = datetime.now().isoformat()

        if not mensagem:
            logger.warning("Webhook recebido sem mensagem textual; usando mensagem vazia")

        logger.info("Telefone extraido: %s", telefone)
        logger.info("Mensagem extraida: %s", mensagem)

        classificacao = classificar_mensagem(mensagem)
        intencao = classificacao.get("intencao", "DUVIDA")
        motivo = classificacao.get("motivo", "OUTRO")
        observacao = classificacao.get("observacao", "nao foi possivel classificar")

        repository.salvar_interacao(
            {
                "telefone": telefone,
                "mensagem": mensagem,
                "classificacao": intencao,
                "intencao": intencao,
                "motivo": motivo,
                "observacao": observacao,
                "data_hora": data_hora,
                "campaign_id": campaign_id,
                "origem": "whatsapp",
                "raw_payload": payload,
            }
        )

        logger.info(
            "Webhook processado | telefone=%s | classificacao=%s | motivo=%s | campaign_id=%s | data_hora=%s",
            telefone,
            intencao,
            motivo,
            campaign_id,
            data_hora,
        )

        return {
            "telefone": telefone,
            "mensagem": mensagem,
            "classificacao": intencao,
            "intencao": intencao,
            "motivo": motivo,
            "observacao": observacao,
            "campaign_id": campaign_id,
            "origem": "whatsapp",
            "data_hora": data_hora,
        }

    def _extract_phone(self, payload: dict[str, Any]) -> str:
        remote_jid = (
            payload.get("data", {}).get("key", {}).get("remoteJid")
            or payload.get("phone")
            or payload.get("from")
            or "unknown"
        )
        return str(remote_jid).replace("@s.whatsapp.net", "").replace("+", "").strip()

    def _extract_message(self, payload: dict[str, Any]) -> str:
        message = payload.get("data", {}).get("message", {})
        conversation = message.get("conversation")
        if conversation is None:
            return str(payload.get("text") or payload.get("message") or "")
        return str(conversation)

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


webhook_service = WebhookService()
