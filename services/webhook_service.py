from datetime import datetime
from typing import Any

from ai.classifier import classificar_mensagem
from core.logging import get_logger
from data.repository import repository


logger = get_logger(__name__)


class WebhookService:
    def process_incoming(self, payload: dict[str, Any]) -> dict[str, str]:
        logger.info("Webhook recebido")

        if not self._should_process_payload(payload):
            logger.info("Webhook ignorado | evento nao elegivel para interacao")
            return {
                "telefone": "",
                "mensagem": "",
                "classificacao": "IGNORADO",
                "intencao": "IGNORADO",
                "motivo": "IGNORADO",
                "observacao": "evento ignorado",
                "student_name": "",
                "class_name": "",
                "ra": "",
                "tipo_responsavel": "",
                "campaign_id": "",
                "origem": "whatsapp",
                "data_hora": datetime.now().isoformat(),
            }

        if repository.interacao_ja_registrada(payload):
            logger.info("Webhook ignorado | mensagem duplicada")
            return {
                "telefone": "",
                "mensagem": "",
                "classificacao": "IGNORADO",
                "intencao": "IGNORADO",
                "motivo": "IGNORADO",
                "observacao": "mensagem duplicada",
                "student_name": "",
                "class_name": "",
                "ra": "",
                "tipo_responsavel": "",
                "campaign_id": "",
                "origem": "whatsapp",
                "data_hora": datetime.now().isoformat(),
            }

        telefone = self._extract_phone(payload)
        mensagem = self._extract_message(payload)
        campaign_id = self._extract_campaign_id(payload)
        push_name = self._extract_push_name(payload)
        context = repository.resolver_contexto_aluno(
            telefone,
            student_name="",
            push_name=push_name,
            data_hora=datetime.now().isoformat(),
        )
        student_name = context.get("student_name", "")
        class_name = context.get("class_name", "")
        ra = context.get("ra", "")
        tipo_responsavel = context.get("tipo_responsavel", "")
        message_id = repository._extract_message_id(payload)
        data_hora = datetime.now().isoformat()

        if not mensagem:
            logger.warning("Webhook recebido sem mensagem textual; usando mensagem vazia")

        if not student_name:
            logger.warning(
                "nome_aluno_nao_resolvido | telefone=%s | push_name=%s | message_id=%s",
                telefone,
                push_name,
                message_id,
            )

        logger.info("Telefone extraido: %s", telefone)
        logger.info("Mensagem extraida: %s", mensagem)

        classificacao = classificar_mensagem(mensagem)
        intencao = classificacao.get("intencao", "DUVIDA")
        motivo = classificacao.get("motivo", "OUTRO")
        observacao = classificacao.get("observacao", "nao foi possivel classificar")

        repository.save_message(
            conversation_id=telefone,
            direction="inbound",
            text=mensagem,
            metadata={
                "campaign_id": campaign_id,
                "student_name": student_name,
                "class_name": class_name,
                "ra": ra,
                "tipo_responsavel": tipo_responsavel,
                "intencao": intencao,
                "motivo": motivo,
                "observacao": observacao,
                "raw_payload": payload,
            },
        )

        repository.salvar_interacao(
            {
                "numero_chamado": context.get("numero_chamado", ""),
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
            "Webhook processado | telefone=%s | aluno=%s | classificacao=%s | motivo=%s | campaign_id=%s | data_hora=%s",
            telefone,
            student_name,
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
            "student_name": student_name,
            "class_name": class_name,
            "ra": ra,
            "tipo_responsavel": tipo_responsavel,
            "numero_chamado": context.get("numero_chamado", ""),
            "identificador_remetente": telefone,
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

    def _extract_push_name(self, payload: dict[str, Any]) -> str:
        return str(payload.get("data", {}).get("pushName", "")).strip()

    def _should_process_payload(self, payload: dict[str, Any]) -> bool:
        return repository._should_persist_incoming_payload(payload)


webhook_service = WebhookService()
