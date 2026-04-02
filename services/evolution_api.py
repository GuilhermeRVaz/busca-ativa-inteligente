from typing import Any

import requests

from core.config import settings
from core.logging import get_logger


logger = get_logger(__name__)


class EvolutionAPIService:
    def is_configured(self) -> bool:
        return all(
            [
                settings.evolution_api_url.strip(),
                settings.evolution_api_key.strip(),
                settings.evolution_api_instance.strip(),
            ]
        )

    def get_instance_name(self) -> str:
        return settings.evolution_api_instance.strip() or "nao-configurada"

    def validate_configuration(self) -> None:
        missing = []
        if not settings.evolution_api_url.strip():
            missing.append("EVOLUTION_API_URL")
        if not settings.evolution_api_key.strip():
            missing.append("EVOLUTION_API_KEY")
        if not settings.evolution_api_instance.strip():
            missing.append("EVOLUTION_API_INSTANCE")
        if missing:
            raise ValueError(
                "Configuracao da Evolution incompleta: " + ", ".join(missing)
            )

    def send_text_message(
        self,
        phone: str,
        text: str,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        if dry_run:
            logger.info("DRY RUN EVOLUTION | phone=%s", phone)
            return {
                "success": True,
                "provider_message_id": None,
                "error": None,
                "used_fallback": False,
                "mock": True,
            }

        self.validate_configuration()

        primary_payload = {
            "number": phone,
            "text": text,
        }
        result = self._perform_send(primary_payload)
        if result["success"] or not result["fallback_needed"]:
            result["used_fallback"] = False
            return result

        fallback_payload = {
            "number": phone,
            "textMessage": {"text": text},
        }
        fallback_result = self._perform_send(fallback_payload)
        fallback_result["used_fallback"] = True
        return fallback_result

    def _perform_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = requests.post(
                self._build_send_url(),
                headers=self._build_headers(),
                json=payload,
                timeout=settings.evolution_timeout_seconds,
            )
        except requests.RequestException as exc:
            logger.warning("Falha de conexao com Evolution API: %s", exc)
            return {
                "success": False,
                "provider_message_id": None,
                "error": str(exc),
                "fallback_needed": False,
            }

        response_data = self._parse_response_json(response)
        success = self._is_successful_response(response)
        return {
            "success": success,
            "provider_message_id": (
                self._extract_provider_message_id(response_data) if success else None
            ),
            "error": None if success else response.text,
            "fallback_needed": self._should_retry_with_fallback(response, payload),
        }

    def _build_send_url(self) -> str:
        return (
            f"{settings.evolution_api_url.rstrip('/')}/"
            f"message/sendText/{settings.evolution_api_instance}"
        )

    @staticmethod
    def _build_headers() -> dict[str, str]:
        return {
            "apikey": settings.evolution_api_key,
            "Content-Type": "application/json",
        }

    @staticmethod
    def _parse_response_json(response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            return {}

    @staticmethod
    def _is_successful_response(response: requests.Response) -> bool:
        return response.status_code in [200, 201] and "error" not in response.text.lower()

    def _should_retry_with_fallback(
        self,
        response: requests.Response,
        payload: dict[str, Any],
    ) -> bool:
        return "text" in payload and not self._is_successful_response(response)

    @staticmethod
    def _extract_provider_message_id(response_data: Any) -> str | None:
        if not isinstance(response_data, dict):
            return None

        provider_message_id = (
            response_data.get("key", {}).get("id")
            or response_data.get("id")
            or response_data.get("messageId")
        )
        return str(provider_message_id) if provider_message_id else None


evolution_api_service = EvolutionAPIService()
