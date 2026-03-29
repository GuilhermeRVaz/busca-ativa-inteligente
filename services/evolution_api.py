from typing import Any

import requests

from core.config import settings
from core.logging import get_logger


logger = get_logger(__name__)


class EvolutionAPIService:
    def send_text_message(self, phone: str, text: str) -> dict[str, Any]:
        if not settings.evolution_api_url:
            logger.info("Evolution API URL not configured. Returning mock response.")
            return {
                "success": True,
                "mock": True,
                "phone": phone,
                "text": text,
            }

        url = (
            f"{settings.evolution_api_url.rstrip('/')}/"
            f"message/sendText/{settings.evolution_api_instance}"
        )
        headers = {
            "apikey": settings.evolution_api_key,
            "Content-Type": "application/json",
        }
        payload = {"number": phone, "text": text}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=15)
            response.raise_for_status()
            return {"success": True, "response": response.json()}
        except requests.RequestException as exc:
            logger.exception("Failed to send message using Evolution API.")
            return {"success": False, "error": str(exc)}


evolution_api_service = EvolutionAPIService()
