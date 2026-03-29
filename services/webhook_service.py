class WebhookService:
    def parse_incoming(self, payload: dict) -> dict | None:
        # Accepts a simple generic payload and normalizes it.
        phone = payload.get("phone") or payload.get("from")
        text = payload.get("text") or payload.get("message")

        if not phone or not text:
            return None

        return {
            "conversation_id": str(phone),
            "phone": str(phone),
            "text": str(text),
        }


webhook_service = WebhookService()
