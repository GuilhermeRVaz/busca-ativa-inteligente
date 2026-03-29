from data.repository import repository
from services.evolution_api import evolution_api_service


class CampaignService:
    def send_campaign_message(self, campaign_id: str, phone: str, text: str) -> dict:
        repository.save_campaign_event(
            campaign_id=campaign_id,
            event_type="send_attempt",
            payload={"phone": phone, "text": text},
        )

        result = evolution_api_service.send_text_message(phone=phone, text=text)

        repository.save_campaign_event(
            campaign_id=campaign_id,
            event_type="send_result",
            payload=result,
        )

        repository.save_message(
            conversation_id=phone,
            direction="outbound",
            text=text,
            metadata={"campaign_id": campaign_id, "delivery_result": result},
        )

        return {
            "campaign_id": campaign_id,
            "phone": phone,
            "result": result,
        }


campaign_service = CampaignService()
