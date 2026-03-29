from fastapi import APIRouter, HTTPException

from ai.classifier import classify_message
from ai.responder import generate_reply
from data.repository import repository
from services.campaign_service import campaign_service
from services.webhook_service import webhook_service


router = APIRouter()


@router.get("/health")
def health_check() -> dict:
    return {"status": "ok", "service": "busca-ativa-inteligente"}


@router.post("/webhook/messages")
def receive_message(payload: dict) -> dict:
    message = webhook_service.parse_incoming(payload)
    if not message:
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    classification = classify_message(message["text"])
    reply = generate_reply(message["text"], classification)

    repository.save_message(
        conversation_id=message["conversation_id"],
        direction="inbound",
        text=message["text"],
        metadata={"classification": classification},
    )
    repository.save_message(
        conversation_id=message["conversation_id"],
        direction="outbound",
        text=reply,
        metadata={"source": "ai"},
    )

    return {
        "received": True,
        "classification": classification,
        "reply": reply,
    }


@router.post("/campaigns/send")
def send_campaign_message(payload: dict) -> dict:
    campaign_id = payload.get("campaign_id", "default-campaign")
    phone = payload.get("phone")
    text = payload.get("text", "Hello from busca-ativa-inteligente.")

    if not phone:
        raise HTTPException(status_code=400, detail="Field 'phone' is required")

    result = campaign_service.send_campaign_message(
        campaign_id=campaign_id,
        phone=phone,
        text=text,
    )
    return result
