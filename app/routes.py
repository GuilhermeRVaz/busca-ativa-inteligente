from fastapi import APIRouter, HTTPException

from services.campaign_service import campaign_service
from services.webhook_service import webhook_service


router = APIRouter()


@router.get("/health")
def health_check() -> dict:
    return {"status": "ok", "service": "busca-ativa-inteligente"}


@router.post("/webhook/messages")
def receive_message(payload: dict) -> dict:
    result = webhook_service.process_incoming(payload)

    return {
        "ok": True,
        "received": True,
        "classification": result["classificacao"],
        "telefone": result["telefone"],
        "data_hora": result["data_hora"],
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
