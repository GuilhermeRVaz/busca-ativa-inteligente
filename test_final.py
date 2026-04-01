from services.sender import send_campaign
import time

campaign = [
    {
        "campaign_id": "manual-test",
        "student_name": "Teste 1",
        "class_name": "1A",
        "phone": "14981324832",
        "message": "teste final evolution",
        "status": "pending",
    }
]

print(send_campaign(campaign))