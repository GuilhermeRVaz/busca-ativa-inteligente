import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.sender import send_campaign
import time

# 👇 primeiro define a campanha
campaign = [
    {
        "campaign_id": "manual-test",
        "student_name": "Teste 1",
        "class_name": "1A",
        "phone": "14981324832",
        "message": "oi, estou testando o agente de busca ativa da escola Déciao",
        "status": "pending",
    },
    {
        "campaign_id": "manual-test",
        "student_name": "Teste 2",
        "class_name": "2A",
        "phone": "14982307099",
        "message": "Boa tarde, tudo bem? Estou testando o agente de busca ativa da escola Déciao? funcionou?",
        "status": "pending",
    }
]

# 👇 depois executa com delay
for item in campaign:
    print(f"Enviando para {item['phone']}...")
    send_campaign([item])
    time.sleep(30)

print("✅ Envio finalizado")