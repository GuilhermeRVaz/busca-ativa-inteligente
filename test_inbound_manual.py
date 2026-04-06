import sys
import os

# Adiciona a raiz do projeto no path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from services.webhook_service import webhook_service

def run_test():
    payload = {
        "event": "messages.upsert",
        "data": {
            "key": {
                "remoteJid": "5511999999999@s.whatsapp.net",
                "fromMe": False
            },
            "message": {
                "conversation": "levei no dentista"
            },
            "pushName": "Teste Aluno"
        },
        "campaign_id": "teste-inbound-123",
        "phone": "5511999999999"
    }

    print("Testando fluxo INBOUND...")
    result = webhook_service.process_incoming(payload)
    print("\nResultado do processamento:")
    for k, v in result.items():
        print(f"{k}: {v}")
    print("\nVerifique se os dados apareceram no Supabase, no Google Sheets e no JSON.")

if __name__ == "__main__":
    run_test()
