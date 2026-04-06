import requests
import json
import sys
from datetime import datetime

def test_webhook(url="https://rosaline-chalklike-campbell.ngrok-free.dev/webhook/messages"):
    print(f"🚀 Testando Webhook em: {url}")
    
    # Simula um payload real da Evolution API
    payload = {
        "event": "messages.upsert",
        "instance": "escola-decia",
        "data": {
            "key": {
                "remoteJid": "5514997978770@s.whatsapp.net", # Número de teste
                "fromMe": False,
                "id": f"TESTE_{int(datetime.now().timestamp())}"
            },
            "pushName": "Responsavel Teste",
            "message": {
                "conversation": "Ola, levei o aluno no dentista hoje."
            },
            "messageTimestamp": int(datetime.now().timestamp())
        }
    }

    try:
        response = requests.post(
            url, 
            json=payload, 
            timeout=10,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"Status Code: {response.status_code}")
        print("Resposta do Servidor:")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
        
        if response.status_code == 200 and response.json().get("ok"):
            print("\n✅ SUCESSO! O servidor processou a mensagem.")
            print("Verifique agora se ela apareceu no Supabase ou no Google Sheets.")
        else:
            print("\n❌ FALHA: O servidor recebeu, mas retornou erro ou ignorou.")
            
    except Exception as e:
        print(f"\n❌ ERRO DE CONEXAO: {e}")
        print("Certifique-se de que o servidor (main.py) esta rodando na porta 8000.")

if __name__ == "__main__":
    target_url = sys.argv[1] if len(sys.argv) > 1 else "https://rosaline-chalklike-campbell.ngrok-free.dev/webhook/messages"
    test_webhook(target_url)
