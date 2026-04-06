import requests
import sys
from datetime import datetime
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from core.config import settings
from services.webhook_service import webhook_service
from core.logging import get_logger

logger = get_logger("backfill")

def run_backfill():
    base_url = settings.evolution_api_url.rstrip('/')
    instance_name = settings.evolution_api_instance
    # Usando o ID que descobrimos no diagnóstico
    instance_id = "2359a269-03b8-49a1-829a-66467239f1ce" 
    
    headers = {
        "apikey": settings.evolution_api_key,
        "Content-Type": "application/json"
    }

    # Tentativas focadas na V2 da Evolution
    attempts = [
        # 1. Rota padrão de busca (POST)
        {
            "url": f"{base_url}/chat/findMessages/{instance_name}", 
            "method": "POST", 
            "payload": {"limit": 50, "where": {}}
        },
        # 2. Rota via ID da instancia (POST)
        {
            "url": f"{base_url}/chat/findMessages/{instance_id}", 
            "method": "POST", 
            "payload": {"limit": 50, "where": {}}
        },
        # 3. Rota simplificada (GET)
        {
            "url": f"{base_url}/chat/findMessages/{instance_name}?limit=50", 
            "method": "GET"
        },
    ]

    messages = []
    print(f"[*] Iniciando recuperacao de mensagens...")

    for attempt in attempts:
        url = attempt["url"]
        method = attempt["method"]
        try:
            if method == "POST":
                res = requests.post(url, headers=headers, json=attempt["payload"], timeout=15)
            else:
                res = requests.get(url, headers=headers, timeout=15)

            if res.status_code == 200:
                data = res.json()
                # A V2 costuma retornar { "messages": { "records": [...] } } ou apenas lista
                if isinstance(data, list):
                    messages = data
                elif isinstance(data, dict):
                    messages = data.get("records", []) or data.get("messages", {}).get("records", []) or data.get("messages", [])
                
                if messages:
                    print(f"[OK] Encontradas {len(messages)} mensagens via {url}")
                    break
        except Exception as e:
            continue

    if not messages:
        print("[-] Nao conseguimos extrair mensagens. Tentando fallback...")
        # Fallback: buscar chats e depois mensagens de cada chat (mais pesado, mas garantido)
        try:
            res_chats = requests.get(f"{base_url}/chat/findChats/{instance_name}", headers=headers, timeout=10)
            if res_chats.status_code == 200:
                chats = res_chats.json()
                print(f"[*] Encontrados {len(chats)} chats. Buscando mensagens...")
                for chat in chats[:10]: # Limita aos 10 primeiros para teste rápido
                    jid = chat.get("id") or chat.get("remoteJid")
                    res_m = requests.post(f"{base_url}/chat/findMessages/{instance_name}", 
                                        headers=headers, 
                                        json={"where": {"key": {"remoteJid": jid}}, "limit": 5}, 
                                        timeout=5)
                    if res_m.status_code == 200:
                        m_data = res_m.json()
                        m_list = m_data if isinstance(m_data, list) else m_data.get("records", [])
                        messages.extend(m_list)
        except:
            pass

    if not messages:
        print("[-] Todas as tentativas de busca falharam.")
        return

    print(f"[*] Processando {len(messages)} mensagens para o banco...")
    count = 0
    for m in messages:
        if m.get("key", {}).get("fromMe"): continue
        
        payload = {"event": "messages.upsert", "instance": instance_name, "data": m}
        try:
            r = webhook_service.process_incoming(payload)
            if r.get("received") and r.get("classification") != "IGNORADO":
                count += 1
        except:
            pass

    print(f"[OK] Sucesso! {count} mensagens recuperadas e classificadas no sistema.")

if __name__ == "__main__":
    run_backfill()
