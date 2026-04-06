import requests
import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from core.config import settings

def diagnose_evolution():
    base_url = settings.evolution_api_url.rstrip('/')
    headers = {"apikey": settings.evolution_api_key}
    
    print(f"🕵️ Investigando Evolution API em: {base_url}")
    
    # 1. Tentar listar instâncias
    print("\n--- Verificando Instancias ---")
    try:
        # Tenta rotas comuns de listagem
        for path in ["/instance/fetchInstances", "/instance/instanceFetch"]:
            url = f"{base_url}{path}"
            print(f"Tentando {url}...")
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                print(f"✅ SUCESSO! Instancias encontradas:")
                print(res.text)
                return
            else:
                print(f"❌ Erro {res.status_code}")
    except Exception as e:
        print(f"💥 Falha de conexao: {e}")

    # 2. Tentar ver versao/status
    print("\n--- Verificando Status Global ---")
    try:
        res = requests.get(f"{base_url}/", timeout=5)
        print(f"Resposta raiz (Status {res.status_code}): {res.text[:100]}")
    except:
        print("Raiz inacessivel.")

if __name__ == "__main__":
    diagnose_evolution()
