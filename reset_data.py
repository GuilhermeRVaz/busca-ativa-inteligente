import os
import sys
from pathlib import Path

# Adiciona a raiz do projeto no path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from core.config import settings
from data.repository import repository
from data.supabase_repository import _get_client

def reset_local_json():
    print("Limpando arquivos JSON locais...")
    files_to_clear = [
        repository.incoming_messages_file,
        repository.messages_file
    ]
    for f in files_to_clear:
        if f.exists():
            f.write_text("[]", encoding="utf-8")
            print(f"  - {f.name} resetado.")

def reset_google_sheets():
    print("Limpando Google Sheets (Interações)...")
    if not settings.google_sheet_dados_url:
        print("  - GOOGLE_SHEET_DADOS_URL não configurada. Pulando.")
        return

    try:
        worksheets = repository._get_worksheets(
            settings.google_sheet_dados_url,
            settings.google_sheet_dados_worksheet,
        )
        if worksheets:
            ws = worksheets[0]
            header = repository._incoming_sheet_header()
            ws.clear()
            ws.append_row(header)
            print("  - Planilha de interações limpa (apenas cabeçalho mantido).")
    except Exception as e:
        print(f"  - Erro ao limpar Google Sheets: {e}")

def reset_supabase():
    print("Limpando Supabase (Tabela messages - INTERAÇÕES)...")
    client = _get_client()
    if not client:
        print("  - Supabase não configurado ou offline. Pulando.")
        return

    try:
        # Limpar apenas messages (interações recebidas/enviadas)
        res_msg = client.table("messages").delete().neq("id", "none").execute()
        print(f"  - Tabela 'messages' limpa ({len(res_msg.data)} registros removidos).")
        
        # Opcional: LIMPAR STUDENTS (Contatos)
        # print("Limpando Supabase (Tabela students - CONTATOS)...")
        # res_std = client.table("students").delete().neq("telefone", "none").execute()
        # print(f"  - Tabela 'students' limpa ({len(res_std.data)} registros removidos).")
        
    except Exception as e:
        print(f"  - Erro ao limpar Supabase: {e}")

if __name__ == "__main__":
    print("=== INICIANDO RESET DE DADOS ===\n")
    confirm = input("Isso irá APAGAR todas as interações e histórico. Tem certeza? (s/N): ")
    if confirm.lower() == 's':
        reset_local_json()
        reset_google_sheets()
        reset_supabase()
        print("\n=== RESET CONCLUÍDO COM SUCESSO ===")
    else:
        print("\nOperação cancelada.")
