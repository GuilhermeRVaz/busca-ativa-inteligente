import os
import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.logging import get_logger
from data.repository import LocalRepository
from data.supabase_repository import _get_client, normalize_phone

logger = get_logger(__name__)

def import_contacts():
    logger.info("Iniciando importação de contatos do Google Sheets para o Supabase...")
    
    # 1. Pegar dados do Sheets
    repo = LocalRepository()
    client = repo._get_gspread_client()
    
    if not client:
        logger.error("Falha ao obter cliente do Google Sheets.")
        return
        
    from core.config import settings
    url = settings.google_sheet_contatos_url
    
    try:
        spreadsheet = client.open_by_url(url)
        # Tenta todas as planilhas para carregar todas as turmas
        worksheets = spreadsheet.worksheets()
    except Exception as e:
        logger.error(f"Erro ao abrir planilha: {e}")
        return

    supabase_client = _get_client()
    if not supabase_client:
        logger.error("Falha ao conectar no Supabase.")
        return

    total_inserted = 0
    total_updated = 0
    
    for ws in worksheets:
        # A aba "Respostas ao formulário" talvez seja ignorada, dependendo do nome
        turma = ws.title.strip()
        if "Respostas" in turma or "Dashboard" in turma or "Interacoes" in turma:
            continue
            
        logger.info(f"Processando aba/turma: {turma}")
        
        try:
            records = ws.get_all_records()
        except Exception as e:
            logger.error(f"Erro ao ler aba {turma}: {e}")
            continue
            
        for row in records:
            ra = str(row.get("RA", "")).strip()
            nome_aluno = str(row.get("Nome do Aluno", "")).strip()
            
            if not ra or not nome_aluno:
                continue
                
            situacao = str(row.get("Situação", "")).strip()
            data_nasc = str(row.get("Data de Nascimento", "")).strip()
            
            resp1 = str(row.get("responsável 1", "")).strip()
            tel1 = normalize_phone(row.get("telefone 1", ""))
            
            resp2 = str(row.get("responsavel 2", row.get("responsável 2", ""))).strip()
            tel2 = normalize_phone(row.get("telefone 2", ""))
            
            resp3 = str(row.get("responsavel 3", row.get("responsável 3", ""))).strip()
            tel3 = normalize_phone(row.get("telefone 3", ""))

            contact_data = {
                "ra": ra,
                "nome_aluno": nome_aluno,
                "turma": turma,
                "situacao": situacao,
                "data_nascimento": data_nasc,
                "responsavel_1": resp1,
                "telefone_1": tel1,
                "responsavel_2": resp2,
                "telefone_2": tel2,
                "responsavel_3": resp3,
                "telefone_3": tel3
            }

            try:
                # Upsert idempotente usando RA
                result = supabase_client.table("contacts").upsert(contact_data, on_conflict="ra").execute()
                # Apenas soma ao contador geral
                total_inserted += 1
            except Exception as e:
                logger.error(f"Erro ao inserir contato {nome_aluno} (RA: {ra}): {e}")

    logger.info(f"Importação finalizada! Total de contatos processados: {total_inserted}")

if __name__ == "__main__":
    import_contacts()
