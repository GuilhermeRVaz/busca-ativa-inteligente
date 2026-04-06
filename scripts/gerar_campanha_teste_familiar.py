import os
import sys
from pathlib import Path
from datetime import datetime

# Adiciona o diretório raiz ao PYTHONPATH para acessar os módulos
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.campaign_engine import generate_campaign, save_campaign_to_json
from core.logging import get_logger

logger = get_logger(__name__)

def rodar_teste():
    """
    Roda um teste gerando uma campanha controlada de Busca Ativa
    contendo os 4 contatos familiares solicitados.
    """
    logger.info("Iniciando geração de campanha de teste (Familiares)...")
    
    # 1. Definindo os contatos (como se tivessem vindo do Supabase ou Planilha)
    contatos_mock = [
        {
            "student_name": "Aluno Guilherme",
            "class_name": "Trincheira DEV",
            "phone1": "5514981324832",
            "responsavel_1": "Guilherme DEV",
        },
        {
            "student_name": "Aluno da Regina",
            "class_name": "3A",
            "phone1": "5513982307099",
            "responsavel_1": "Regina",
        },
        {
            "student_name": "Aluno da Ana Paula",
            "class_name": "2B",
            "phone1": "5514997336127",
            "responsavel_1": "Ana Paula",
        },
        {
            "student_name": "Aluno da Ana Laura",
            "class_name": "9C",
            "phone1": "5514998676738",
            "responsavel_1": "Ana Laura",
        }
    ]

    # 2. Simulando que esses mesmos 4 alunos processados pelo SEDUC faltaram hoje
    dia_falta = datetime.now().day
    faltantes_mock = [
        {
            "student_name": "Aluno Guilherme",
            "class_name": "Trincheira DEV",
            "absence_days": str(dia_falta)
        },
        {
            "student_name": "Aluno da Regina",
            "class_name": "3A",
            "absence_days": str(dia_falta)
        },
        {
            "student_name": "Aluno da Ana Paula",
            "class_name": "2B",
            "absence_days": str(dia_falta)
        },
        {
            "student_name": "Aluno da Ana Laura",
            "class_name": "9C",
            "absence_days": str(dia_falta)
        }
    ]
    
    # 3. Gerar a campanha através da engine passando os nossos mocks
    campanha = generate_campaign(
        absences=faltantes_mock,
        contacts=contatos_mock,
        campaign_type="faltas_teste"
    )
    
    # 4. Salvar na pasta `data/campaigns`
    if campanha:
        caminho_salvo = save_campaign_to_json(campanha)
        logger.info(f"Sucesso! Campanha de TESTE gerada com {len(campanha)} disparos.")
        logger.info(f"O arquivo está em: {caminho_salvo}")
        print(f"\n[SUCESSO] Tudo pronto! Vá na pasta data/campaigns e verifique o arquivo JSON criado.\nCaminho: {caminho_salvo}")
    else:
        logger.error("A campanha não gerou nenhum disparo. Verifique os dados.")

if __name__ == "__main__":
    rodar_teste()
