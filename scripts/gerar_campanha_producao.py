"""
gerar_campanha_producao.py
--------------------------
Gera a campanha de Busca Ativa para um dia específico, lendo:
  - Faltas reais do Relatorio_Consolidado_BuscaAtiva.xlsx
  - Contatos reais da planilha Google Sheets (GOOGLE_SHEET_CONTATOS_URL)

Uso:
  python scripts/gerar_campanha_producao.py --day 6
  python scripts/gerar_campanha_producao.py --day 6 --dry-run
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Garante que o diretório raiz esteja no PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from openpyxl import load_workbook

from core.campaign_engine import generate_campaign, save_campaign_to_json
from core.config import settings
from core.logging import get_logger
from data.repository import repository

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
#  Leitura do Relatório Consolidado (.xlsx)                                   #
# --------------------------------------------------------------------------- #

def _normalize_col(value) -> str:
    import unicodedata
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return "".join(c if c.isalnum() else "_" for c in text).strip("_")


def _find_header_row(rows: list[tuple]) -> int:
    """Retorna o índice da linha de cabeçalho que contenha 'nome', 'ra' e 'turma'."""
    for i, row in enumerate(rows):
        normalized = {_normalize_col(v) for v in row if str(v or "").strip()}
        if {"nome", "ra", "turma"}.issubset(normalized):
            return i
    return -1


def _col_index(headers: list[str], options: set[str]) -> int:
    for i, h in enumerate(headers):
        if h in options:
            return i
    return -1


def _str(row: tuple, index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    return str(row[index] or "").strip()


def carregar_faltantes_do_xlsx(day: int) -> list[dict]:
    """
    Lê o relatório consolidado e retorna uma lista de dicionários:
      [{"student_name": ..., "class_name": ..., "absence_days": "6"}, ...]

    Estratégia de detecção de faltas:
      - Procura colunas cujo nome contenha 'falt' (ex.: 'faltas', 'total_faltas',
        'dias_com_falta', 'falta_dia_6', etc.)
      - Se a coluna contiver o dia no próprio nome (ex.: 'dia_6', '06'), filtra
        apenas as linhas com valor > 0 nessa coluna.
      - Se não houver colunas com o dia no nome, usa qualquer coluna de falta
        com valor > 0 (fallback).
    """
    report_path = Path(settings.consolidated_report_path)
    if not report_path.exists():
        logger.error("Relatório consolidado não encontrado em: %s", report_path)
        return []

    logger.info("Lendo relatório consolidado: %s", report_path)
    workbook = load_workbook(report_path, data_only=True)
    sheet = workbook.active
    rows = list(sheet.iter_rows(values_only=True))

    header_index = _find_header_row(rows)
    if header_index < 0:
        logger.error(
            "Cabeçalho com colunas 'nome', 'ra', 'turma' não encontrado no relatório."
        )
        return []

    headers = [_normalize_col(v) for v in rows[header_index]]
    logger.info("Cabeçalhos detectados: %s", headers)

    name_col = _col_index(headers, {"nome", "student_name", "aluno"})
    turma_col = _col_index(headers, {"turma", "class_name", "classe"})

    if name_col < 0:
        logger.error("Coluna 'nome' não encontrada no cabeçalho.")
        return []

    # Estratégia 1: colunas numéricas com o número do dia (ex.: coluna '6' para dia 6)
    #   Formato do relatório: turma | n | nome | ra | 1 | 2 | ... | 31 | total
    day_str_variants = {str(day), str(day).zfill(2), f"dia_{day}", f"dia_{str(day).zfill(2)}", f"falta_{day}"}
    falta_day_cols = [
        i for i, h in enumerate(headers)
        if h in day_str_variants
    ]

    # Estratégia 2: colunas cujo nome contenha 'falt' + o dia
    if not falta_day_cols:
        falta_day_cols = [
            i for i, h in enumerate(headers)
            if "falt" in h and any(v in h for v in day_str_variants)
        ]

    # Fallback: qualquer coluna com 'falt' no nome
    falta_any_cols = [i for i, h in enumerate(headers) if "falt" in h]

    active_falta_cols = falta_day_cols if falta_day_cols else falta_any_cols
    if not active_falta_cols:
        logger.warning(
            "Nenhuma coluna de falta encontrada. "
            "Coletando todos os alunos com nome preenchido como faltantes."
        )

    faltantes = []
    for row in rows[header_index + 1:]:
        student_name = _str(row, name_col)
        if not student_name or student_name.upper() in {"NOME", "TOTAL", ""}:
            continue

        class_name = _str(row, turma_col) if turma_col >= 0 else ""

        # Verifica se tem falta no dia
        if active_falta_cols:
            has_absence = any(
                _is_positive(_str(row, col)) for col in active_falta_cols
            )
            if not has_absence:
                continue

        faltantes.append({
            "student_name": student_name,
            "class_name": class_name,
            "absence_days": str(day),
        })

    logger.info("Alunos faltantes detectados no dia %d: %d", day, len(faltantes))
    return faltantes


def _is_positive(value: str) -> bool:
    """Retorna True se o valor representar um número positivo (> 0)."""
    try:
        return float(value.replace(",", ".")) > 0
    except (ValueError, AttributeError):
        # Aceita 'x', 'X', 'sim', 'yes', 'S' como presença de falta
        return value.strip().lower() in {"x", "sim", "yes", "s", "1", "faltou"}


# --------------------------------------------------------------------------- #
#  Script principal                                                            #
# --------------------------------------------------------------------------- #

def main(day: int, *, dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("Iniciando geração de campanha de PRODUÇÃO para o dia %d", day)
    if dry_run:
        logger.info("[DRY-RUN] Nenhuma mensagem será disparada.")
    logger.info("=" * 60)

    # 1. Carrega os alunos faltantes do .xlsx
    faltantes = carregar_faltantes_do_xlsx(day)
    if not faltantes:
        logger.error("Nenhum aluno faltante encontrado para o dia %d. Encerrando.", day)
        print(f"\n[ERRO] Nenhum aluno faltante encontrado para o dia {day}.")
        return

    logger.info("Total de faltantes a processar: %d", len(faltantes))

    # 2. Carrega os contatos reais do Google Sheets
    logger.info("Carregando contatos do Google Sheets...")
    try:
        contatos = repository.carregar_contatos()
    except Exception as exc:
        logger.error("Falha ao carregar contatos: %s", exc)
        print(f"\n[ERRO] Não foi possível carregar os contatos: {exc}")
        return

    if not contatos:
        logger.error("Nenhum contato carregado. Verifique GOOGLE_SHEET_CONTATOS_URL.")
        print("\n[ERRO] Nenhum contato carregado.")
        return

    logger.info("Contatos carregados: %d", len(contatos))

    # 3. Gera a campanha via engine
    campanha = generate_campaign(
        absences=faltantes,
        contacts=contatos,
        campaign_type="faltas",
        school_name=settings.school_name,
    )

    if not campanha:
        logger.error(
            "A engine não gerou nenhum disparo. "
            "Verifique se os nomes dos alunos no relatório batem com os da planilha de contatos."
        )
        # Lista os alunos sem correspondência
        nomes_faltantes = {f["student_name"].strip().lower() for f in faltantes}
        nomes_contatos = {c.get("student_name", "").strip().lower() for c in contatos}
        sem_contato = nomes_faltantes - nomes_contatos
        logger.warning("Alunos sem correspondência nos contatos (%d):", len(sem_contato))
        for nome in sorted(sem_contato):
            logger.warning("  - %s", nome)
        print(f"\n[ATENÇÃO] {len(sem_contato)} alunos sem contato encontrado.")
        return

    # 4. Salva o JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"campaign_producao_dia{day}_{timestamp}.json"
    caminho = save_campaign_to_json(campanha, filename=filename)

    logger.info("Campanha gerada com %d disparos.", len(campanha))
    logger.info("Arquivo salvo em: %s", caminho)

    sem_contato_count = len(faltantes) - len(campanha)

    print("\n" + "=" * 60)
    print(f"  Campanha de PRODUÇÃO gerada com sucesso!")
    print(f"  Alunos faltantes no dia {day}: {len(faltantes)}")
    print(f"  Disparos gerados:              {len(campanha)}")
    if sem_contato_count > 0:
        print(f"  Sem contato (não disparados):  {sem_contato_count}")
    print(f"  Arquivo: {caminho}")
    print("=" * 60)

    if dry_run:
        print("\n[DRY-RUN] Para disparar de verdade, remova --dry-run e execute:")
        print(f'  python scripts/run_campaign_from_json.py "{caminho}"')
    else:
        print("\nAgora execute o disparo com:")
        print(f'  python scripts/run_campaign_from_json.py "{caminho}"')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera campanha de Busca Ativa para os alunos faltantes de um dia."
    )
    parser.add_argument(
        "--day",
        type=int,
        required=True,
        help="Dia do mês para buscar faltas (ex: 6 para o dia 6)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida sem disparar mensagens reais",
    )
    args = parser.parse_args()
    main(args.day, dry_run=args.dry_run)
