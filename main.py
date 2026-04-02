import argparse
from pathlib import Path

from app.main import app
from core.config import settings
from services.campaign_orchestrator import CampaignOrchestrator
from services.evolution_api import evolution_api_service
from services.sender import save_sent_campaign_to_json, send_campaign


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Busca Ativa Inteligente")
    parser.add_argument("--tipo", choices=["faltas", "reuniao"], help="Tipo de campanha")
    parser.add_argument("--dia", type=int, help="Dia alvo para campanha de faltas")
    parser.add_argument(
        "--report-path",
        help="Caminho opcional para o relatorio consolidado legado",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Gera e simula o envio sem tocar na Evolution API",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        help="Limita a quantidade de itens enviados para homologacao controlada",
    )
    parser.add_argument(
        "--offset-items",
        type=int,
        default=0,
        help="Pula os primeiros itens da campanha para comecar de um aluno especifico",
    )
    parser.add_argument(
        "--diagnostico",
        action="store_true",
        help="Executa validacoes de pre-voo para producao",
    )
    return parser


def run_campaign(args: argparse.Namespace) -> str:
    orchestrator = CampaignOrchestrator()
    result = orchestrator.run(
        campaign_type=args.tipo,
        day=args.dia,
        report_path=args.report_path,
    )
    campaign = _slice_campaign(result["campaign"], args.offset_items, args.max_items)
    mode = "dry-run" if args.dry_run else "real"

    print(f"Modo de envio: {mode}")
    print(f"Instancia Evolution: {evolution_api_service.get_instance_name()}")
    print(f"Tipo de campanha: {result['campaign_type']}")
    print(f"Itens planejados: {len(result['campaign'])}")
    print(f"Itens pulados: {max(args.offset_items, 0)}")
    print(f"Itens a enviar: {len(campaign)}")

    sent_campaign = send_campaign(campaign, dry_run=args.dry_run)
    sent_file_path = save_sent_campaign_to_json(
        sent_campaign,
        campaign_type=result["campaign_type"],
        day=result.get("target_day", args.dia),
        output_dir=Path(result["file_path"]).parent,
    )
    sent_count = sum(1 for item in sent_campaign if item.get("status") == "sent")
    failed_count = sum(1 for item in sent_campaign if item.get("status") == "failed")

    print(f"Campanha gerada: {result['campaign_type']}")
    print(f"Arquivo salvo: {result['file_path']}")
    print(f"Arquivo enviado salvo: {sent_file_path}")
    print(f"Alunos processados: {result['processed_students']}")
    print(f"Itens gerados: {result['generated_items']}")
    print(f"Alunos sem contato: {result['students_without_contact']}")
    print(f"Total de itens: {len(sent_campaign)}")
    print(f"Enviados: {sent_count}")
    print(f"Falharam: {failed_count}")
    return sent_file_path


def run_diagnostics(report_path: str | None = None) -> None:
    print("Diagnostico de pre-voo")

    if not settings.google_service_account_file.exists():
        raise FileNotFoundError(
            f"Arquivo de service account nao encontrado: {settings.google_service_account_file}"
        )
    if not settings.google_sheet_contatos_url.strip():
        raise ValueError("GOOGLE_SHEET_CONTATOS_URL nao configurada")
    if not settings.google_sheet_dados_url.strip():
        raise ValueError("GOOGLE_SHEET_DADOS_URL nao configurada")

    evolution_api_service.validate_configuration()

    contacts = CampaignOrchestrator().contacts_provider.fetch_contacts()
    if not contacts:
        raise ValueError("Nenhum contato disponivel na planilha configurada")

    resolved_report_path = Path(report_path or settings.consolidated_report_path)
    if resolved_report_path.exists():
        print(f"Consolidado configurado: {resolved_report_path}")
    else:
        print(f"Consolidado ausente para campanha de faltas: {resolved_report_path}")

    print(f"Contatos disponiveis: {len(contacts)}")
    print("Diagnostico concluido com sucesso")


def run_server() -> None:
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.debug,
    )


def _slice_campaign(
    campaign: list[dict],
    offset_items: int | None,
    max_items: int | None,
) -> list[dict]:
    offset = max(offset_items or 0, 0)
    sliced = campaign[offset:]
    if max_items is None or max_items <= 0:
        return sliced
    return sliced[:max_items]


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    if args.diagnostico:
        run_diagnostics(report_path=args.report_path)
    elif args.tipo:
        run_campaign(args)
    else:
        run_server()
