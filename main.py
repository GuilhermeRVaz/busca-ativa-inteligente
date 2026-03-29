import argparse

from app.main import app
from core.config import settings
from services.campaign_orchestrator import CampaignOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Busca Ativa Inteligente")
    parser.add_argument("--tipo", choices=["faltas", "reuniao"], help="Tipo de campanha")
    parser.add_argument("--dia", type=int, help="Dia alvo para campanha de faltas")
    parser.add_argument(
        "--report-path",
        help="Caminho opcional para o relatorio consolidado legado",
    )
    return parser


def run_campaign(args: argparse.Namespace) -> None:
    orchestrator = CampaignOrchestrator()
    result = orchestrator.run(
        campaign_type=args.tipo,
        day=args.dia,
        report_path=args.report_path,
    )
    print(f"Campanha gerada: {result['campaign_type']}")
    print(f"Arquivo salvo: {result['file_path']}")
    print(f"Alunos processados: {result['processed_students']}")
    print(f"Itens gerados: {result['generated_items']}")
    print(f"Alunos sem contato: {result['students_without_contact']}")


def run_server() -> None:
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    if args.tipo:
        run_campaign(args)
    else:
        run_server()
