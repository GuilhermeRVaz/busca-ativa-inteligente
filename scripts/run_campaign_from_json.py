import os
import sys
from pathlib import Path
import json

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.sender import send_campaign, save_sent_campaign_to_json
from core.logging import get_logger

logger = get_logger(__name__)

def main(json_path: str, *, dry_run: bool = False) -> None:
    """Carrega o JSON gerado por `gerar_campanha_teste_familiar.py` e o envia.

    Args:
        json_path: caminho absoluto ou relativo para o arquivo JSON da campanha.
        dry_run:  se True, apenas valida (não dispara mensagens reais).
    """
    path = Path(json_path)
    if not path.is_file():
        logger.error("Arquivo de campanha não encontrado: %s", json_path)
        return

    # Carrega a lista de disparos que a engine já montou
    with path.open(encoding="utf-8") as f:
        campanha = json.load(f)

    logger.info("Campanha carregada (%d itens). Iniciando envio…", len(campanha))

    # Envia (ou faz dry‑run) usando o módulo sender
    campanha_enviada = send_campaign(campanha, dry_run=dry_run)

    # Salva o resultado final (com status, provider_message_id, etc.)
    out_path = save_sent_campaign_to_json(
        campanha_enviada,
        campaign_type="teste_familiar",
        day=None,
        output_dir=Path("data/storage/campaigns"),
    )
    logger.info("Campanha finalizada. Resultado salvo em: %s", out_path)
    print(f"\n[SUCESSO] Resultado da campanha gravado em: {out_path}\n")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Carrega um JSON de campanha e o envia via Evolution (ou dry‑run)."
    )
    parser.add_argument(
        "json_path",
        help="Caminho para o arquivo JSON gerado por gerar_campanha_teste_familiar.py",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida a campanha sem enviar mensagens reais",
    )
    args = parser.parse_args()

    main(args.json_path, dry_run=args.dry_run)
