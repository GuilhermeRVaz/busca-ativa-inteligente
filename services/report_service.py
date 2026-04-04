import json
from datetime import datetime, date, timedelta
from typing import Any, Dict

from core.logging import get_logger
from data.supabase_repository import _get_client

logger = get_logger(__name__)

class ReportService:
    def __init__(self):
        self.client = _get_client()

    def gerar_metricas_periodo(self, data_inicio: str, data_fim: str) -> Dict[str, Any]:
        """
        Gera métricas filtrando a tabela 'messages' pelo periodo de created_at.
        Formato esperado das datas: YYYY-MM-DD
        """
        if not self.client:
            logger.error("Supabase client não está inicializado.")
            return {}

        start_date = f"{data_inicio}T00:00:00"
        end_date = f"{data_fim}T23:59:59.999999"

        try:
            # Buscar mensagens do período
            response = (
                self.client.table("messages")
                .select("direcao, tipo_resposta, motivo")
                .gte("created_at", start_date)
                .lte("created_at", end_date)
                .execute()
            )
            messages = response.data
        except Exception as e:
            logger.error(f"Erro ao buscar dados do Supabase: {e}")
            return {}

        metrics = {
            "periodo": f"{data_inicio} até {data_fim}",
            "total_faltantes": 0, # Calculado como total_contatados (outbound) ou aproximado
            "total_contatados": 0,
            "total_responderam": 0,
            "total_justificaram": 0,
            "total_vai_regularizar": 0,
            "total_resistencia": 0,
            "motivos": {
                "SAUDE": 0,
                "TRANSPORTE": 0,
                "FAMILIAR": 0,
                "ESCOLAR": 0,
                "LOGISTICA": 0,
                "OUTROS": 0,
            }
        }

        # Contadores específicos
        for msg in messages:
            direcao = (msg.get("direcao") or "").lower()
            tipo_resposta = (msg.get("tipo_resposta") or "").upper()
            motivo = (msg.get("motivo") or "").upper()

            if direcao == "outbound":
                metrics["total_contatados"] += 1
            elif direcao == "inbound":
                metrics["total_responderam"] += 1
                
                if tipo_resposta == "JUSTIFICOU":
                    metrics["total_justificaram"] += 1
                elif tipo_resposta == "VAI_REGULARIZAR":
                    metrics["total_vai_regularizar"] += 1
                elif tipo_resposta == "RESISTENCIA":
                    metrics["total_resistencia"] += 1
                
                # Agrupar motivos se foi justificado (ou outros tipos, garantindo que o motivo seja mapeado)
                if motivo in metrics["motivos"]:
                    metrics["motivos"][motivo] += 1
                elif motivo:
                    metrics["motivos"]["OUTROS"] += 1

        # Assumindo que total_faltantes seja igual aos contatados (a entrada da campanha)
        metrics["total_faltantes"] = metrics["total_contatados"]

        return metrics

    def gerar_relatorio_diario(self, data_ref: str = None) -> Dict[str, Any]:
        """Gera o relatório de um dia específico."""
        if not data_ref:
            data_ref = date.today().isoformat()
        return self.gerar_metricas_periodo(data_ref, data_ref)

    def exportar_relatorio(self, metricas: Dict[str, Any], file_path: str = "relatorio.json"):
        """Exporta as métricas geradas para um arquivo JSON."""
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(metricas, f, indent=4, ensure_ascii=False)
            logger.info(f"Relatório exportado com sucesso para {file_path}")
        except Exception as e:
            logger.error(f"Erro ao exportar relatório: {e}")

    def formatar_relatorio_texto(self, metricas: Dict[str, Any]) -> str:
        """Formata as métricas em texto estruturado legível."""
        if not metricas:
            return "Erro: Nenhuma métrica disponível."

        linhas = [
            f"RELATÓRIO BUSCA ATIVA - {metricas.get('periodo', '')}",
            "------------------------------------------------",
            f"Faltantes:     {metricas['total_faltantes']}",
            f"Contatados:    {metricas['total_contatados']}",
            f"Responderam:   {metricas['total_responderam']}",
            f"Justificaram:  {metricas['total_justificaram']}",
            f"Vão retornar:  {metricas['total_vai_regularizar']}",
            f"Resistência:   {metricas['total_resistencia']}",
            "",
            "Motivos:",
            f"- Saúde:       {metricas['motivos'].get('SAUDE', 0)}",
            f"- Transporte:  {metricas['motivos'].get('TRANSPORTE', 0)}",
            f"- Familiar:    {metricas['motivos'].get('FAMILIAR', 0)}",
            f"- Escolar:     {metricas['motivos'].get('ESCOLAR', 0)}",
            f"- Logística:   {metricas['motivos'].get('LOGISTICA', 0)}",
            f"- Outros:      {metricas['motivos'].get('OUTROS', 0)}",
        ]
        return "\n".join(linhas)

    def atualizar_dashboard_google_sheets(self, metricas: Dict[str, Any]):
        """Atualiza as abas RESUMO_DIARIO e MOTIVOS no Google Sheets da aplicação."""
        try:
            from data.repository import LocalRepository
            from core.config import settings
            import gspread

            repo = LocalRepository()
            client = repo._get_gspread_client()
            if not client or not settings.google_sheet_dados_url:
                logger.warning("Google Sheets não configurado para o dashboard.")
                return

            spreadsheet = client.open_by_url(settings.google_sheet_dados_url)
            data_periodo = metricas.get("periodo", "").split(" até ")[0]

            # 1. Atualizar RESUMO_DIARIO
            try:
                ws_resumo = spreadsheet.worksheet("RESUMO_DIARIO")
            except gspread.exceptions.WorksheetNotFound:
                ws_resumo = spreadsheet.add_worksheet(title="RESUMO_DIARIO", rows=1000, cols=10)
                ws_resumo.append_row(["data", "faltantes", "contatados", "responderam", "justificaram"])

            cell = None
            try:
                cell = ws_resumo.find(data_periodo, in_column=1)
            except gspread.exceptions.CellNotFound:
                pass

            row_data = [
                data_periodo,
                metricas.get("total_faltantes", 0),
                metricas.get("total_contatados", 0),
                metricas.get("total_responderam", 0),
                metricas.get("total_justificaram", 0)
            ]
            
            if cell:
                row_idx = cell.row
                # Atualiza os dados da linha.
                try:
                    ws_resumo.update(f"A{row_idx}:E{row_idx}", [row_data])
                except Exception:
                    # Fallback pra versões nativas
                    ws_resumo.update([row_data], f"A{row_idx}:E{row_idx}")
                logger.info(f"Aba RESUMO_DIARIO atualizada para data {data_periodo}")
            else:
                ws_resumo.append_row(row_data)
                logger.info(f"Aba RESUMO_DIARIO inserida para data {data_periodo}")

            # 2. Atualizar MOTIVOS
            try:
                ws_motivos = spreadsheet.worksheet("MOTIVOS")
            except gspread.exceptions.WorksheetNotFound:
                ws_motivos = spreadsheet.add_worksheet(title="MOTIVOS", rows=1000, cols=5)
                ws_motivos.append_row(["data", "motivo", "quantidade"])

            all_motivos_rows = ws_motivos.get_all_values()
            
            novas_linhas = []
            for row in all_motivos_rows:
                if len(row) > 0 and row[0] != data_periodo:
                    novas_linhas.append(row)
            
            for mot, qtd in metricas.get("motivos", {}).items():
                novas_linhas.append([data_periodo, mot, qtd])
            
            ws_motivos.clear()
            try:
                ws_motivos.update("A1", novas_linhas)
            except Exception:
                ws_motivos.update(novas_linhas, "A1")
            logger.info("Aba MOTIVOS atualizada e sobrescrita com dados consolidados")

        except Exception as e:
            logger.error(f"Erro ao atualizar dashboard do google sheets: {e}")

report_service = ReportService()
