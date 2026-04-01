from pathlib import Path
import os

from dotenv import load_dotenv


load_dotenv()


class Settings:
    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "busca-ativa-inteligente")
        self.app_host = os.getenv("APP_HOST", "0.0.0.0")
        self.app_port = int(os.getenv("APP_PORT", "8000"))
        self.debug = os.getenv("DEBUG", "true").lower() == "true"
        self.school_name = os.getenv("SCHOOL_NAME", "Escola")

        self.evolution_api_url = os.getenv("EVOLUTION_API_URL", "")
        self.evolution_api_key = os.getenv("EVOLUTION_API_KEY", "")
        self.evolution_api_instance = os.getenv("EVOLUTION_API_INSTANCE", "")

        self.llm_provider = os.getenv("LLM_PROVIDER", "mock")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "")
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        self.webhook_verify_token = os.getenv("WEBHOOK_VERIFY_TOKEN", "change-me")
        self.data_dir = Path(os.getenv("DATA_DIR", "data/storage"))
        self.consolidated_report_path = Path(
            os.getenv(
                "CONSOLIDATED_REPORT_PATH",
                "relatorios/Relatorio_Consolidado_BuscaAtiva.xlsx",
            )
        )
        self.google_service_account_file = Path(
            os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service-account.json")
        )
        self.google_sheet_contatos_url = os.getenv("GOOGLE_SHEET_CONTATOS_URL", "")
        self.google_sheet_contatos_worksheet = os.getenv(
            "GOOGLE_SHEET_CONTATOS_WORKSHEET",
            "*",
        )
        self.google_sheet_dados_url = os.getenv("GOOGLE_SHEET_DADOS_URL", "")
        self.google_sheet_dados_worksheet = os.getenv(
            "GOOGLE_SHEET_DADOS_WORKSHEET",
            "*",
        )


settings = Settings()
