import json
from pathlib import Path
from typing import Any

from core.config import settings
from core.logging import get_logger


logger = get_logger(__name__)


class LocalRepository:
    def __init__(self) -> None:
        self.base_path = Path(settings.data_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.messages_file = self.base_path / "messages.json"
        self.campaigns_file = self.base_path / "campaigns.json"

    def _read_json(self, file_path: Path) -> list[dict[str, Any]]:
        if not file_path.exists():
            return []
        try:
            return json.loads(file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Invalid JSON found in %s. Resetting file.", file_path)
            return []

    def _write_json(self, file_path: Path, data: list[dict[str, Any]]) -> None:
        file_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def save_message(
        self,
        conversation_id: str,
        direction: str,
        text: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        messages = self._read_json(self.messages_file)
        messages.append(
            {
                "conversation_id": conversation_id,
                "direction": direction,
                "text": text,
                "metadata": metadata or {},
            }
        )
        self._write_json(self.messages_file, messages)

    def save_campaign_event(
        self,
        campaign_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        campaigns = self._read_json(self.campaigns_file)
        campaigns.append(
            {
                "campaign_id": campaign_id,
                "event_type": event_type,
                "payload": payload or {},
            }
        )
        self._write_json(self.campaigns_file, campaigns)


repository = LocalRepository()
