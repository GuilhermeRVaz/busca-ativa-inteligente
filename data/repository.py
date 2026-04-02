import json
import unicodedata
from pathlib import Path
from typing import Any

import gspread

from core.config import settings
from core.logging import get_logger


logger = get_logger(__name__)


class LocalRepository:
    def __init__(self) -> None:
        self.base_path = Path(settings.data_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.messages_file = self.base_path / "messages.json"
        self.incoming_messages_file = self.base_path / "incoming_messages.json"
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

    def _get_gspread_client(self) -> gspread.Client | None:
        credentials_file = Path(settings.google_service_account_file)
        if not credentials_file.exists():
            logger.warning(
                "Arquivo de service account nao encontrado em %s",
                credentials_file,
            )
            return None

        try:
            return gspread.service_account(filename=str(credentials_file))
        except Exception as exc:
            logger.error("Falha ao autenticar no Google Sheets: %s", exc)
            return None

    def _get_worksheets(self, spreadsheet_url: str, worksheet_setting: str) -> list[Any]:
        client = self._get_gspread_client()
        if client is None:
            return []

        try:
            spreadsheet = client.open_by_url(spreadsheet_url)
            resolved_setting = worksheet_setting.strip()
            if not resolved_setting or resolved_setting == "*":
                return list(spreadsheet.worksheets())

            worksheet_names = [
                item.strip() for item in resolved_setting.split(",") if item.strip()
            ]
            return [spreadsheet.worksheet(name) for name in worksheet_names]
        except Exception as exc:
            logger.error("Falha ao abrir planilha no Google Sheets: %s", exc)
            return []

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

    def carregar_contatos(self) -> list[dict[str, str]]:
        if not settings.google_sheet_contatos_url:
            raise ValueError("Defina GOOGLE_SHEET_CONTATOS_URL para carregar contatos.")

        worksheets = self._get_worksheets(
            settings.google_sheet_contatos_url,
            settings.google_sheet_contatos_worksheet,
        )
        if not worksheets:
            return []

        records: list[dict[str, Any]] = []
        for worksheet in worksheets:
            records.extend(worksheet.get_all_records())
        return self._records_to_contacts(records)

    def salvar_interacao(self, data: dict[str, Any]) -> None:
        entry = {
            "data_hora": str(data.get("data_hora", "")).strip(),
            "student_name": str(data.get("student_name", "")).strip(),
            "telefone": str(data.get("telefone", "")).strip(),
            "mensagem": str(data.get("mensagem", "")),
            "classificacao": str(data.get("classificacao", "")).strip(),
            "intencao": str(data.get("intencao", "")).strip(),
            "motivo": str(data.get("motivo", "")).strip(),
            "observacao": str(data.get("observacao", "")).strip(),
            "campaign_id": str(data.get("campaign_id", "")).strip(),
            "origem": str(data.get("origem", "whatsapp")).strip() or "whatsapp",
            "raw_payload": data.get("raw_payload") or {},
        }
        if not entry["intencao"] and entry["classificacao"]:
            entry["intencao"] = entry["classificacao"]
        if not entry["intencao"]:
            entry["intencao"] = "DUVIDA"
        if not entry["motivo"]:
            entry["motivo"] = "OUTRO"
        if not entry["observacao"]:
            entry["observacao"] = "nao foi possivel classificar"

        self._save_incoming_json(entry)
        self._save_incoming_google_sheet(entry)

    def save_incoming_message(
        self,
        telefone: str,
        mensagem: str,
        classificacao: str,
        data_hora: str,
        raw_payload: dict[str, Any] | None = None,
        campaign_id: str = "",
        origem: str = "whatsapp",
        intencao: str = "",
        motivo: str = "",
        observacao: str = "",
    ) -> None:
        self.salvar_interacao(
            {
                "telefone": telefone,
                "mensagem": mensagem,
                "classificacao": classificacao,
                "intencao": intencao,
                "motivo": motivo,
                "observacao": observacao,
                "data_hora": data_hora,
                "campaign_id": campaign_id,
                "origem": origem,
                "raw_payload": raw_payload or {},
            }
        )

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

    def _save_incoming_json(self, entry: dict[str, Any]) -> None:
        messages = self._read_json(self.incoming_messages_file)
        messages.append(entry)
        self._write_json(self.incoming_messages_file, messages)
        logger.info("backup json salvo | arquivo=%s", self.incoming_messages_file)

    def _save_incoming_google_sheet(self, entry: dict[str, Any]) -> None:
        if not settings.google_sheet_dados_url:
            logger.warning("GOOGLE_SHEET_DADOS_URL nao configurada; usando backup JSON")
            return

        worksheets = self._get_worksheets(
            settings.google_sheet_dados_url,
            settings.google_sheet_dados_worksheet,
        )
        if not worksheets:
            logger.warning("Nao foi possivel acessar Google Sheets de dados; usando backup JSON")
            return

        row = [
            entry["data_hora"],
            entry["student_name"],
            entry["telefone"],
            entry["mensagem"],
            entry["intencao"],
            entry["motivo"],
            entry["observacao"],
            entry["campaign_id"],
            entry["origem"],
        ]

        try:
            worksheets[0].append_row(row)
            logger.info(
                "salvo no sheets | telefone=%s | class=%s",
                entry["telefone"],
                entry["intencao"] or entry["classificacao"],
            )
        except Exception as exc:
            logger.error("falha no sheets | usando backup JSON | erro=%s", exc)

    def resolver_nome_aluno(
        self,
        telefone: str,
        *,
        push_name: str = "",
    ) -> str:
        normalized_phone = self._normalize_phone_lookup(telefone)
        if normalized_phone:
            campaign_name = self._find_student_name_in_sent_campaigns(normalized_phone)
            if campaign_name:
                return campaign_name

            contact_name = self._find_student_name_in_contacts_by_phone(normalized_phone)
            if contact_name:
                return contact_name

        normalized_push_name = self._normalize_text_lookup(push_name)
        if normalized_push_name:
            campaign_name = self._find_student_name_in_sent_campaigns_by_name(normalized_push_name)
            if campaign_name:
                return campaign_name

            contact_name = self._find_student_name_in_contacts_by_name(normalized_push_name)
            if contact_name:
                return contact_name

        recent_name = self._find_most_recent_student_name_in_sent_campaigns()
        if recent_name:
            return recent_name

        return ""

    def limpar_interacoes_salvas(self) -> int:
        entries = self._read_json(self.incoming_messages_file)
        cleaned_entries = self._clean_interaction_entries(entries)
        self._write_json(self.incoming_messages_file, cleaned_entries)
        self._replace_incoming_google_sheet(cleaned_entries)
        return len(cleaned_entries)

    def _clean_interaction_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected_entries: dict[str, dict[str, Any]] = {}
        anonymous_entries: list[dict[str, Any]] = []

        for entry in entries:
            raw_payload = entry.get("raw_payload") or {}
            if not self._should_persist_incoming_payload(raw_payload):
                continue

            message_id = self._extract_message_id(raw_payload)
            if not message_id:
                anonymous_entries.append(self._enrich_interaction_entry(entry))
                continue

            current_best = selected_entries.get(message_id)
            if current_best is None or self._score_interaction_entry(entry) >= self._score_interaction_entry(
                current_best
            ):
                selected_entries[message_id] = entry

        cleaned = [self._enrich_interaction_entry(entry) for entry in selected_entries.values()]
        cleaned.extend(anonymous_entries)
        cleaned.sort(key=lambda item: str(item.get("data_hora", "")))
        return cleaned

    def interacao_ja_registrada(self, payload: dict[str, Any]) -> bool:
        message_id = self._extract_message_id(payload)
        if not message_id:
            return False
        for entry in self._read_json(self.incoming_messages_file):
            existing_payload = entry.get("raw_payload") or {}
            if self._extract_message_id(existing_payload) == message_id:
                return True
        return False

    def _replace_incoming_google_sheet(self, entries: list[dict[str, Any]]) -> None:
        if not settings.google_sheet_dados_url:
            logger.warning("GOOGLE_SHEET_DADOS_URL nao configurada; limpeza apenas local")
            return

        worksheets = self._get_worksheets(
            settings.google_sheet_dados_url,
            settings.google_sheet_dados_worksheet,
        )
        if not worksheets:
            logger.warning("Nao foi possivel acessar Google Sheets de dados durante limpeza")
            return

        worksheet = worksheets[0]
        header = [
            "data_hora",
            "student_name",
            "telefone",
            "mensagem",
            "intencao",
            "motivo",
            "observacao",
            "campaign_id",
            "origem",
        ]
        rows = [
            [
                entry.get("data_hora", ""),
                entry.get("student_name", ""),
                entry.get("telefone", ""),
                entry.get("mensagem", ""),
                entry.get("intencao", ""),
                entry.get("motivo", ""),
                entry.get("observacao", ""),
                entry.get("campaign_id", ""),
                entry.get("origem", "whatsapp"),
            ]
            for entry in entries
        ]

        try:
            worksheet.clear()
            worksheet.append_row(header)
            if rows:
                worksheet.append_rows(rows)
            logger.info("Planilha de interacoes limpa e regravada com %s registros", len(rows))
        except Exception as exc:
            logger.error("Falha ao regravar planilha de interacoes: %s", exc)

    def _enrich_interaction_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        raw_payload = entry.get("raw_payload") or {}
        phone = str(entry.get("telefone", "")).strip()
        push_name = str(raw_payload.get("data", {}).get("pushName", "")).strip()
        student_name = str(entry.get("student_name", "")).strip() or self.resolver_nome_aluno(
            phone,
            push_name=push_name,
        )
        cleaned_entry = dict(entry)
        cleaned_entry["student_name"] = student_name
        return cleaned_entry

    @staticmethod
    def _score_interaction_entry(entry: dict[str, Any]) -> tuple[int, int, str]:
        raw_payload = entry.get("raw_payload") or {}
        data = raw_payload.get("data", {}) if isinstance(raw_payload, dict) else {}
        status = str(data.get("status", "")).strip().upper()
        push_name = str(data.get("pushName", "")).strip()
        status_score = 1 if status and status != "ERROR" else 0
        push_name_score = 1 if push_name else 0
        return (status_score, push_name_score, str(entry.get("data_hora", "")))

    def _find_student_name_in_sent_campaigns(self, telefone: str) -> str:
        campaigns_dir = self.base_path / "campaigns"
        if not campaigns_dir.exists():
            return ""

        for file_path in sorted(campaigns_dir.glob("*_sent.json"), reverse=True):
            for item in self._read_json(file_path):
                item_phone = self._normalize_phone_lookup(item.get("phone"))
                if item_phone == telefone:
                    return str(item.get("student_name", "")).strip()
        return ""

    def _find_student_name_in_sent_campaigns_by_name(self, push_name: str) -> str:
        campaigns_dir = self.base_path / "campaigns"
        if not campaigns_dir.exists():
            return ""

        for file_path in sorted(campaigns_dir.glob("*_sent.json"), reverse=True):
            for item in self._read_json(file_path):
                student_name = str(item.get("student_name", "")).strip()
                if self._normalize_text_lookup(student_name) == push_name:
                    return student_name
        return ""

    def _find_student_name_in_contacts_by_phone(self, telefone: str) -> str:
        for contact in self.carregar_contatos():
            phones = [
                self._normalize_phone_lookup(contact.get("phone1")),
                self._normalize_phone_lookup(contact.get("phone2")),
                self._normalize_phone_lookup(contact.get("phone3")),
            ]
            if telefone in phones:
                return str(contact.get("student_name", "")).strip()
        return ""

    def _find_student_name_in_contacts_by_name(self, push_name: str) -> str:
        for contact in self.carregar_contatos():
            student_name = str(contact.get("student_name", "")).strip()
            if self._normalize_text_lookup(student_name) == push_name:
                return student_name
        return ""

    def _find_most_recent_student_name_in_sent_campaigns(self) -> str:
        campaigns_dir = self.base_path / "campaigns"
        if not campaigns_dir.exists():
            return ""

        for file_path in sorted(campaigns_dir.glob("*_sent.json"), reverse=True):
            items = self._read_json(file_path)
            for item in reversed(items):
                if str(item.get("status", "")).strip().lower() != "sent":
                    continue
                student_name = str(item.get("student_name", "")).strip()
                if student_name:
                    return student_name
        return ""

    @staticmethod
    def _should_persist_incoming_payload(payload: dict[str, Any]) -> bool:
        event = str(payload.get("event", "")).strip().lower()
        data = payload.get("data", {})
        key = data.get("key", {}) if isinstance(data, dict) else {}
        if event != "messages.upsert":
            return False
        if bool(key.get("fromMe")):
            return False
        return True

    @staticmethod
    def _extract_message_id(payload: dict[str, Any]) -> str:
        data = payload.get("data", {})
        key = data.get("key", {}) if isinstance(data, dict) else {}
        return str(key.get("id", "")).strip()

    @staticmethod
    def _normalize_phone_lookup(value: Any) -> str:
        text = str(value or "").replace("@s.whatsapp.net", "").replace("+", "").strip()
        digits = "".join(char for char in text if char.isdigit())
        return digits if len(digits) >= 10 else ""

    @staticmethod
    def _normalize_text_lookup(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
        return " ".join(text.split())

    def _records_to_contacts(self, records: list[dict[str, Any]]) -> list[dict[str, str]]:
        contacts: list[dict[str, str]] = []
        for record in records:
            normalized_record = {
                self._normalize_column_name(key): value for key, value in record.items()
            }

            student_name = self._pick_first_value(
                normalized_record,
                ["student_name", "nome_do_aluno", "nome_aluno", "aluno", "nome"],
            )
            if not student_name:
                continue

            phone_columns = self._find_phone_columns(normalized_record)
            phone_values = [
                self._sanitize_phone(normalized_record.get(column))
                for column in phone_columns
            ]
            valid_phones = [phone for phone in phone_values if phone]

            contacts.append(
                {
                    "student_name": student_name,
                    "class_name": self._pick_first_value(
                        normalized_record,
                        ["class_name", "turma", "classe"],
                    ),
                    "phone1": valid_phones[0] if len(valid_phones) > 0 else "",
                    "phone2": valid_phones[1] if len(valid_phones) > 1 else "",
                    "phone3": valid_phones[2] if len(valid_phones) > 2 else "",
                }
            )

        return contacts

    def _find_phone_columns(self, record: dict[str, Any]) -> list[str]:
        preferred = [
            "phone1",
            "phone2",
            "phone3",
            "telefone1",
            "telefone2",
            "telefone3",
            "celular1",
            "celular2",
            "celular3",
        ]
        selected = [column for column in preferred if column in record]
        if selected:
            return selected[:3]

        discovered = [
            column
            for column in record
            if any(
                token in column
                for token in ("telefone", "celular", "fone", "whatsapp", "phone")
            )
        ]
        return discovered[:3]

    @staticmethod
    def _pick_first_value(record: dict[str, Any], columns: list[str]) -> str:
        for column in columns:
            value = record.get(column)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    @staticmethod
    def _sanitize_phone(value: Any) -> str:
        digits = "".join(char for char in str(value or "") if char.isdigit())
        if len(digits) < 10:
            return ""
        return digits

    @staticmethod
    def _normalize_column_name(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
        normalized = []
        for char in text:
            normalized.append(char if char.isalnum() else "_")
        return "".join(normalized).strip("_")


repository = LocalRepository()
