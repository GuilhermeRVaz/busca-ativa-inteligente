import unicodedata
from pathlib import Path
from typing import Any

from core.config import settings


class GoogleSheetsContactsProvider:
    def fetch_contacts(self) -> list[dict[str, str]]:
        records = self._load_records()
        return self._records_to_contacts(records)

    def _load_records(self) -> list[dict[str, Any]]:
        if not settings.google_sheet_url:
            raise ValueError("Defina GOOGLE_SHEET_URL para carregar contatos.")

        credentials_file = Path(settings.google_service_account_file)
        if not credentials_file.exists():
            raise FileNotFoundError(
                f"Arquivo de credenciais do Google nao encontrado: {credentials_file}"
            )

        try:
            import gspread
        except ImportError as exc:
            raise ImportError("Instale gspread para usar o provider de contatos.") from exc

        client = gspread.service_account(filename=str(credentials_file))
        workbook = client.open_by_url(settings.google_sheet_url)

        worksheet_setting = settings.google_sheet_worksheet.strip()
        if worksheet_setting and worksheet_setting != "*":
            worksheet_names = [item.strip() for item in worksheet_setting.split(",") if item.strip()]
        else:
            worksheet_names = [sheet.title for sheet in workbook.worksheets()]

        records: list[dict[str, Any]] = []
        for worksheet_name in worksheet_names:
            worksheet = workbook.worksheet(worksheet_name)
            records.extend(worksheet.get_all_records())
        return records

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
            phone_values = [self._sanitize_phone(normalized_record.get(column)) for column in phone_columns]
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
            if any(token in column for token in ("telefone", "celular", "fone", "whatsapp", "phone"))
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
