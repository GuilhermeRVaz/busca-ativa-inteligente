import hashlib
import re

from message_catalog import MESSAGE_TEMPLATES as FALTAS_TEMPLATES
from institutional_message_catalog import INSTITUTIONAL_MESSAGE_TEMPLATES

CATALOG_MAP = {
    "faltas": FALTAS_TEMPLATES,
    "faltas_teste": FALTAS_TEMPLATES,
    "institutional": INSTITUTIONAL_MESSAGE_TEMPLATES,
}

def get_template_catalog(campaign_type: str):
    return CATALOG_MAP.get(campaign_type, FALTAS_TEMPLATES)

def generate_message(
    student_name: str,
    class_name: str,
    campaign_type: str = "faltas",
    *,
    parent_name: str = "Responsavel",
    school_name: str = "Escola",
    absence_days: str = "nao informado",
    unique_key: str | None = None,
) -> dict[str, str]:
    templates = get_template_catalog(campaign_type)
    if not templates:
        templates = FALTAS_TEMPLATES
        
    template = _choose_template(templates, unique_key)

    class_name_normalized = _normalize_class_name(class_name)
    class_name_short = _normalize_class_name_short(class_name)

    return {
        "template_id": template.template_id,
        "message": template.text.format(
            parent_name=(parent_name or "Responsavel").strip(),
            student_name=(student_name or "Aluno(a)").strip(),
            class_name=class_name_normalized,
            class_name_short=class_name_short,
            school_name=(school_name or "Escola").strip(),
            absence_days=(absence_days or "nao informado").strip(),
        ),
    }

def _choose_template(templates: list, unique_key: str | None):
    if not unique_key or not templates:
        return templates[0]

    digest = hashlib.sha256(unique_key.encode("utf-8")).hexdigest()
    index = int(digest[:8], 16) % len(templates)
    return templates[index]

def _normalize_class_name(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "nao informada"

    match = re.search(r"\b([6-9])\s*ANO\b.*?\b([A-Z])\b", text)
    if match:
        return f"{match.group(1)} ANO {match.group(2)}"

    return text

def _normalize_class_name_short(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "nao informada"
    text = re.sub(r"^\s*TURMA\s+", "", text)
    match = re.search(r"\b([6-9])\s*ANO\b.*?\b(?:[6-9]\s*)?([A-Z])\b", text)
    if match:
        return f"{match.group(1)} ANO {match.group(2)}"
    match_short = re.search(r"\b([6-9])\s*ANO\s+([A-Z])\b", text)
    if match_short:
        return f"{match_short.group(1)} ANO {match_short.group(2)}"
    return text or "nao informada"
