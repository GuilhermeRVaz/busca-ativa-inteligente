import hashlib
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MessageTemplate:
    template_id: str
    text: str


MESSAGE_TEMPLATES = {
    "faltas": [
        MessageTemplate(
            template_id="absence_followup_1",
            text=(
                "Ola {parent_name}, aqui e da {school_name}. O(a) aluno(a) "
                "{student_name}, da turma {class_name}, esteve ausente no dia "
                "{absence_days}. Poderia nos informar o motivo?"
            ),
        ),
        MessageTemplate(
            template_id="absence_followup_2",
            text=(
                "Bom dia! A {school_name} identificou falta de {student_name}, "
                "da turma {class_name}, no dia {absence_days}. Se precisar, "
                "estamos a disposicao para apoiar."
            ),
        ),
        MessageTemplate(
            template_id="absence_followup_3",
            text=(
                "Entramos em contato sobre a ausencia de {student_name}, da "
                "turma {class_name}, no dia {absence_days}. Por favor, nos "
                "retorne quando puder."
            ),
        ),
    ],
    "reuniao": [
        MessageTemplate(
            template_id="meeting_notice_1",
            text=(
                "Ola {parent_name}, a {school_name} convida o responsavel de "
                "{student_name}, da turma {class_name}, para uma reuniao "
                "escolar. Em breve compartilharemos os detalhes."
            ),
        ),
        MessageTemplate(
            template_id="meeting_notice_2",
            text=(
                "Comunicamos a realizacao de reuniao para os responsaveis de "
                "{student_name}, da turma {class_name}. A {school_name} conta "
                "com sua participacao."
            ),
        ),
    ],
}


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
    templates = MESSAGE_TEMPLATES.get(campaign_type, MESSAGE_TEMPLATES["faltas"])
    template = _choose_template(templates, unique_key)

    return {
        "template_id": template.template_id,
        "message": template.text.format(
            parent_name=(parent_name or "Responsavel").strip(),
            student_name=(student_name or "Aluno(a)").strip(),
            class_name=_normalize_class_name(class_name),
            school_name=(school_name or "Escola").strip(),
            absence_days=(absence_days or "nao informado").strip(),
        ),
    }


def _choose_template(
    templates: list[MessageTemplate],
    unique_key: str | None,
) -> MessageTemplate:
    if not unique_key:
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
