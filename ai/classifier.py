import json

from openai import OpenAI

from core.config import settings
from core.logging import get_logger


logger = get_logger(__name__)

VALID_INTENCOES = {"RETORNAR", "NAO_QUER", "DUVIDA"}
VALID_MOTIVOS = {
    "TRANSPORTE",
    "TRABALHO",
    "SAUDE",
    "FAMILIA",
    "DESINTERESSE",
    "ROTINA",
    "FINANCEIRO",
    "OUTRO",
}

DEFAULT_RESULT = {
    "intencao": "DUVIDA",
    "motivo": "OUTRO",
    "observacao": "nao foi possivel classificar",
}


def classificar_mensagem(mensagem: str) -> dict[str, str]:
    if not mensagem.strip():
        logger.warning("Mensagem vazia recebida para classificacao; usando fallback")
        return DEFAULT_RESULT.copy()

    if not settings.openai_api_key:
        logger.warning("OPENAI_API_KEY nao configurada; usando fallback")
        return DEFAULT_RESULT.copy()

    try:
        client = OpenAI(api_key=settings.openai_api_key)
        response = client.chat.completions.create(
            model=settings.openai_model,
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Responda APENAS com JSON valido no formato: "
                        '{"intencao":"RETORNAR|NAO_QUER|DUVIDA",'
                        '"motivo":"TRANSPORTE|TRABALHO|SAUDE|FAMILIA|DESINTERESSE|'
                        'ROTINA|FINANCEIRO|OUTRO",'
                        '"observacao":"resumo curto da situacao do aluno"}'
                    ),
                },
                {"role": "user", "content": mensagem},
            ],
        )
        raw = (response.choices[0].message.content or "").strip()
        data = json.loads(raw)
    except Exception as exc:
        logger.warning("Falha ao classificar mensagem com OpenAI: %s", exc)
        return DEFAULT_RESULT.copy()

    intencao = str(data.get("intencao", "")).strip().upper()
    motivo = str(data.get("motivo", "")).strip().upper()
    observacao = str(data.get("observacao", "")).strip()

    if intencao not in VALID_INTENCOES:
        logger.warning("Intencao invalida retornada: %s", intencao)
        return DEFAULT_RESULT.copy()

    if motivo not in VALID_MOTIVOS:
        logger.warning("Motivo invalido retornado: %s", motivo)
        return DEFAULT_RESULT.copy()

    if not observacao:
        logger.warning("Observacao vazia retornada; usando fallback")
        return DEFAULT_RESULT.copy()

    return {
        "intencao": intencao,
        "motivo": motivo,
        "observacao": observacao,
    }


def classify_message(text: str) -> dict[str, str]:
    return classificar_mensagem(text)
