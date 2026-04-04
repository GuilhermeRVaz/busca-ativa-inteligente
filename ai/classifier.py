"""
Classificador de mensagens de pais/responsáveis para Busca Ativa Escolar.

Vocabulário alinhado com o schema do Supabase:
  intencao: JUSTIFICOU | VAI_REGULARIZAR | DUVIDA | RESISTENCIA | NAO_IDENTIFICADO
  motivo:   SAUDE | TRANSPORTE | FAMILIAR | ESCOLAR | LOGISTICA | OUTROS
"""

import json

from openai import OpenAI

from core.config import settings
from core.logging import get_logger


logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Vocabulário válido (deve bater com CHECK constraints do Supabase)
# ---------------------------------------------------------------------------

VALID_INTENCOES = {
    "JUSTIFICOU",
    "VAI_REGULARIZAR",
    "DUVIDA",
    "RESISTENCIA",
    "NAO_IDENTIFICADO",
}

VALID_MOTIVOS = {
    "SAUDE",
    "TRANSPORTE",
    "FAMILIAR",
    "ESCOLAR",
    "LOGISTICA",
    "OUTROS",
}

DEFAULT_RESULT: dict[str, str] = {
    "intencao": "NAO_IDENTIFICADO",
    "motivo": "OUTROS",
    "observacao": "nao foi possivel classificar",
}

# ---------------------------------------------------------------------------
# Prompt do sistema — exemplos explícitos para reduzir erros do LLM
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
Você classifica mensagens de pais/responsáveis de alunos ausentes na escola.

Responda APENAS com JSON válido, sem markdown, no formato exato:
{"intencao":"<VALOR>","motivo":"<VALOR>","observacao":"<resumo curto em português>"}

INTENCAO — escolha UMA:
  JUSTIFICOU      → responsável explicou o motivo da falta (dentista, doente, viagem, etc.)
  VAI_REGULARIZAR → responsável diz que o aluno voltará ou vai resolver (amanhã vai, semana que vem)
  DUVIDA          → responsável está perguntando algo (qual matéria? o que aconteceu?)
  RESISTENCIA     → responsável se recusa, protesta ou não quer cooperar
  NAO_IDENTIFICADO → mensagem sem contexto claro sobre a ausência

MOTIVO — escolha UM (obrigatório quando intencao=JUSTIFICOU, OUTROS nos demais):
  SAUDE      → doença, médico, dentista, hospital, remédio, febre
  TRANSPORTE → ônibus, carro, moto, condução, não teve transporte
  FAMILIAR   → familiar doente, problema em casa, luto, emergência familiar
  ESCOLAR    → problema com a escola, bullying, dificuldade escolar
  LOGISTICA  → trabalho do responsável, não teve com quem deixar, horário
  OUTROS     → qualquer outro motivo não listado acima

Exemplos:
  "levei no dentista"           → JUSTIFICOU / SAUDE
  "estava com febre"            → JUSTIFICOU / SAUDE
  "problema no carro"           → JUSTIFICOU / TRANSPORTE
  "vai amanhã"                  → VAI_REGULARIZAR / OUTROS
  "qual foi a matéria?"         → DUVIDA / OUTROS
  "não vou mandar mais"         → RESISTENCIA / OUTROS
  "tá bom"                      → NAO_IDENTIFICADO / OUTROS
  "oi"                          → NAO_IDENTIFICADO / OUTROS
"""

# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------


def classificar_mensagem(mensagem: str) -> dict[str, str]:
    """
    Classifica uma mensagem de WhatsApp usando OpenAI.
    Retorna dict com chaves: intencao, motivo, observacao.
    Nunca lança exceção — fallback para DEFAULT_RESULT em qualquer erro.
    """
    if not mensagem.strip():
        logger.warning("classificacao | mensagem vazia | usando fallback")
        return DEFAULT_RESULT.copy()

    if not settings.openai_api_key:
        logger.warning("classificacao | OPENAI_API_KEY ausente | usando fallback")
        return DEFAULT_RESULT.copy()

    try:
        client = OpenAI(api_key=settings.openai_api_key)
        response = client.chat.completions.create(
            model=settings.openai_model,
            temperature=0,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": mensagem},
            ],
        )
        raw = (response.choices[0].message.content or "").strip()
        data = json.loads(raw)
    except Exception as exc:
        logger.warning("classificacao | falha OpenAI | erro=%s | usando fallback", exc)
        return DEFAULT_RESULT.copy()

    intencao = str(data.get("intencao", "")).strip().upper()
    motivo = str(data.get("motivo", "")).strip().upper()
    observacao = str(data.get("observacao", "")).strip()

    # Validar intencao
    if intencao not in VALID_INTENCOES:
        logger.warning("classificacao | intencao invalida=%s | usando fallback", intencao)
        intencao = "NAO_IDENTIFICADO"

    # Validar motivo
    if motivo not in VALID_MOTIVOS:
        logger.warning("classificacao | motivo invalido=%s | corrigindo para OUTROS", motivo)
        motivo = "OUTROS"

    # Observação padrão se vazia
    if not observacao:
        observacao = f"classificado como {intencao.lower()}"

    result = {
        "intencao": intencao,
        "motivo": motivo,
        "observacao": observacao,
    }

    logger.info(
        "classificacao aplicada | intencao=%s | motivo=%s | msg=%.60s",
        intencao,
        motivo,
        mensagem,
    )
    return result


# Alias para compatibilidade com código legado
def classify_message(text: str) -> dict[str, str]:
    return classificar_mensagem(text)
