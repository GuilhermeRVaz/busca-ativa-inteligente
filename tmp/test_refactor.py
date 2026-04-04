"""
Script de verificacao rapida das refatoracoes de classificacao e validacao de telefone.
Execute com: .\\venv\\Scripts\\python.exe tmp\\test_refactor.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.classifier import classificar_mensagem, VALID_INTENCOES, VALID_MOTIVOS
from services.webhook_service import WebhookService


# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("PARTE 1 — Validacao de telefone")
print("=" * 60)

svc = WebhookService()

# @lid deve ser ignorado
p_lid = {"data": {"key": {"remoteJid": "22153491648743@lid", "fromMe": False, "id": "ID_LID"}, "message": {"conversation": "teste"}}}
t_lid = svc._normalize_and_validate_phone(p_lid)
ok1 = t_lid is None
print(f"  @lid   => {t_lid!r:20s} {'OK' if ok1 else 'FALHA'} (esperado: None)")

# Número válido
p_valid = {"data": {"key": {"remoteJid": "5514991234567@s.whatsapp.net", "fromMe": False, "id": "ID_V"}, "message": {"conversation": "teste"}}}
t_valid = svc._normalize_and_validate_phone(p_valid)
ok2 = t_valid == "5514991234567"
print(f"  válido => {t_valid!r:20s} {'OK' if ok2 else 'FALHA'} (esperado: 5514991234567)")

# Vazio
p_empty = {"data": {"key": {"remoteJid": "", "fromMe": False, "id": "ID_E"}, "message": {"conversation": "teste"}}}
t_empty = svc._normalize_and_validate_phone(p_empty)
ok3 = t_empty is None
print(f"  vazio  => {t_empty!r:20s} {'OK' if ok3 else 'FALHA'} (esperado: None)")

# Número sem 55
p_sem55 = {"data": {"key": {"remoteJid": "14991234567", "fromMe": False, "id": "ID_S"}, "message": {"conversation": "teste"}}}
t_sem55 = svc._normalize_and_validate_phone(p_sem55)
ok4 = t_sem55 == "5514991234567"
print(f"  sem55  => {t_sem55!r:20s} {'OK' if ok4 else 'FALHA'} (esperado: 5514991234567)")

# ─────────────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("PARTE 2 — Vocabulario do classificador")
print("=" * 60)

intencoes_esperadas = {"JUSTIFICOU", "VAI_REGULARIZAR", "DUVIDA", "RESISTENCIA", "NAO_IDENTIFICADO"}
motivos_esperados = {"SAUDE", "TRANSPORTE", "FAMILIAR", "ESCOLAR", "LOGISTICA", "OUTROS"}

ok5 = VALID_INTENCOES == intencoes_esperadas
ok6 = VALID_MOTIVOS == motivos_esperados

print(f"  VALID_INTENCOES = {VALID_INTENCOES}")
print(f"  = {intencoes_esperadas} ? {'OK' if ok5 else 'FALHA'}")
print(f"  VALID_MOTIVOS = {VALID_MOTIVOS}")
print(f"  = {motivos_esperados} ? {'OK' if ok6 else 'FALHA'}")

# ─────────────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("PARTE 3 — Classificacao (chamada real via OpenAI)")
print("=" * 60)

test_cases = [
    ("levei no dentista",       "JUSTIFICOU",       "SAUDE"),
    ("estava com febre",        "JUSTIFICOU",       "SAUDE"),
    ("vai amanha",              "VAI_REGULARIZAR",  None),
    ("nao vou mandar mais",     "RESISTENCIA",      None),
    ("qual foi a materia",      "DUVIDA",           None),
]

all_ok = True
for msg, expected_intencao, expected_motivo in test_cases:
    result = classificar_mensagem(msg)
    intencao = result["intencao"]
    motivo = result["motivo"]
    ok_i = intencao == expected_intencao
    ok_m = expected_motivo is None or motivo == expected_motivo
    status = "OK" if (ok_i and ok_m) else "FALHA"
    if not (ok_i and ok_m):
        all_ok = False
    print(f"  [{status}] \"{msg}\"")
    print(f"          intencao={intencao} (esperado={expected_intencao}) {'OK' if ok_i else 'FALHA'}")
    if expected_motivo:
        print(f"          motivo={motivo} (esperado={expected_motivo}) {'OK' if ok_m else 'FALHA'}")

# ─────────────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
phone_ok = ok1 and ok2 and ok3 and ok4
vocab_ok = ok5 and ok6
total_ok = phone_ok and vocab_ok and all_ok
print(f"Telefone:      {'PASS' if phone_ok else 'FAIL'}")
print(f"Vocabulário:   {'PASS' if vocab_ok else 'FAIL'}")
print(f"Classificação: {'PASS' if all_ok else 'FAIL'}")
print(f"RESULTADO FINAL: {'TODOS OS TESTES PASSARAM' if total_ok else 'HA FALHAS ACIMA'}")
print("=" * 60)
