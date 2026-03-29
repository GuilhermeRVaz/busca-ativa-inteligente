def generate_reply(text: str, classification: str) -> str:
    if classification == "interested":
        return "Perfeito! Vou seguir com as proximas informacoes."
    if classification == "not_interested":
        return "Tudo bem. Obrigado pelo retorno."
    return "Recebi sua mensagem. Em breve seguiremos com o atendimento."
