def classify_message(text: str) -> str:
    normalized = text.lower()

    if any(word in normalized for word in ["yes", "sim", "quero", "interesse"]):
        return "interested"
    if any(word in normalized for word in ["no", "nao", "não", "pare"]):
        return "not_interested"
    return "neutral"
