import requests

url = "http://localhost:8080/message/sendText/escola-decia"

headers = {
    "apikey": "escola123",
    "Content-Type": "application/json"
}

payload = {
    "number": "5514981324832@s.whatsapp.net",
    "text": "teste busca ativa"
}

r = requests.post(url, json=payload, headers=headers)

print(r.status_code)
print(r.text)