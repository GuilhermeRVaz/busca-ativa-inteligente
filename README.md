# busca-ativa-inteligente

Simple Python project scaffold for an active outreach system that:

- sends WhatsApp messages using Evolution API
- receives incoming messages via FastAPI webhook
- classifies and drafts responses with an AI layer
- stores campaign and conversation data locally
- reads the legacy consolidated report to generate test campaigns

## Project structure

```text
busca-ativa-inteligente/
├── app/
├── core/
├── services/
├── ai/
├── data/
├── legacy/
├── tests/
├── utils/
├── .env.example
├── main.py
├── README.md
└── requirements.txt
```

## Quick start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python main.py
```

The API will start at `http://127.0.0.1:8000`.

## Main endpoints

- `GET /health`
- `POST /webhook/messages`
- `POST /campaigns/send`

## Campaign test flow

Generate a campaign from the legacy consolidated report:

```bash
python main.py --tipo faltas --dia 25
```

Generate a simple meeting campaign from contacts:

```bash
python main.py --tipo reuniao
```

## Notes

- The Evolution API integration is a minimal placeholder using `requests`.
- The AI layer is mocked so the project runs without external providers.
- Storage uses local JSON files to keep the initial version simple.
- The first campaign tests use the existing consolidated Excel report and Google Sheets contacts.
