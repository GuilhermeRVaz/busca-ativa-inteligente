# busca-ativa-inteligente

Projeto FastAPI para busca ativa escolar com:

- envio de mensagens via WhatsApp/Evolution API
- recebimento de mensagens via webhook
- classificacao de respostas com IA
- leitura de contatos em Google Sheets
- persistencia de interacoes em Google Sheets com backup local em JSON

## Estrutura

```text
busca-ativa-inteligente/
|-- app/
|-- core/
|-- services/
|-- ai/
|-- data/
|-- providers/
|-- tests/
|-- .env.example
|-- main.py
|-- README.md
```

## Inicio rapido

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python main.py
```

A API sobe em `http://127.0.0.1:8000`.

## Endpoints

- `GET /health`
- `POST /webhook/messages`
- `POST /campaigns/send`

## Configuracao do .env

Preencha pelo menos estas variaveis:

```env
APP_NAME=busca-ativa-inteligente
APP_HOST=0.0.0.0
APP_PORT=8000
DEBUG=true

WEBHOOK_VERIFY_TOKEN=change-me
DATA_DIR=data/storage
CONSOLIDATED_REPORT_PATH=relatorios/Relatorio_Consolidado_BuscaAtiva.xlsx

EVOLUTION_API_URL=https://sua-evolution.example.com
EVOLUTION_API_KEY=sua-chave
EVOLUTION_API_INSTANCE=sua-instancia
EVOLUTION_TIMEOUT_SECONDS=30
SEND_MIN_DELAY_SECONDS=30
SEND_MAX_DELAY_SECONDS=90
SEND_BATCH_EXTRA_EVERY=10
SEND_BATCH_EXTRA_DELAY_MIN_SECONDS=420
SEND_BATCH_EXTRA_DELAY_MAX_SECONDS=900

GOOGLE_SERVICE_ACCOUNT_FILE=service-account.json

GOOGLE_SHEET_CONTATOS_URL=https://docs.google.com/spreadsheets/d/...
GOOGLE_SHEET_CONTATOS_WORKSHEET=Contatos

GOOGLE_SHEET_DADOS_URL=https://docs.google.com/spreadsheets/d/...
GOOGLE_SHEET_DADOS_WORKSHEET=Interacoes
```

## Google Sheets

O sistema usa duas planilhas separadas:

- contatos: usada para leitura dos alunos e telefones de campanha
- dados: usada para registrar as interacoes recebidas via webhook

### Como compartilhar com a service account

1. Crie ou baixe a service account no Google Cloud.
2. Salve o JSON localmente no projeto ou em outro caminho seguro.
3. Configure `GOOGLE_SERVICE_ACCOUNT_FILE` apontando para esse arquivo.
4. Copie o e-mail da service account.
5. Abra cada planilha no Google Sheets e clique em `Compartilhar`.
6. Compartilhe as duas planilhas com o e-mail da service account com permissao de `Editor`.

## Estrutura esperada das abas

### Aba de contatos

Pode usar os nomes atuais de colunas. O sistema aceita aliases comuns como:

- `Nome do Aluno`, `nome_aluno`, `student_name`
- `Turma`, `class_name`
- `Telefone 1`, `Telefone 2`, `Telefone 3`

### Aba de dados

Use estes cabecalhos:

- `data_hora`
- `telefone`
- `mensagem`
- `intencao`
- `motivo`
- `observacao`
- `campaign_id`
- `origem`

`origem` comeca como `whatsapp`, o que facilita futuras integracoes.

## Resiliencia

Toda interacao recebida via webhook e salva primeiro em:

- `data/storage/incoming_messages.json`

Depois disso, o sistema tenta gravar na planilha de dados. Se o Google Sheets falhar, o processamento continua e o backup local permanece salvo.

## Fluxo de campanha

Gerar campanha de faltas:

```bash
python main.py --tipo faltas --dia 25
```

Gerar campanha de reuniao:

```bash
python main.py --tipo reuniao
```

Gerar campanha sem enviar para a Evolution:

```bash
python main.py --tipo reuniao --dry-run --max-items 3
```

Executar diagnostico de pre-voo:

```bash
python main.py --diagnostico
```

## Homologacao segura

Para testar com 2 ou 3 numeros controlados:

1. Crie uma aba ou planilha de contatos de teste com apenas esses registros.
2. Aponte `GOOGLE_SHEET_CONTATOS_URL` e `GOOGLE_SHEET_CONTATOS_WORKSHEET` para essa origem.
3. Rode primeiro `python main.py --tipo reuniao --dry-run --max-items 3`.
4. Rode depois `python main.py --tipo reuniao --max-items 3`.
5. Responda uma mensagem e valide `incoming_messages.json` e a planilha de dados.
