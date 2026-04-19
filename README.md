# CNPJ API

API REST de consulta de CNPJ com dados públicos da Receita Federal do Brasil.

## Stack

- **Framework:** FastAPI
- **Banco de dados:** SQLite (~60 GB, acesso assíncrono via aiosqlite)
- **Scheduler:** APScheduler (importação mensal automática)
- **Deploy:** VPS Linux (sem Docker)

## Setup rápido

```bash
# 1. Clonar e entrar na pasta
git clone <repo-url> cnpj-api && cd cnpj-api

# 2. Criar e ativar virtualenv
python3 -m venv .venv && source .venv/bin/activate

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Configurar variáveis de ambiente
cp .env.example .env
# editar .env com seus secrets

# 5. Rodar
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

A primeira execução cria o banco vazio em `data/cnpj.db`. O scheduler inicia a importação dos dados da Receita Federal automaticamente 30 segundos após o start.

## Endpoints

| Método | Rota | Auth | Descrição |
|--------|------|------|-----------|
| `GET` | `/` | — | Info da API |
| `GET` | `/status` | — | Health-check + último import |
| `GET` | `/v1/cnpj/{cnpj}` | ✅ | Consulta de CNPJ (14 dígitos) |
| `GET` | `/docs` | — | Documentação OpenAPI (Swagger) |

## Autenticação

A API aceita requisições de marketplaces (RapidAPI, Zyla) e acesso direto:

| Header | Variável `.env` |
|--------|-----------------|
| `X-RapidAPI-Proxy-Secret` | `RAPIDAPI_PROXY_SECRET` |
| `X-Zyla-API-Gateway-Secret` | `ZYLA_PROXY_SECRET` |
| `X-API-Key` | `MASTER_API_KEY` |

> Se **nenhum** secret estiver configurado, a auth é desativada (modo desenvolvimento).

## Variáveis de ambiente

Veja `.env.example` para a lista completa.
