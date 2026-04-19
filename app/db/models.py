SCHEMA = """
CREATE TABLE IF NOT EXISTS empresas (
    cnpj_basico        TEXT PRIMARY KEY,
    razao_social       TEXT,
    natureza_juridica  TEXT,
    qualificacao_responsavel TEXT,
    capital_social     TEXT,
    porte              TEXT,
    ente_federativo    TEXT
);

CREATE TABLE IF NOT EXISTS estabelecimentos (
    cnpj_basico              TEXT NOT NULL,
    cnpj_ordem               TEXT NOT NULL,
    cnpj_dv                  TEXT NOT NULL,
    identificador            TEXT,
    nome_fantasia            TEXT,
    situacao_cadastral       TEXT,
    data_situacao_cadastral  TEXT,
    motivo_situacao_cadastral TEXT,
    nome_cidade_exterior     TEXT,
    pais                     TEXT,
    data_inicio_atividade    TEXT,
    cnae_fiscal_principal    TEXT,
    cnae_fiscal_secundaria   TEXT,
    tipo_logradouro          TEXT,
    logradouro               TEXT,
    numero                   TEXT,
    complemento              TEXT,
    bairro                   TEXT,
    cep                      TEXT,
    uf                       TEXT,
    municipio                TEXT,
    ddd1                     TEXT,
    telefone1                TEXT,
    ddd2                     TEXT,
    telefone2                TEXT,
    ddd_fax                  TEXT,
    fax                      TEXT,
    correio_eletronico       TEXT,
    situacao_especial        TEXT,
    data_situacao_especial   TEXT,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE TABLE IF NOT EXISTS simples (
    cnpj_basico          TEXT PRIMARY KEY,
    opcao_simples        TEXT,
    data_opcao_simples   TEXT,
    data_exclusao_simples TEXT,
    opcao_mei            TEXT,
    data_opcao_mei       TEXT,
    data_exclusao_mei    TEXT
);

CREATE TABLE IF NOT EXISTS socios (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    cnpj_basico               TEXT NOT NULL,
    identificador_socio       TEXT,
    nome                      TEXT,
    cpf_cnpj                  TEXT,
    qualificacao              TEXT,
    data_entrada              TEXT,
    pais                      TEXT,
    representante_cpf_cnpj    TEXT,
    representante_nome        TEXT,
    representante_qualificacao TEXT,
    faixa_etaria              TEXT
);

CREATE TABLE IF NOT EXISTS cnaes (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS naturezas (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS qualificacoes (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS motivos (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS municipios (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paises (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_estab_basico ON estabelecimentos(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socios_basico ON socios(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_runs_batch ON import_runs(batch, status);
"""
