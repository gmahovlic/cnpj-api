import re
from typing import Optional

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException

from app.db.database import get_db
from app.middleware.auth import verify_auth

router = APIRouter(prefix="/v1", dependencies=[Depends(verify_auth)])

# ---------------------------------------------------------------------------
# Hardcoded domain maps (not available in Receita Federal CSV files)
# ---------------------------------------------------------------------------

PORTE_MAP = {
    "00": "Não informado",
    "01": "Micro Empresa",
    "03": "Empresa de Pequeno Porte",
    "05": "Demais",
}

SITUACAO_MAP = {
    "01": "Nula",
    "02": "Ativa",
    "03": "Suspensa",
    "04": "Inapta",
    "08": "Baixada",
}

IDENTIFICADOR_MATRIZ_FILIAL = {
    "1": "Matriz",
    "2": "Filial",
}

IDENTIFICADOR_SOCIO = {
    "1": "Pessoa Jurídica",
    "2": "Pessoa Física",
    "3": "Estrangeiro",
}

FAIXA_ETARIA_MAP = {
    "0": "Não se aplica",
    "1": "0 a 12 anos",
    "2": "13 a 20 anos",
    "3": "21 a 30 anos",
    "4": "31 a 40 anos",
    "5": "41 a 50 anos",
    "6": "51 a 60 anos",
    "7": "61 a 70 anos",
    "8": "71 a 80 anos",
    "9": "Maiores de 80 anos",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_cnpj(raw: str) -> str:
    return re.sub(r"\D", "", raw)


def _fmt_date(compact: Optional[str]) -> Optional[str]:
    if not compact or len(compact) != 8 or compact == "00000000":
        return None
    return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"


def _parse_capital(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    try:
        return float(raw.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def _code_desc(codigo: Optional[str], descricao: Optional[str]) -> Optional[dict]:
    if not codigo:
        return None
    return {"codigo": codigo, "descricao": descricao or ""}


def _simples_flag(value: Optional[str]) -> Optional[bool]:
    if not value:
        return None
    v = value.strip().upper()
    if v == "S":
        return True
    if v == "N":
        return False
    return None


async def _batch_lookup(
    db: aiosqlite.Connection,
    table: str,
    codes: set,
) -> dict[str, str]:
    """Resolve a set of domain codes in one query."""
    allowed = {"naturezas", "qualificacoes", "motivos", "municipios", "paises", "cnaes"}
    if table not in allowed or not codes:
        return {}
    codes_list = list(codes)
    placeholders = ",".join("?" * len(codes_list))
    cursor = await db.execute(
        f"SELECT codigo, descricao FROM {table} WHERE codigo IN ({placeholders})",
        codes_list,
    )
    return {r["codigo"]: r["descricao"] for r in await cursor.fetchall()}


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/cnpj/{cnpj}",
    summary="Consultar CNPJ",
    description=(
        "Retorna os dados cadastrais completos de um CNPJ, incluindo empresa, "
        "estabelecimento, sócios, Simples Nacional, MEI e se possui filiais "
        "(campo `hasBranches`)."
    ),
)
async def consultar_cnpj(cnpj: str, db: aiosqlite.Connection = Depends(get_db)):
    digits = _clean_cnpj(cnpj)
    if len(digits) != 14:
        raise HTTPException(
            status_code=400,
            detail={"error": "CNPJ deve conter 14 dígitos"},
        )

    basico = digits[:8]
    ordem = digits[8:12]
    dv = digits[12:14]

    # ---- Empresa ----
    cur = await db.execute(
        "SELECT * FROM empresas WHERE cnpj_basico = ?", (basico,)
    )
    empresa = await cur.fetchone()
    if not empresa:
        raise HTTPException(
            status_code=404,
            detail={"error": "CNPJ não encontrado"},
        )

    # ---- Estabelecimento ----
    cur = await db.execute(
        "SELECT * FROM estabelecimentos "
        "WHERE cnpj_basico = ? AND cnpj_ordem = ? AND cnpj_dv = ?",
        (basico, ordem, dv),
    )
    estab = await cur.fetchone()
    if not estab:
        raise HTTPException(
            status_code=404,
            detail={"error": "Estabelecimento não encontrado para este CNPJ"},
        )

    # ---- Simples ----
    cur = await db.execute(
        "SELECT * FROM simples WHERE cnpj_basico = ?", (basico,)
    )
    simples = await cur.fetchone()

    # ---- Sócios ----
    cur = await db.execute(
        "SELECT * FROM socios WHERE cnpj_basico = ?", (basico,)
    )
    socios_rows = await cur.fetchall()

    # ---- Filiais (branches check) ----
    cur = await db.execute(
        "SELECT 1 FROM estabelecimentos "
        "WHERE cnpj_basico = ? AND identificador = '2' LIMIT 1",
        (basico,),
    )
    has_branches = await cur.fetchone() is not None

    # ---- Batch domain lookups ----
    nat_codes = {empresa["natureza_juridica"]} if empresa["natureza_juridica"] else set()
    nat_map = await _batch_lookup(db, "naturezas", nat_codes)

    qual_codes: set[str] = set()
    if empresa["qualificacao_responsavel"]:
        qual_codes.add(empresa["qualificacao_responsavel"])
    for s in socios_rows:
        if s["qualificacao"]:
            qual_codes.add(s["qualificacao"])
        if s["representante_qualificacao"]:
            qual_codes.add(s["representante_qualificacao"])
    qual_map = await _batch_lookup(db, "qualificacoes", qual_codes)

    motivo_codes = {estab["motivo_situacao_cadastral"]} if estab["motivo_situacao_cadastral"] else set()
    motivo_map = await _batch_lookup(db, "motivos", motivo_codes)

    mun_codes = {estab["municipio"]} if estab["municipio"] else set()
    mun_map = await _batch_lookup(db, "municipios", mun_codes)

    pais_codes: set[str] = set()
    if estab["pais"]:
        pais_codes.add(estab["pais"])
    for s in socios_rows:
        if s["pais"]:
            pais_codes.add(s["pais"])
    pais_map = await _batch_lookup(db, "paises", pais_codes)

    cnae_codes: set[str] = set()
    if estab["cnae_fiscal_principal"]:
        cnae_codes.add(estab["cnae_fiscal_principal"])
    sec_codes: list[str] = []
    if estab["cnae_fiscal_secundaria"]:
        sec_codes = [c.strip() for c in estab["cnae_fiscal_secundaria"].split(",") if c.strip()]
        cnae_codes.update(sec_codes)
    cnae_map = await _batch_lookup(db, "cnaes", cnae_codes)

    # ---- Build response ----
    porte_code = empresa["porte"] or ""
    sit_code = estab["situacao_cadastral"] or ""

    telefones = []
    if estab["telefone1"]:
        telefones.append({"ddd": estab["ddd1"] or "", "numero": estab["telefone1"]})
    if estab["telefone2"]:
        telefones.append({"ddd": estab["ddd2"] or "", "numero": estab["telefone2"]})

    socios_list = []
    for s in socios_rows:
        representante = None
        if s["representante_nome"]:
            representante = {
                "nome": s["representante_nome"],
                "cpf_cnpj": s["representante_cpf_cnpj"] or None,
                "qualificacao": _code_desc(
                    s["representante_qualificacao"],
                    qual_map.get(s["representante_qualificacao"]),
                ),
            }
        socios_list.append({
            "tipo": IDENTIFICADOR_SOCIO.get(
                s["identificador_socio"], s["identificador_socio"]
            ),
            "nome": s["nome"],
            "cpf_cnpj": s["cpf_cnpj"] or None,
            "qualificacao": _code_desc(
                s["qualificacao"], qual_map.get(s["qualificacao"])
            ),
            "data_entrada": _fmt_date(s["data_entrada"]),
            "pais": _code_desc(s["pais"], pais_map.get(s["pais"])),
            "faixa_etaria": FAIXA_ETARIA_MAP.get(
                s["faixa_etaria"], s["faixa_etaria"]
            ),
            "representante": representante,
        })

    return {
        "cnpj": digits,
        "identificador_matriz_filial": IDENTIFICADOR_MATRIZ_FILIAL.get(
            estab["identificador"], estab["identificador"]
        ),
        "razao_social": empresa["razao_social"],
        "nome_fantasia": estab["nome_fantasia"] or None,
        "situacao_cadastral": {
            "codigo": sit_code,
            "descricao": SITUACAO_MAP.get(sit_code, ""),
        },
        "data_situacao_cadastral": _fmt_date(estab["data_situacao_cadastral"]),
        "motivo_situacao_cadastral": _code_desc(
            estab["motivo_situacao_cadastral"],
            motivo_map.get(estab["motivo_situacao_cadastral"]),
        ),
        "data_inicio_atividade": _fmt_date(estab["data_inicio_atividade"]),
        "natureza_juridica": _code_desc(
            empresa["natureza_juridica"],
            nat_map.get(empresa["natureza_juridica"]),
        ),
        "qualificacao_responsavel": _code_desc(
            empresa["qualificacao_responsavel"],
            qual_map.get(empresa["qualificacao_responsavel"]),
        ),
        "capital_social": _parse_capital(empresa["capital_social"]),
        "porte": {
            "codigo": porte_code,
            "descricao": PORTE_MAP.get(porte_code, ""),
        },
        "ente_federativo": empresa["ente_federativo"] or None,
        "cnae_fiscal_principal": _code_desc(
            estab["cnae_fiscal_principal"],
            cnae_map.get(estab["cnae_fiscal_principal"]),
        ),
        "cnaes_fiscais_secundarios": [
            {"codigo": c, "descricao": cnae_map.get(c, "")} for c in sec_codes
        ],
        "endereco": {
            "tipo_logradouro": estab["tipo_logradouro"] or None,
            "logradouro": estab["logradouro"] or None,
            "numero": estab["numero"] or None,
            "complemento": estab["complemento"] or None,
            "bairro": estab["bairro"] or None,
            "cep": estab["cep"] or None,
            "uf": estab["uf"] or None,
            "municipio": _code_desc(
                estab["municipio"], mun_map.get(estab["municipio"])
            ),
        },
        "nome_cidade_exterior": estab["nome_cidade_exterior"] or None,
        "pais": _code_desc(estab["pais"], pais_map.get(estab["pais"])),
        "telefones": telefones,
        "fax": (
            {"ddd": estab["ddd_fax"] or "", "numero": estab["fax"]}
            if estab["fax"]
            else None
        ),
        "correio_eletronico": estab["correio_eletronico"] or None,
        "situacao_especial": estab["situacao_especial"] or None,
        "data_situacao_especial": _fmt_date(estab["data_situacao_especial"]),
        "simples_nacional": (
            {
                "optante": _simples_flag(simples["opcao_simples"]),
                "data_opcao": _fmt_date(simples["data_opcao_simples"]),
                "data_exclusao": _fmt_date(simples["data_exclusao_simples"]),
            }
            if simples
            else None
        ),
        "mei": (
            {
                "optante": _simples_flag(simples["opcao_mei"]),
                "data_opcao": _fmt_date(simples["data_opcao_mei"]),
                "data_exclusao": _fmt_date(simples["data_exclusao_mei"]),
            }
            if simples
            else None
        ),
        "socios": socios_list,
        "hasBranches": has_branches,
    }


@router.get("/cnpj/{cnpj}/filiais")
async def listar_filiais(
    cnpj: str,
    page: int = 1,
    db: aiosqlite.Connection = Depends(get_db),
):
    digits = _clean_cnpj(cnpj)
    if len(digits) != 14:
        raise HTTPException(
            status_code=400,
            detail={"error": "CNPJ deve conter 14 dígitos"},
        )

    basico = digits[:8]

    # Verify the CNPJ belongs to a matriz (identificador = '1')
    cur = await db.execute(
        "SELECT identificador FROM estabelecimentos "
        "WHERE cnpj_basico = ? AND cnpj_ordem = ? AND cnpj_dv = ?",
        (basico, digits[8:12], digits[12:14]),
    )
    matriz = await cur.fetchone()
    if not matriz:
        raise HTTPException(
            status_code=404,
            detail={"error": "CNPJ não encontrado"},
        )
    if matriz["identificador"] != "1":
        raise HTTPException(
            status_code=400,
            detail={"error": "O CNPJ informado não é de uma matriz"},
        )

    if page < 1:
        page = 1
    limit = 50
    offset = (page - 1) * limit

    # Total count
    cur = await db.execute(
        "SELECT COUNT(*) as total FROM estabelecimentos "
        "WHERE cnpj_basico = ? AND identificador = '2'",
        (basico,),
    )
    total = (await cur.fetchone())["total"]

    # Page results
    cur = await db.execute(
        "SELECT cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, "
        "situacao_cadastral, uf, municipio "
        "FROM estabelecimentos "
        "WHERE cnpj_basico = ? AND identificador = '2' "
        "ORDER BY cnpj_ordem, cnpj_dv "
        "LIMIT ? OFFSET ?",
        (basico, limit, offset),
    )
    rows = await cur.fetchall()

    mun_codes = {r["municipio"] for r in rows if r["municipio"]}
    mun_map = await _batch_lookup(db, "municipios", mun_codes)

    filiais = []
    for r in rows:
        cnpj_full = f"{r['cnpj_basico']}{r['cnpj_ordem']}{r['cnpj_dv']}"
        sit = r["situacao_cadastral"] or ""
        filiais.append({
            "cnpj": cnpj_full,
            "nome_fantasia": r["nome_fantasia"] or None,
            "situacao_cadastral": {
                "codigo": sit,
                "descricao": SITUACAO_MAP.get(sit, ""),
            },
            "uf": r["uf"] or None,
            "municipio": _code_desc(r["municipio"], mun_map.get(r["municipio"])),
        })

    total_pages = (total + limit - 1) // limit if total > 0 else 0

    return {
        "cnpj_matriz": digits,
        "total_filiais": total,
        "page": page,
        "total_pages": total_pages,
        "filiais": filiais,
    }
