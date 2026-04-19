"""Receita Federal data import job.

Adapted from the Django-based import_receita_latest.py — stripped of all
Person / WorkRelationship / Event logic.  Downloads the monthly open-data
dump and stores the raw rows in SQLite for the CNPJ lookup API.
"""

import csv
import datetime as dt
import re
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional
from urllib.parse import (
    parse_qs,
    quote,
    unquote,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)
from xml.etree import ElementTree
from zipfile import ZipFile

import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import settings

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHARE_URL_PATTERN = re.compile(
    r"^(?P<origin>https?://[^/]+)/index\.php/s/(?P<token>[^/?#]+)"
)

CATEGORIES: List[str] = [
    "Cnaes",
    "Empresas",
    "Estabelecimentos",
    "Motivos",
    "Municipios",
    "Naturezas",
    "Paises",
    "Qualificacoes",
    "Simples",
    "Socios",
]

DOMAIN_TABLES: Dict[str, str] = {
    "Cnaes": "cnaes",
    "Naturezas": "naturezas",
    "Qualificacoes": "qualificacoes",
    "Motivos": "motivos",
    "Municipios": "municipios",
    "Paises": "paises",
}

BATCH_SIZE = 50_000
MAX_RUNNING_MINUTES = 360

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

scheduler = AsyncIOScheduler()


def setup_scheduler() -> None:
    scheduler.add_job(
        import_receita,
        "interval",
        hours=settings.import_interval_hours,
        id="import_receita",
        max_instances=1,
        coalesce=True,
        next_run_time=dt.datetime.now() + dt.timedelta(seconds=30),
    )


# ---------------------------------------------------------------------------
# Main import function
# ---------------------------------------------------------------------------


def import_receita() -> None:
    """Download and import the latest Receita Federal batch into SQLite."""

    db_path = Path(settings.database_path)
    if not db_path.exists():
        print("[receita] Database file not found — skipping import", flush=True)
        return

    conn = sqlite3.connect(str(db_path), timeout=300)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-256000")  # 256 MB
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA mmap_size=268435456")  # 256 MB

    try:
        _cleanup_stale_runs(conn)

        session = requests.Session()
        session.headers.update({"Connection": "keep-alive"})

        print("[receita] Resolving latest batch…", flush=True)
        result = _resolve_latest_batch(session)
        if result is None:
            print("[receita] No valid batch found — will retry next run", flush=True)
            return
        base_url, batch_date, links = result
        batch_name = batch_date.strftime("%Y-%m")
        print(f"[receita] Latest batch: {batch_name} ({base_url})", flush=True)

        # Already imported?
        cur = conn.execute(
            "SELECT id FROM import_runs WHERE batch = ? AND status = 'success'",
            (batch_name,),
        )
        if cur.fetchone():
            print(f"[receita] Batch {batch_name} already imported", flush=True)
            return

        # Another run in progress?
        cur = conn.execute("SELECT id FROM import_runs WHERE status = 'running'")
        if cur.fetchone():
            print("[receita] Another import is already running", flush=True)
            return

        # Create run record
        now = dt.datetime.now().isoformat()
        conn.execute(
            "INSERT INTO import_runs (batch, status, started_at) VALUES (?, 'running', ?)",
            (batch_name, now),
        )
        conn.commit()
        run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        print(f"[receita] Run #{run_id} started", flush=True)

        try:
            with tempfile.TemporaryDirectory(prefix="receita_") as tmp_dir:
                root = Path(tmp_dir)

                for category, table in DOMAIN_TABLES.items():
                    print(f"[receita] Importing {table}…", flush=True)
                    count = _stream_category(
                        session, links, category, root,
                        lambda files, t=table: _import_domain_files(conn, files, t),
                    )
                    print(f"[receita]   {table}: {count:,} rows", flush=True)

                for label, category, fn in [
                    ("empresas", "Empresas", lambda f: _import_empresas_files(conn, f)),
                    ("estabelecimentos", "Estabelecimentos", lambda f: _import_estabelecimentos_files(conn, f)),
                    ("simples", "Simples", lambda f: _import_simples_files(conn, f)),
                ]:
                    print(f"[receita] Importing {label}…", flush=True)
                    count = _stream_category(session, links, category, root, fn)
                    print(f"[receita]   Total: {count:,} {label}", flush=True)

                # Socios uses a staging table for atomic swap
                print("[receita] Importing socios…", flush=True)
                _setup_socios_staging(conn)
                count = _stream_category(
                    session, links, "Socios", root,
                    lambda f: _import_socios_files(conn, f),
                )
                _finalize_socios_staging(conn)
                print(f"[receita]   Total: {count:,} socios", flush=True)

            conn.execute("ANALYZE")
            conn.execute(
                "UPDATE import_runs SET status = 'success', finished_at = ? WHERE id = ?",
                (dt.datetime.now().isoformat(), run_id),
            )
            conn.commit()
            print(f"[receita] Batch {batch_name} imported successfully", flush=True)

        except BaseException as exc:
            conn.execute(
                "UPDATE import_runs SET status = 'failure', finished_at = ?, notes = ? WHERE id = ?",
                (dt.datetime.now().isoformat(), str(exc)[:1000], run_id),
            )
            conn.commit()
            print(f"[receita] Import failed: {exc}", flush=True)
            raise

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Streaming helper
# ---------------------------------------------------------------------------


def _stream_category(
    session: requests.Session,
    links: Dict[str, List[str]],
    category: str,
    root: Path,
    process_files,
) -> int:
    """Download one zip at a time, process its CSVs, delete them."""
    total = 0
    for url in links.get(category, []):
        files = _download_and_extract(session, url, root)
        try:
            total += process_files(files)
        finally:
            for f in files:
                f.unlink(missing_ok=True)
    return total


# ---------------------------------------------------------------------------
# Table import helpers
# ---------------------------------------------------------------------------


def _import_domain_files(conn: sqlite3.Connection, files: List[Path], table: str) -> int:
    count = 0
    batch: list[tuple] = []
    for fp in files:
        for row in _iter_csv_rows(fp):
            if len(row) < 2:
                continue
            code, desc = row[0].strip(), row[1].strip()
            if not code or code.lower() == "codigo":
                continue
            batch.append((code, desc))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    f"INSERT OR REPLACE INTO {table} (codigo, descricao) VALUES (?, ?)",
                    batch,
                )
                conn.commit()
                count += len(batch)
                batch.clear()
    if batch:
        conn.executemany(
            f"INSERT OR REPLACE INTO {table} (codigo, descricao) VALUES (?, ?)",
            batch,
        )
        conn.commit()
        count += len(batch)
    return count


def _import_empresas_files(conn: sqlite3.Connection, files: List[Path]) -> int:
    count = 0
    batch: list[tuple] = []
    sql = (
        "INSERT OR REPLACE INTO empresas "
        "(cnpj_basico, razao_social, natureza_juridica, "
        "qualificacao_responsavel, capital_social, porte, ente_federativo) "
        "VALUES (?,?,?,?,?,?,?)"
    )
    for fp in files:
        for row in _iter_csv_rows(fp):
            if len(row) < 7:
                continue
            batch.append((
                _digits_only(row[0]).zfill(8),
                row[1], row[2], row[3], row[4], row[5], row[6],
            ))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(sql, batch)
                conn.commit()
                count += len(batch)
                batch.clear()
                if count % 500_000 == 0:
                    print(f"[receita]   {count:,} empresas…", flush=True)
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        count += len(batch)
    return count


def _import_estabelecimentos_files(conn: sqlite3.Connection, files: List[Path]) -> int:
    count = 0
    batch: list[tuple] = []
    sql = (
        "INSERT OR REPLACE INTO estabelecimentos "
        "(cnpj_basico, cnpj_ordem, cnpj_dv, identificador, nome_fantasia, "
        "situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, "
        "nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, "
        "cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento, "
        "bairro, cep, uf, municipio, ddd1, telefone1, ddd2, telefone2, ddd_fax, "
        "fax, correio_eletronico, situacao_especial, data_situacao_especial) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    for fp in files:
        for row in _iter_csv_rows(fp):
            if len(row) < 30:
                continue
            batch.append((
                _digits_only(row[0]).zfill(8),
                _digits_only(row[1]).zfill(4),
                _digits_only(row[2]).zfill(2),
                row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                row[10], row[11], row[12], row[13], row[14], row[15],
                row[16], row[17], row[18], row[19], row[20], row[21],
                row[22], row[23], row[24], row[25], row[26], row[27],
                row[28], row[29],
            ))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(sql, batch)
                conn.commit()
                count += len(batch)
                batch.clear()
                if count % 500_000 == 0:
                    print(f"[receita]   {count:,} estabelecimentos…", flush=True)
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        count += len(batch)
    return count


def _import_simples_files(conn: sqlite3.Connection, files: List[Path]) -> int:
    count = 0
    batch: list[tuple] = []
    sql = (
        "INSERT OR REPLACE INTO simples "
        "(cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples, "
        "opcao_mei, data_opcao_mei, data_exclusao_mei) VALUES (?,?,?,?,?,?,?)"
    )
    for fp in files:
        for row in _iter_csv_rows(fp):
            if len(row) < 4:
                continue
            # Pad to 7 columns (some files only have 4)
            padded = (row + [""] * 7)[:7]
            batch.append((
                _digits_only(padded[0]).zfill(8),
                padded[1], padded[2], padded[3],
                padded[4], padded[5], padded[6],
            ))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(sql, batch)
                conn.commit()
                count += len(batch)
                batch.clear()
                if count % 500_000 == 0:
                    print(f"[receita]   {count:,} simples…", flush=True)
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        count += len(batch)
    return count


def _setup_socios_staging(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS socios_new")
    conn.execute(
        "CREATE TABLE socios_new ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  cnpj_basico TEXT NOT NULL,"
        "  identificador_socio TEXT,"
        "  nome TEXT,"
        "  cpf_cnpj TEXT,"
        "  qualificacao TEXT,"
        "  data_entrada TEXT,"
        "  pais TEXT,"
        "  representante_cpf_cnpj TEXT,"
        "  representante_nome TEXT,"
        "  representante_qualificacao TEXT,"
        "  faixa_etaria TEXT"
        ")"
    )
    conn.commit()


def _finalize_socios_staging(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS socios")
    conn.execute("ALTER TABLE socios_new RENAME TO socios")
    conn.execute("CREATE INDEX idx_socios_basico ON socios(cnpj_basico)")
    conn.commit()


def _import_socios_files(conn: sqlite3.Connection, files: List[Path]) -> int:
    count = 0
    batch: list[tuple] = []
    sql = (
        "INSERT INTO socios_new "
        "(cnpj_basico, identificador_socio, nome, cpf_cnpj, qualificacao, "
        "data_entrada, pais, representante_cpf_cnpj, representante_nome, "
        "representante_qualificacao, faixa_etaria) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    )
    for fp in files:
        for row in _iter_csv_rows(fp):
            if len(row) < 11:
                continue
            batch.append((
                _digits_only(row[0]).zfill(8),
                row[1], row[2], row[3], row[4], row[5],
                row[6], row[7], row[8], row[9], row[10],
            ))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(sql, batch)
                conn.commit()
                count += len(batch)
                batch.clear()
                if count % 500_000 == 0:
                    print(f"[receita]   {count:,} socios…", flush=True)
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        count += len(batch)
    return count


# ---------------------------------------------------------------------------
# Download / extract
# ---------------------------------------------------------------------------


def _download_and_extract(
    session: requests.Session, url: str, target_dir: Path
) -> List[Path]:
    """Download a zip, extract CSVs, delete the zip. Returns extracted file paths."""
    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"[receita] Downloading {url}", flush=True)
    with session.get(url, stream=True, timeout=(30, None)) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length") or 0)
        downloaded = 0
        last_report = time.monotonic()
        next_report = 50 * 1024 * 1024
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            for chunk in resp.iter_content(1024 * 1024):
                if chunk:
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= next_report or (time.monotonic() - last_report) > 30:
                        if total:
                            pct = downloaded / total * 100
                            print(
                                f"[receita]   {downloaded / 1e6:.1f}MB"
                                f"/{total / 1e6:.1f}MB ({pct:.1f}%)",
                                flush=True,
                            )
                        else:
                            print(f"[receita]   {downloaded / 1e6:.1f}MB", flush=True)
                        next_report += 50 * 1024 * 1024
                        last_report = time.monotonic()
            tmp_path = Path(tmp.name)
    extracted: List[Path] = []
    try:
        with ZipFile(tmp_path) as archive:
            members = []
            for name in archive.namelist():
                norm = _normalize_member_filename(name)
                if norm:
                    members.append((name, norm))
            if not members:
                print(f"[receita]   No CSV in {url}", flush=True)
                return extracted
            zip_stem = Path(urlparse(url).path).stem
            for orig, norm in members:
                dest = target_dir / norm
                if dest.exists():
                    suffix = Path(norm).suffix or ".csv"
                    dest = target_dir / f"{zip_stem}{suffix}"
                with archive.open(orig) as src, dest.open("wb") as out:
                    shutil.copyfileobj(src, out)
                extracted.append(dest)
    finally:
        tmp_path.unlink(missing_ok=True)
    return extracted


# ---------------------------------------------------------------------------
# Batch resolution
# ---------------------------------------------------------------------------


def _resolve_latest_batch(
    session: requests.Session,
) -> Optional[tuple[str, dt.date, Dict[str, List[str]]]]:
    """Try current month then up to 5 months back. Returns None when no batch is available."""
    today = dt.date.today()
    candidate = today
    for _ in range(6):
        batch = f"{candidate.year:04d}-{candidate.month:02d}"
        base_url = _build_batch_url(settings.receita_base_url, batch)
        print(f"[receita] Trying batch {batch}", flush=True)
        try:
            links = _discover_zip_links(session, base_url)
        except requests.RequestException as exc:
            print(f"[receita] Error fetching {batch}: {exc}", flush=True)
            candidate = _previous_month(candidate)
            continue
        missing = [c for c in CATEGORIES if not links.get(c)]
        if missing:
            print(
                f"[receita] Batch {batch} missing: {', '.join(missing)}",
                flush=True,
            )
            candidate = _previous_month(candidate)
            continue
        return base_url, dt.date(candidate.year, candidate.month, 1), links
    return None


def _discover_zip_links(
    session: requests.Session, base_url: str
) -> Dict[str, List[str]]:
    share_info = _parse_share_link(base_url)
    if share_info:
        origin, token, dir_path = share_info
        session.auth = (token, "")
        zip_urls = _list_webdav_zip_urls(session, origin, dir_path)
    else:
        resp = session.get(base_url, timeout=120)
        resp.raise_for_status()
        zip_urls = [
            urljoin(base_url, href)
            for href in re.findall(
                r"href=[\"']([^\"']+\.zip)[\"']", resp.text, re.IGNORECASE
            )
        ]

    discovered: Dict[str, List[str]] = {c: [] for c in CATEGORIES}
    for url in zip_urls:
        filename = Path(urlparse(url).path).name
        tok = _normalize_token(filename)
        for cat in CATEGORIES:
            if _normalize_token(cat) in tok:
                if url not in discovered[cat]:
                    discovered[cat].append(url)
                break

    for urls in discovered.values():
        urls.sort(key=lambda u: Path(urlparse(u).path).name.lower())
    return discovered


def _parse_share_link(
    base_url: str,
) -> Optional[tuple[str, str, str]]:
    m = SHARE_URL_PATTERN.match(base_url.strip())
    if not m:
        return None
    origin, token = m.group("origin"), m.group("token")
    query = parse_qs(urlparse(base_url).query)
    dir_path = query.get("dir", query.get("path", ["/"]))[0]
    if not dir_path.startswith("/"):
        dir_path = f"/{dir_path}"
    return origin, token, dir_path


def _list_webdav_zip_urls(
    session: requests.Session, origin: str, dir_path: str
) -> List[str]:
    norm = "/" if dir_path == "/" else f"{dir_path.rstrip('/')}/"
    webdav_url = f"{origin}/public.php/webdav{quote(norm)}"
    resp = session.request(
        "PROPFIND", webdav_url, headers={"Depth": "1"}, timeout=120
    )
    resp.raise_for_status()
    root_el = ElementTree.fromstring(resp.content)
    ns = {"d": "DAV:"}
    urls: List[str] = []
    for item in root_el.findall("d:response", ns):
        href = item.findtext("d:href", default="", namespaces=ns)
        if not href:
            continue
        href_path = unquote(urlparse(href).path)
        if href_path.endswith("/"):
            continue
        if not Path(href_path).name.lower().endswith(".zip"):
            continue
        urls.append(urljoin(origin, href))
    return urls


# ---------------------------------------------------------------------------
# CSV utilities
# ---------------------------------------------------------------------------


def _iter_csv_rows(file_path: Path) -> Iterator[List[str]]:
    with file_path.open("r", encoding="latin-1", newline="") as f:
        reader = csv.reader(f, delimiter=";", quotechar='"')
        for row in reader:
            if row:
                yield [col.strip() for col in row]


def _normalize_member_filename(member: str) -> Optional[str]:
    name = Path(member).name.strip()
    if not name:
        return None
    return name if name.lower().endswith(".csv") else f"{name}.csv"


# ---------------------------------------------------------------------------
# URL / string utilities
# ---------------------------------------------------------------------------


def _build_batch_url(template: str, batch: str) -> str:
    t = template.strip()
    if "{batch}" in t:
        return t.format(batch=batch)
    parsed = urlparse(t)
    if parsed.query:
        query = parse_qs(parsed.query)
        key = "dir" if "dir" in query else "path"
        val = query.get(key, ["/"])[0]
        current = val if val.endswith("/") else f"{val}/"
        query[key] = [f"{current}{batch}"]
        return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
    return f"{t.rstrip('/')}/{batch}/"


def _normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _digits_only(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _previous_month(d: dt.date) -> dt.date:
    return d.replace(day=1) - dt.timedelta(days=1)


def _cleanup_stale_runs(conn: sqlite3.Connection) -> None:
    cutoff = (
        dt.datetime.now() - dt.timedelta(minutes=MAX_RUNNING_MINUTES)
    ).isoformat()
    conn.execute(
        "UPDATE import_runs SET status = 'failure', notes = 'Stale run cleanup' "
        "WHERE status = 'running' AND started_at < ?",
        (cutoff,),
    )
    conn.commit()
