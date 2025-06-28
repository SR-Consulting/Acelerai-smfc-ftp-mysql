"""
Carga de CSVs Diários direto do FTP ("*_Historical*.csv")
▶ Conecta‑se ao SFTP do Marketing Cloud
▶ Faz download temporário dos CSVs que correspondem ao padrão em /Import
▶ Insere dados na tabela MySQL sfmc_data_extension_item (criando colunas se necessário)

Autor: Vinícius
Atualizado: 28/06/2025
"""

import os
import sys
import stat
import csv
import json
import fnmatch
import logging
import traceback
from pathlib import Path
from itertools import islice
from datetime import datetime

import paramiko
import mysql.connector
from mysql.connector import pooling, errors

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
log_file = Path(f"main_{datetime.now():%Y%m%d_%H%M%S}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# -------------------------------------------------
# SFTP – use variáveis de ambiente em produção
# -------------------------------------------------
SFTP_HOST = "mc-yv8y8vj7n37jkr0llbdmdcplm.ftp.marketingcloudops.com"
SFTP_PORT = 22
SFTP_USER = "546005055_3"
SFTP_PASS = os.getenv("SFTP_PASS", "sr@@consulting123")
SFTP_DIR = "Import"  # pasta remota onde ficam os CSVs
PATTERN = "*_Historical*.csv"  # padrão procurado no FTP

# -------------------------------------------------
# MySQL
# -------------------------------------------------
MYSQL_CONFIG = {
    "host": "db-acelerai.cpzj1r8xyy4q.us-west-2.rds.amazonaws.com",
    "user": "marcos.torres@acelerai.com.br",
    "password": "Marcosacelerai2025!",
    "database": "dbacelerai",
    "port": 3306,
    "connection_timeout": 15,
    "pool_name": "sfmc_pool",
    "pool_size": 5,
}
TABLE_NAME = "sfmc_data_extension_item"

# -------------------------------------------------
# Diretórios locais
# -------------------------------------------------
TMP_DIR = Path(__file__).resolve().parent / "downloads"
TMP_DIR.mkdir(exist_ok=True)
QUAR_DIR = TMP_DIR / "_falhas"
QUAR_DIR.mkdir(exist_ok=True)

CSV_ENCODING = "utf-16"
BATCH_SIZE = 25_000
COMMIT_INT = 50_000

# -------------------------------------------------
# SFTP utilitários
# -------------------------------------------------


def _fmt_size(item):
    """Retorna string formatada para tamanho ou <DIR>."""
    if stat.S_ISDIR(item.st_mode):
        return "<DIR>".ljust(12)
    return f"{item.st_size / 1024:10.1f} kB"


def list_dir(sftp, path="."):
    log.info(f"Listando conteúdo de '{path}':")
    for item in sftp.listdir_attr(path):
        log.info(f"{item.filename:40}  {_fmt_size(item)}")


def _progress(transferred: int, total: int):
    percent = transferred / total * 100
    bar_len = 40
    filled = int(bar_len * percent / 100)
    bar = "█" * filled + "-" * (bar_len - filled)
    sys.stdout.write(f"\r[{bar}] {percent:6.2f}%")
    sys.stdout.flush()
    if transferred == total:
        sys.stdout.write("\n")


def download_file(sftp, remote_path: str, local_path: Path):
    """Baixa arquivo remoto exibindo progresso."""
    log.info("↓ %s → %s", remote_path, local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        try:
            local_path.unlink()
        except PermissionError as e:
            log.error("Arquivo aberto em outro programa – feche‑o e execute novamente.")
            raise e
    sftp.get(remote_path, str(local_path), callback=_progress)


# -------------------------------------------------
# Funções genéricas (mesmas do script anterior)
# -------------------------------------------------


def chunked(it, size):
    it = iter(it)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


def build_insert(cols):
    ph = ", ".join(["%s"] * len(cols))
    return f"INSERT INTO {TABLE_NAME} ({', '.join(cols)}) VALUES ({ph})"


def get_pool():
    pool_keys = ("pool_name", "pool_size")
    pool_args = {k: MYSQL_CONFIG[k] for k in pool_keys}
    conn_args = {k: v for k, v in MYSQL_CONFIG.items() if k not in pool_keys}
    return pooling.MySQLConnectionPool(**pool_args, **conn_args)


def dedup_case_insensitive(seq):
    seen, unique, pos_map = set(), [], []
    for idx, col in enumerate(seq):
        norm = col.strip().casefold()
        if norm not in seen:
            seen.add(norm)
            unique.append(col.strip())
            pos_map.append(idx)
    return unique, pos_map


def check_and_create_columns(conn, cols):
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM {TABLE_NAME}")
        existentes = {row[0].casefold() for row in cur.fetchall()}
        for col in cols:
            if col.casefold() in existentes:
                continue
            try:
                cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN `{col}` VARCHAR(255)")
                log.info("Coluna %s criada.", col)
            except errors.ProgrammingError as e:
                if e.errno == 1060:
                    log.debug("Coluna %s já existe – ignorando.", col)
                else:
                    raise
        conn.commit()
    finally:
        cur.close()


# -------------------------------------------------
# Carga de um único CSV
# -------------------------------------------------


def load_csv(csv_path: Path, pool):
    customer_key_const = csv_path.stem
    log.info(
        "Iniciando carga de %s (CustomerKey=%s)", csv_path.name, customer_key_const
    )
    conn = pool.get_connection()
    cur = None
    try:
        with csv_path.open(newline="", encoding=CSV_ENCODING) as f:
            rdr = csv.reader(f)
            raw_header = next(rdr)
            header, idx_map = dedup_case_insensitive(raw_header)
            cols = header + ["CustomerKey"]
            check_and_create_columns(conn, cols)
            sql = build_insert(cols)
            total = pending = 0
            cur = conn.cursor()
            for batch_no, rows in enumerate(chunked(rdr, BATCH_SIZE), start=1):
                dados = [
                    tuple([row[i] for i in idx_map] + [customer_key_const])
                    for row in rows
                    if len(row) >= len(raw_header)
                ]
                if not dados:
                    continue
                try:
                    cur.executemany(sql, dados)
                except Exception as e:
                    log.error(
                        "Falha no lote %d (%d linhas) do arquivo %s",
                        batch_no,
                        len(dados),
                        csv_path.name,
                    )
                    log.error("Tipo: %s – %s", type(e).__name__, e)
                    log.debug("Traceback:\n%s", traceback.format_exc())
                    quar_file = QUAR_DIR / f"{customer_key_const}_batch{batch_no}.json"
                    with quar_file.open("w", encoding="utf-8") as qf:
                        json.dump(
                            {"header": cols, "rows": dados[:50]}, qf, ensure_ascii=False
                        )
                    log.warning("Lote %d exportado para %s", batch_no, quar_file)
                    conn.rollback()
                    raise
                total += len(dados)
                pending += len(dados)
                log.info(
                    "Lote %d: %d linhas (acum.: %s)", batch_no, len(dados), f"{total:,}"
                )
                if pending >= COMMIT_INT:
                    conn.commit()
                    log.info("COMMIT parcial (%s linhas).", f"{total:,}")
                    pending = 0
            conn.commit()
            log.info("COMMIT final (%s linhas).", f"{total:,}")
    except Exception:
        if conn.is_connected():
            conn.rollback()
        log.exception("Falha – rollback total do arquivo.")
        raise
    finally:
        if cur:
            cur.close()
        conn.close()
        log.info("Conexão encerrada para %s.", csv_path.name)


# -------------------------------------------------
# MAIN
# -------------------------------------------------


def main():
    pool = get_pool()

    # 1) Conectar ao SFTP
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    log.info("Conectando ao SFTP…")
    ssh.connect(
        hostname=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USER,
        password=SFTP_PASS,
        timeout=10,
    )
    sftp = ssh.open_sftp()
    log.info("Conexão estabelecida.")

    try:
        # 2) Localizar arquivos ‑Historical no FTP
        remote_items = [
            it
            for it in sftp.listdir_attr(SFTP_DIR)
            if fnmatch.fnmatch(it.filename, PATTERN)
        ]
        if not remote_items:
            log.warning("Nenhum arquivo %s encontrado em %s.", PATTERN, SFTP_DIR)
            return

        for item in remote_items:
            remote_path = f"{SFTP_DIR}/{item.filename}"
            local_path = TMP_DIR / item.filename
            download_file(sftp, remote_path, local_path)
            try:
                load_csv(local_path, pool)
                # Opcional: mover ou excluir o arquivo remoto após sucesso
                # sftp.remove(remote_path)
            finally:
                try:
                    local_path.unlink()  # remove tmp
                except Exception:
                    pass
    finally:
        sftp.close()
        ssh.close()
        log.info("Sessão SFTP encerrada.")


if __name__ == "__main__":
    main()
