"""
Carga de CSVs Diários (“*_Daily*.csv”)
Vinícius – 25 jun 2025
"""

import csv, logging, traceback, json
from pathlib import Path
from itertools import islice
from datetime import datetime
import mysql.connector
from mysql.connector import pooling, errors

# ---------- logging ----------
log_file = Path(f"csv2mysql_{datetime.now():%Y%m%d_%H%M%S}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------- MySQL ----------
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

# ---------- config ----------
CSV_DIR = Path(r"C:\Users\vinicius\Documents\Work\Sr Consulting\Acelerai\csv")
PATTERN = "*_Daily*.csv" 
TABLE_NAME = "sfmc_data_extension_item"
BATCH_SIZE = 25_000
COMMIT_INT = 50_000
CSV_ENCODING = "utf-16"
QUAR_DIR = CSV_DIR / "_falhas"
QUAR_DIR.mkdir(exist_ok=True)


# ---------- utilidades ----------
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
    pool_args = {k: MYSQL_CONFIG[k] for k in pool_keys if k in MYSQL_CONFIG}
    conn_args = {k: v for k, v in MYSQL_CONFIG.items() if k not in pool_keys}
    return pooling.MySQLConnectionPool(**pool_args, **conn_args)


def dedup_case_insensitive(seq):
    """Remove duplicatas ignorando maiúsc/minúsc e espaços; preserva a 1ª ocorrência."""
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
                if e.errno == 1060:  # coluna já existe
                    log.debug("Coluna %s já existe – ignorando.", col)
                else:
                    raise
        conn.commit()
    finally:
        cur.close()


# ---------- carga de um único arquivo ----------
def load_csv(csv_path: Path, pool):
    customer_key_const = csv_path.stem
    log.info(
        "Iniciando carga de %s (CustomerKey=%s)", csv_path.name, customer_key_const
    )

    conn = pool.get_connection()
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
                    "Lote %d: %d linhas (acumulado: %s)",
                    batch_no,
                    len(dados),
                    f"{total:,}",
                )

                if pending >= COMMIT_INT:
                    conn.commit()
                    log.info("COMMIT parcial (%s linhas).", f"{total:,}")
                    pending = 0

            conn.commit()
            log.info("COMMIT final (%s linhas totais).", f"{total:,}")

    except Exception:
        conn.rollback()
        log.exception("Falha – rollback total do arquivo.")
        raise
    finally:
        cur.close()
        conn.close()
        log.info("Conexão encerrada para %s.", csv_path.name)


# ---------- principal ----------
def main():
    pool = get_pool()
    arquivos = sorted(CSV_DIR.glob(PATTERN))
    if not arquivos:
        log.warning("Nenhum arquivo %s encontrado em %s.", PATTERN, CSV_DIR)
        return
    for csv_file in arquivos:
        try:
            load_csv(csv_file, pool)
        except Exception:
            log.error("Erro ao processar %s – seguindo para o próximo.", csv_file.name)


if __name__ == "__main__":
    main()
