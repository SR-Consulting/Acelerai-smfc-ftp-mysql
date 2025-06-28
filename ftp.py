"""
SFTP.py
▶ Conecta-se ao servidor SFTP do Marketing Cloud
▶ Lista a pasta raiz e a subpasta “Import”
▶ Faz download do arquivo Tb_Sent3meses_20250623.csv

Autor: Vinícius
Atualizado: 24/06/2025
"""

import os
import sys
import stat
import logging
from pathlib import Path
import paramiko


# -------------------------------------------------
# Configuração de logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("sftp.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# Credenciais (use variáveis de ambiente em produção)
# -------------------------------------------------
SFTP_HOST = "mc-yv8y8vj7n37jkr0llbdmdcplm.ftp.marketingcloudops.com"
SFTP_PORT = 22
SFTP_USER = "546005055_3"
SFTP_PASS = os.getenv("SFTP_PASS", "sr@@consulting123")


# -------------------------------------------------
# Funções utilitárias
# -------------------------------------------------
def _fmt_size(item):
    """Retorna string formatada para tamanho ou <DIR>."""
    if item.st_size is None or stat.S_ISDIR(item.st_mode):
        return "<DIR>".ljust(12)
    return f"{item.st_size / 1024:10.1f} kB"


def list_dir(sftp, path="."):
    """Lista itens de `path` com tamanho."""
    logger.info(f"Listando conteúdo de {path!r}:")
    for item in sftp.listdir_attr(path):
        logger.info(f"{item.filename:40}  {_fmt_size(item)}")


# ------- barra de progresso simples (sem dependências externas) ---------
def _progress(transferred: int, total: int):
    percent = transferred / total * 100
    bar_len = 40
    filled = int(bar_len * percent / 100)
    bar = "█" * filled + "-" * (bar_len - filled)
    sys.stdout.write(f"\r[{bar}] {percent:6.2f}%")
    sys.stdout.flush()
    if transferred == total:  # terminou
        sys.stdout.write("\n")


def download(sftp, remote_path: str, local_path: Path):
    """
    Faz download de `remote_path` → `local_path`, exibindo progresso.
    Cria pasta alvo e remove arquivo existente se necessário.
    """
    logger.info(f"↓ {remote_path} → {local_path}")
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if local_path.exists():
        try:
            local_path.unlink()
        except PermissionError as e:
            logger.error(
                "Permissão negada: o arquivo parece aberto em outro programa.\n"
                "Feche-o e execute novamente."
            )
            raise e

    # callback=_progress exibe barra
    sftp.get(remote_path, str(local_path), callback=_progress)
    logger.info("Download concluído.")


# -------------------------------------------------
# Processo principal
# -------------------------------------------------
def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    logger.info("Conectando ao servidor SFTP…")
    ssh.connect(
        hostname=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USER,
        password=SFTP_PASS,
        timeout=10,
    )

    sftp = ssh.open_sftp()
    logger.info("Conexão estabelecida.")

    try:
        # 1) Listagens básicas
        list_dir(sftp, ".")
        list_dir(sftp, "Import")

        # 2) Download do arquivo solicitado com barra de progresso
        REMOTE_FILE = "Import/Tb_Sent3meses_20250623.csv"
        SCRIPT_DIR = Path(__file__).resolve().parent
        LOCAL_FILE = SCRIPT_DIR / "dataExtensionItens.csv"
        download(sftp, REMOTE_FILE, LOCAL_FILE)

    finally:
        sftp.close()
        ssh.close()
        logger.info("Sessão encerrada com sucesso.")


if __name__ == "__main__":
    main()
