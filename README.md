# Acelerai - SFMC FTP → MySQL

Automação para carga de arquivos CSV do SFTP do Marketing Cloud diretamente para uma base MySQL, com suporte a múltiplos arquivos e criação dinâmica de colunas.

---

## 📂 Visão Geral

Este projeto realiza:

- Conexão via SFTP ao servidor do Marketing Cloud
- Download dos arquivos com padrão `_Daily` da pasta `Import`
- Leitura dos arquivos `.csv` (UTF-16)
- Inserção dos dados em lote na tabela MySQL `sfmc_data_extension_item`
- Criação dinâmica de colunas conforme o cabeçalho do CSV
- Registro de falhas em arquivos `.json` em pasta de quarentena

---

## 📌 Pré-requisitos

- Python 3.10 ou superior
- MySQL Server (acessível remotamente)
- Acesso SFTP ao servidor do Marketing Cloud
- Variável de ambiente `SFTP_PASS` com a senha

---

## 📦 Instalação

1. Clone o repositório:

```bash
git clone https://github.com/SR-Consulting/Acelerai-smfc-ftp-mysql.git
cd Acelerai-smfc-ftp-mysql
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```


---


## ▶️ Execução

Execute o script principal:

```bash
python main.py
```

---

## 🗂️ Estrutura esperada

```
📁 downloads/
 ┣━ 📄*_Daily*.csv         ← arquivos baixados temporariamente
 ┣━ 📁_falhas/              ← falhas de lote (jsons exportados)
📄 main.py
📄 .gitignore
📄 README.md
```

---

## 🧾 Logs

Os logs são gerados automaticamente com timestamp:

```text
main_20250628_143000.log
```

---

## 📌 Observações

- O encoding dos CSVs deve ser **UTF-16**.
- A tabela no MySQL será atualizada dinamicamente com novas colunas quando detectadas.
- Dados inválidos são exportados para `downloads/_falhas` em formato `.json`.

---
