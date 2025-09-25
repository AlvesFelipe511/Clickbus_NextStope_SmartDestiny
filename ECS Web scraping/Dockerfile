# ===== Base com Python + Playwright =====
FROM mcr.microsoft.com/playwright/python:v1.46.0-jammy

# Boa prática p/ logs e performance
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ---- Dependências Python ----
COPY requirements.txt ./
# Garanta que requirements.txt contenha boto3>=1.34
# Ex.: echo "boto3>=1.34" >> requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ---- Código do app (scrapers + notificador) ----
COPY . ./

# Variáveis padrão (podem ser sobrescritas na Task Definition do ECS)
ENV AWS_REGION=sa-east-1 \
    S3_BUCKET=cbchallenge \
    MARKER_PREFIX="Bronze/_markers/"

# Rode os 3 scrapers em sequência; se tudo ok, cria o .done
# 'set -euo pipefail' garante que, se algum script falhar, o contêiner finaliza com erro
CMD ["bash", "-lc", "set -euo pipefail; \
  python sympla_scraper.py; \
  python ticket360_scraper.py; \
  python eventim_scraper.py; \
  python notify_done_s3.py"]
