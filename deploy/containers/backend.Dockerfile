# One image per backend service (Section H, Step 5.6).
#
#   docker build -f deploy/containers/backend.Dockerfile --build-arg SERVICE=bot-backend -t cosmicforge/bot-backend:<rev> .
#
# SERVICE is bot-backend, user-backend or admin-backend. The image holds code and pinned dependencies only:
# no .env, no database, no credentials (secrets arrive at run time; see deploy/staging/README.md). It runs as
# an unprivileged user and writes only to /var/lib/cosmicforge (a volume).
FROM python:3.12-slim AS base
ARG SERVICE
ARG BUILD_REVISION=unknown
ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1 \
    BUILD_VERSION=${BUILD_REVISION} SERVICE=${SERVICE}
# libgomp1: lightgbm's OpenMP runtime (bot-backend); curl: container health checks
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl && rm -rf /var/lib/apt/lists/*
WORKDIR /srv/cosmicforge
COPY deploy/constraints.txt deploy/constraints.txt
COPY backends/shared backends/shared
COPY backends/bot-backend/requirements.txt backends/bot-backend/requirements.txt
COPY backends/user-backend/requirements.txt backends/user-backend/requirements.txt
RUN test -n "$SERVICE" && pip install -e backends/shared \
 && if [ "$SERVICE" = "bot-backend" ]; then pip install -r backends/bot-backend/requirements.txt -c deploy/constraints.txt; \
    elif [ "$SERVICE" = "user-backend" ]; then pip install -r backends/user-backend/requirements.txt -c deploy/constraints.txt; \
    else pip install -r backends/user-backend/requirements.txt -r backends/bot-backend/requirements.txt -c deploy/constraints.txt; fi
COPY backends/${SERVICE} backends/${SERVICE}
COPY scripts scripts
# The family registry verifies the frozen mandate against these files at run time; they ship byte-identical.
COPY research research
COPY docs/research docs/research
RUN useradd --system --uid 10001 --home /var/lib/cosmicforge cosmicforge \
 && mkdir -p /var/lib/cosmicforge && chown cosmicforge /var/lib/cosmicforge \
 && find backends -name ".env" -delete && find backends -name "*.db*" -delete \
 && find backends -name "*firebase-adminsdk*.json" -delete
USER cosmicforge
WORKDIR /srv/cosmicforge/backends/${SERVICE}
ENV PYTHONPATH=/srv/cosmicforge/backends/shared:/srv/cosmicforge/backends/${SERVICE}
EXPOSE 8000
# The health check is the liveness probe; readiness (/health?ready=1) is asked by the deploy script.
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=4 CMD curl -fsS http://127.0.0.1:8000/health || exit 1
CMD ["python", "-u", "-X", "utf8", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
