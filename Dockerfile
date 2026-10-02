# syntax=docker/dockerfile:1

# One image runs all three processes; the command selects which one:
#   api (default)  webhooks-api
#   worker         webhooks-worker
#   receiver       webhooks-receiver
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependency layer: only what the build backend needs, so editing migrations,
# scripts or docs does not invalidate the installed package.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Runtime assets: Alembic configuration and migrations, plus the verification
# scripts executed by the one-shot `verify` Compose service.
COPY alembic.ini ./
COPY migrations ./migrations
COPY scripts ./scripts

# Non-root runtime user.
RUN groupadd --gid 1001 app \
    && useradd --uid 1001 --gid app --create-home --shell /usr/sbin/nologin app \
    && chown -R app:app /app
USER app

# API listens on API_PORT (default 8000); the reference receiver defaults to 9000.
EXPOSE 8000 9000

CMD ["webhooks-api"]
