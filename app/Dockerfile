# One image for the producer, the consumer and the dashboard (different commands in docker-compose.yml).
FROM python:3.12-slim

# PYTHONPATH: `streamlit run dashboard/app.py` only puts dashboard/ on the import path,
# so /app must be added for `import fmi_pipeline` to work in the dashboard.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Non-root user with uid 1000, the same as the default "ubuntu" user on the VM, so the
# service-account key (chmod 600, owned by ubuntu) can be read through the bind mount.
RUN useradd --uid 1000 --create-home app \
    && mkdir -p /state \
    && chown app:app /state

COPY fmi_pipeline ./fmi_pipeline
COPY dashboard ./dashboard

USER app
