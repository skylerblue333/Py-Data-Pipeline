FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --system sky && useradd --system --gid sky --home-dir /app sky
WORKDIR /app
COPY pyproject.toml README.md pipeline.py ./
RUN python -m pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .
RUN mkdir -p /data && chown -R sky:sky /app /data
USER sky
VOLUME ["/data"]
CMD ["python", "pipeline.py"]
