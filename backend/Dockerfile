ARG PYTHON_VERSION=3.13.5
FROM python:${PYTHON_VERSION}-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install uv with the correct path
RUN apt-get update && apt-get install -y curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo "Verifying uv installation:" && \
    ls -la /root/.local/bin/

# Set PATH environment variable to include uv
ENV PATH="/root/.local/bin:${PATH}"

# Verify uv is available in this layer
RUN which uv && uv --version

COPY pyproject.toml .
COPY uv.lock .

# Install dependencies
RUN uv sync --frozen

COPY . .
EXPOSE 5000

CMD ["uv", "run", "flask", "--app", "main", "run", "--host=0.0.0.0"]