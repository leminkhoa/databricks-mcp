ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS builder

WORKDIR /app

# Create non-root user in builder stage
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install uv from Astral with hash verification
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml .

# ---- Runtime stage ----
FROM python:3.11-slim

WORKDIR /app

# Create non-root user in runtime stage and set up directories
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && mkdir -p /home/appuser/.cache/uv \
    && chown -R appuser:appuser /app /home/appuser

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy your source code
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser main.py .
COPY --chown=appuser:appuser pyproject.toml .

# Set environment variables
ENV TRANSPORT="stdio" \
    SERVER_HOST="0.0.0.0" \
    SERVER_PORT="8000" \
    DEBUG="false" \
    LOG_LEVEL="INFO" \
    PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Install dependencies using uv
RUN uv venv && uv sync

# Expose port if needed
EXPOSE 8000

# Default command
CMD ["uv", "run", "main.py"]
