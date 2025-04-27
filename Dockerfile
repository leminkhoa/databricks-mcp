FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv from Astral
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv


# Copy dependency files
COPY pyproject.toml .


# ---- Runtime stage ----
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy your source code
COPY src/ ./src/
COPY main.py .
COPY pyproject.toml .

# Set environment variables (replace with your actual values or set at runtime)
ENV TRANSPORT="stdio"
ENV SERVER_HOST="0.0.0.0"
ENV SERVER_PORT="8000"
ENV DEBUG="false"
ENV LOG_LEVEL="INFO"
ENV PYTHONUNBUFFERED=1

# Expose port if needed
EXPOSE 8000

# Install dependencies using uv
RUN uv venv
RUN uv sync


# Default command (adjust as needed)
CMD ["uv", "run", "main.py"]
