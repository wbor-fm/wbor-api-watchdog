# Stage 1: Builder
FROM python:3.13-slim AS builder
WORKDIR /app

# Install build tools and curl needed to install uv
RUN apt-get update && \
    apt-get install -y curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy dependencies files and create virtual environment with dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Copy the complete source code
COPY . .

# Stage 2: Production
FROM python:3.13-slim
WORKDIR /app

# Create a non-root user for better security
RUN adduser --disabled-password --gecos "" appuser

# Copy virtual environment and application source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app /app

# Ensure the virtual environment's executables are used
ENV PATH="/app/.venv/bin:$PATH"

# Install pgrep for healthcheck
RUN apt-get update \
    && apt-get install -y procps \
    && rm -rf /var/lib/apt/lists/*

# Switch to non-root user
USER appuser

# healthcheck: make sure the watchdog.py process is alive
# - check every 30 seconds after a 5-second start period
# - timeout is 10 seconds, meaning if the process does not respond 
#   within that time, it is considered unhealthy (after 3 retries)
# - pgrep is used to check if the watchdog.py process is in the process 
#   list. 
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f watchdog.py || exit 1

# Default command to run the application
CMD ["python", "-u", "watchdog.py"]
