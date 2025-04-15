# Stage 1: Builder
FROM python:3.12-slim AS builder
WORKDIR /app

# Install build tools and curl needed to install dependencies
RUN apt-get update && \
    apt-get install -y curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and build virtual environment with dependencies
COPY requirements.txt .
RUN python -m venv /venv && \
    /venv/bin/pip install --upgrade pip && \
    /venv/bin/pip install --no-cache-dir -r requirements.txt

# Copy the complete source code
COPY . .

# Stage 2: Production
FROM python:3.12-slim
WORKDIR /app

# Create a non-root user for better security
RUN adduser --disabled-password --gecos "" appuser

# Copy virtual environment and application source from builder
COPY --from=builder /venv /venv
COPY --from=builder /app /app

# Ensure the virtual environment's executables are used
ENV PATH="/venv/bin:$PATH"

# Switch to non-root user
USER appuser

# Default command to run the application
CMD ["python", "-u", "watchdog.py"]