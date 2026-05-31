FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements/ /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/production.txt

# Copy application
COPY backend/ /app/

# Create non-root user
RUN useradd -m -u 1000 wit && chown -R wit:wit /app

# Collect static files (will fail first time, that's ok)
RUN python manage.py collectstatic --noinput || true

USER wit

EXPOSE 8000

CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4"]
