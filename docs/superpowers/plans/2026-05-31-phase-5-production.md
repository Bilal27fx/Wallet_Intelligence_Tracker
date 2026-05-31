# Phase 5: Production Deployment - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prepare production-ready Docker setup and deploy to VPS.

**Architecture:** docker-compose.prod.yml with optimized settings, Nginx reverse proxy, health checks, monitoring, and deployment to VPS.

**Tech Stack:** Docker Compose (prod), Nginx, systemd (VPS)

---

## Prerequisites

✅ Phases 1-4 completed (full stack working in dev)
✅ VPS accessible (root@46.224.0.146)
✅ All tests passing
✅ Frontend connected to API

---

## File Structure

**New files to create:**
```
docker-compose.prod.yml
docker/nginx/nginx.conf
docker/backend/entrypoint.prod.sh
.env.production.example
docs/deployment.md
```

---

## Task 1: Create Production Docker Compose

**Files:**
- Create: `docker-compose.prod.yml`

- [ ] **Step 1: Write production docker-compose**

Create `docker-compose.prod.yml`:
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    restart: always
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data_prod:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - wit_network

  redis:
    image: redis:7-alpine
    restart: always
    command: redis-server --appendonly yes
    volumes:
      - redis_data_prod:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 3
    networks:
      - wit_network

  backend:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    restart: always
    command: gunicorn config.wsgi:application --bind 0.0.0.0:8000 --workers 4 --timeout 120
    volumes:
      - static_volume:/app/staticfiles
      - media_volume:/app/media
    env_file:
      - .env.production
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      - DJANGO_SETTINGS_MODULE=config.settings.production
    networks:
      - wit_network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/admin/"]
      interval: 30s
      timeout: 10s
      retries: 3

  worker:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    restart: always
    command: celery -A config worker --loglevel=info --concurrency=4
    env_file:
      - .env.production
    depends_on:
      - postgres
      - redis
      - backend
    environment:
      - DJANGO_SETTINGS_MODULE=config.settings.production
    networks:
      - wit_network

  beat:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    restart: always
    command: celery -A config beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
    env_file:
      - .env.production
    depends_on:
      - postgres
      - redis
      - backend
    environment:
      - DJANGO_SETTINGS_MODULE=config.settings.production
    networks:
      - wit_network

  nginx:
    image: nginx:alpine
    restart: always
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./docker/nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - static_volume:/app/staticfiles:ro
      - media_volume:/app/media:ro
    depends_on:
      - backend
    networks:
      - wit_network

volumes:
  postgres_data_prod:
  redis_data_prod:
  static_volume:
  media_volume:

networks:
  wit_network:
    driver: bridge
```

- [ ] **Step 2: Commit**

```bash
git add docker-compose.prod.yml
git commit -m "feat(deployment): create production docker-compose"
```

---

## Task 2: Create Nginx Configuration

**Files:**
- Create: `docker/nginx/nginx.conf`

- [ ] **Step 1: Create nginx directory**

```bash
mkdir -p docker/nginx
```

- [ ] **Step 2: Write Nginx config**

Create `docker/nginx/nginx.conf`:
```nginx
events {
    worker_connections 1024;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    upstream backend {
        server backend:8000;
    }

    server {
        listen 80;
        server_name _;

        client_max_body_size 100M;

        # Static files
        location /static/ {
            alias /app/staticfiles/;
            expires 30d;
            add_header Cache-Control "public, immutable";
        }

        # Media files
        location /media/ {
            alias /app/media/;
            expires 30d;
        }

        # API and admin
        location / {
            proxy_pass http://backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Timeouts
            proxy_connect_timeout 120s;
            proxy_send_timeout 120s;
            proxy_read_timeout 120s;
        }

        # Health check
        location /health/ {
            access_log off;
            return 200 "healthy\n";
            add_header Content-Type text/plain;
        }
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add docker/nginx/
git commit -m "feat(deployment): create Nginx configuration"
```

---

## Task 3: Create Production Environment File

**Files:**
- Create: `.env.production.example`

- [ ] **Step 1: Write production env example**

Create `.env.production.example`:
```bash
# Django
DJANGO_SECRET_KEY=CHANGE_THIS_TO_RANDOM_STRING_50_CHARS
DJANGO_SETTINGS_MODULE=config.settings.production
ALLOWED_HOSTS=your-domain.com,46.224.0.146
DEBUG=False

# Database
POSTGRES_DB=wit_database
POSTGRES_USER=wit_user
POSTGRES_PASSWORD=CHANGE_THIS_TO_SECURE_PASSWORD
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# Redis
REDIS_URL=redis://redis:6379/0

# External APIs (copy from current .env)
ZERION_API_KEY=
ZERION_API_KEY_2=
DUNE_API_KEY=
ETHERSCAN_API_KEY=
ALCHEMY_API_KEY=
CG_API_KEY=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHANNEL_ID=
```

- [ ] **Step 2: Commit**

```bash
git add .env.production.example
git commit -m "feat(deployment): create production environment example"
```

---

## Task 4: Create Production Entrypoint Script

**Files:**
- Create: `docker/backend/entrypoint.prod.sh`
- Modify: `docker/backend/Dockerfile`

- [ ] **Step 1: Write entrypoint script**

Create `docker/backend/entrypoint.prod.sh`:
```bash
#!/bin/bash
set -e

echo "Waiting for postgres..."
while ! nc -z $POSTGRES_HOST 5432; do
  sleep 0.1
done
echo "PostgreSQL started"

echo "Running migrations..."
python manage.py migrate --noinput

echo "Collecting static files..."
python manage.py collectstatic --noinput --clear

echo "Creating superuser if not exists..."
python manage.py shell << EOF
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@wit.com', '${DJANGO_ADMIN_PASSWORD:-changeme}')
    print("Superuser created")
else:
    print("Superuser already exists")
EOF

exec "$@"
```

- [ ] **Step 2: Make script executable**

```bash
chmod +x docker/backend/entrypoint.prod.sh
```

- [ ] **Step 3: Update Dockerfile to use entrypoint**

Modify `docker/backend/Dockerfile`, add before CMD:
```dockerfile
# Copy entrypoint
COPY docker/backend/entrypoint.prod.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
```

- [ ] **Step 4: Commit**

```bash
git add docker/backend/
git commit -m "feat(deployment): create production entrypoint script"
```

---

## Task 5: Create Deployment Documentation

**Files:**
- Create: `docs/deployment.md`

- [ ] **Step 1: Write deployment guide**

Create `docs/deployment.md`:
```markdown
# WIT V1 - Production Deployment Guide

## Prerequisites

- VPS with Docker and docker-compose installed
- Domain name (optional) or use IP
- SSH access to VPS

## Deployment Steps

### 1. Prepare VPS

```bash
# SSH to VPS
ssh root@46.224.0.146

# Install Docker if not already
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Install docker-compose
apt-get update
apt-get install docker-compose-plugin
```

### 2. Clone Repository

```bash
cd /opt
git clone <your-repo-url> wit-v1
cd wit-v1
git checkout feature/django-migration
```

### 3. Configure Environment

```bash
# Copy and edit production env
cp .env.production.example .env.production
nano .env.production

# Set:
# - DJANGO_SECRET_KEY (generate with: openssl rand -base64 50)
# - POSTGRES_PASSWORD (secure password)
# - ALLOWED_HOSTS (your domain or IP)
# - API keys from old .env
```

### 4. Build and Start

```bash
# Build images
docker-compose -f docker-compose.prod.yml build

# Start services
docker-compose -f docker-compose.prod.yml up -d

# Check logs
docker-compose -f docker-compose.prod.yml logs -f
```

### 5. Verify Deployment

```bash
# Check services
docker-compose -f docker-compose.prod.yml ps

# Test API
curl http://localhost/api/v1/wallets/

# Test admin
# Open: http://YOUR_IP/admin/
```

### 6. Setup systemd Service (Auto-restart)

```bash
# Create systemd service
cat > /etc/systemd/system/wit.service << EOF
[Unit]
Description=WIT V1 Django Application
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/wit-v1
ExecStart=/usr/bin/docker-compose -f docker-compose.prod.yml up -d
ExecStop=/usr/bin/docker-compose -f docker-compose.prod.yml down
StandardOutput=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
systemctl daemon-reload
systemctl enable wit.service
systemctl start wit.service
```

### 7. Migrate Data from Old System

```bash
# Copy old SQLite database to VPS
scp data/db/wit_database.db root@46.224.0.146:/opt/wit-v1/data/db/

# Run migration
docker-compose -f docker-compose.prod.yml exec backend python manage.py migrate_from_sqlite --sqlite-path /app/data/db/wit_database.db
```

### 8. Stop Old System

```bash
# Stop old scheduler
pkill -f run_pipelines.py

# Or stop systemd service if exists
systemctl stop wit_scheduler.service
systemctl disable wit_scheduler.service
```

## Rollback

If issues occur:

```bash
# Stop new system
docker-compose -f docker-compose.prod.yml down

# Restart old system
git checkout main
python run_pipelines.py scheduler
```

## Monitoring

```bash
# View logs
docker-compose -f docker-compose.prod.yml logs -f

# View specific service
docker-compose -f docker-compose.prod.yml logs -f worker

# Check Celery tasks
docker-compose -f docker-compose.prod.yml exec worker celery -A config inspect active
```

## Troubleshooting

### Services won't start
```bash
docker-compose -f docker-compose.prod.yml logs backend
# Check for migration errors, permission issues
```

### Database connection failed
```bash
# Verify postgres is running
docker-compose -f docker-compose.prod.yml ps postgres

# Check credentials in .env.production
```

### Static files not loading
```bash
# Recollect static files
docker-compose -f docker-compose.prod.yml exec backend python manage.py collectstatic --noinput
```
```

- [ ] **Step 2: Commit**

```bash
git add docs/deployment.md
git commit -m "docs: create production deployment guide"
```

---

## Task 6: Test Production Build Locally

**Files:**
- None (testing only)

- [ ] **Step 1: Create production env locally**

```bash
cp .env.production.example .env.production
# Edit with real values
nano .env.production
```

- [ ] **Step 2: Build production images**

```bash
docker-compose -f docker-compose.prod.yml build
```

Expected: All images built successfully

- [ ] **Step 3: Start production stack locally**

```bash
docker-compose -f docker-compose.prod.yml up -d
```

- [ ] **Step 4: Check all services running**

```bash
docker-compose -f docker-compose.prod.yml ps
```

Expected: All services "Up"

- [ ] **Step 5: Test API**

```bash
curl http://localhost/api/v1/wallets/
```

Expected: JSON response

- [ ] **Step 6: Test admin**

Open: http://localhost/admin/

Expected: Admin login page

- [ ] **Step 7: Check health**

```bash
curl http://localhost/health/
```

Expected: "healthy"

- [ ] **Step 8: Stop production stack**

```bash
docker-compose -f docker-compose.prod.yml down
```

---

## Task 7: Deploy to VPS

**Files:**
- None (deployment only)

- [ ] **Step 1: Push code to repository**

```bash
git push origin feature/django-migration
```

- [ ] **Step 2: SSH to VPS**

```bash
ssh root@46.224.0.146
```

- [ ] **Step 3: Clone repository on VPS**

```bash
cd /opt
git clone <your-repo-url> wit-v1
cd wit-v1
git checkout feature/django-migration
```

- [ ] **Step 4: Configure production environment**

```bash
cp .env.production.example .env.production
nano .env.production
# Set all values (secret key, passwords, API keys)
```

- [ ] **Step 5: Build and start on VPS**

```bash
docker-compose -f docker-compose.prod.yml build
docker-compose -f docker-compose.prod.yml up -d
```

- [ ] **Step 6: Migrate data**

```bash
# If old SQLite DB is on VPS
docker-compose -f docker-compose.prod.yml exec backend python manage.py migrate_from_sqlite --sqlite-path /path/to/old/wit_database.db
```

- [ ] **Step 7: Verify deployment**

```bash
# Check services
docker-compose -f docker-compose.prod.yml ps

# Check logs
docker-compose -f docker-compose.prod.yml logs backend

# Test API
curl http://localhost/api/v1/wallets/
```

- [ ] **Step 8: Setup systemd auto-restart**

Follow guide in `docs/deployment.md`

- [ ] **Step 9: Stop old system**

```bash
pkill -f run_pipelines.py
# Or: systemctl stop wit_scheduler.service
```

---

## Task 8: Final Verification

**Files:**
- None (verification only)

- [ ] **Step 1: Verify all services running**

```bash
docker-compose -f docker-compose.prod.yml ps
```

Expected: All "Up"

- [ ] **Step 2: Verify data migrated**

```bash
docker-compose -f docker-compose.prod.yml exec backend python manage.py shell
```

```python
from wallets.models import Wallet, SmartWallet
print(f"Wallets: {Wallet.objects.count()}")
print(f"Smart wallets: {SmartWallet.objects.count()}")
```

- [ ] **Step 3: Verify API accessible externally**

From your local machine:
```bash
curl http://46.224.0.146/api/v1/wallets/
```

- [ ] **Step 4: Verify Celery tasks running**

```bash
docker-compose -f docker-compose.prod.yml logs worker | grep "Task"
```

Expected: Tasks executing

- [ ] **Step 5: Verify Beat scheduler**

```bash
docker-compose -f docker-compose.prod.yml logs beat
```

Expected: Scheduled tasks visible

- [ ] **Step 6: Test frontend connection**

Update frontend API URL to point to VPS:
```typescript
// frontend/lib/api.ts
const API_BASE_URL = 'http://46.224.0.146/api/v1';
```

Open frontend: http://localhost:3000

Expected: Real data from VPS

---

## Final Validation Checkpoint

- [ ] **All services running on VPS**
- [ ] **API accessible externally**
- [ ] **Admin accessible**
- [ ] **Data migrated successfully**
- [ ] **Celery tasks executing**
- [ ] **Beat scheduler working**
- [ ] **Frontend connected**
- [ ] **Old system stopped**
- [ ] **Auto-restart configured (systemd)**

---

## Task 9: Merge to Main

**Files:**
- None (git only)

- [ ] **Step 1: Final commit**

```bash
git status
git add .
git commit -m "feat: complete Django migration - production ready"
```

- [ ] **Step 2: Push feature branch**

```bash
git push origin feature/django-migration
```

- [ ] **Step 3: Merge to main**

```bash
git checkout main
git merge feature/django-migration
```

- [ ] **Step 4: Tag release**

```bash
git tag -a v2.0.0 -m "Django migration complete - production"
git push origin main --tags
```

- [ ] **Step 5: Update VPS to main branch**

```bash
ssh root@46.224.0.146
cd /opt/wit-v1
git checkout main
git pull
docker-compose -f docker-compose.prod.yml up -d --build
```

---

## Summary

**Phase 5 completed! Migration complete! 🎉**

✅ Production docker-compose configured
✅ Nginx reverse proxy setup
✅ Production environment configured
✅ Deployment documentation written
✅ Deployed to VPS
✅ Data migrated
✅ Old system stopped
✅ Auto-restart configured
✅ Merged to main

**WIT V1 is now running on Django + PostgreSQL + Celery + Redis + Docker in production!**

**Post-deployment tasks (optional):**
- [ ] Setup HTTPS with Let's Encrypt
- [ ] Configure domain name
- [ ] Setup monitoring (Sentry, Prometheus)
- [ ] Configure backups (PostgreSQL dumps)
- [ ] Implement remaining service stubs (discovery, tracking, consensus)

**Congratulations! The migration is complete.** 🚀
