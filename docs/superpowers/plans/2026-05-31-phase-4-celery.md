# Phase 4: Celery + Scheduler - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert pipelines to Celery tasks with automatic scheduling via Celery Beat.

**Architecture:** Celery workers execute async tasks, Celery Beat triggers scheduled tasks, Redis as broker/result backend.

**Tech Stack:** Celery 5.x, Redis 7, django-celery-beat

---

## Prerequisites

✅ Phase 1-3 completed (Django + API + Services)
✅ Redis running in Docker
✅ Worker and beat containers defined in docker-compose

---

## File Structure

**New files to create:**
```
backend/
├── config/
│   └── celery.py
├── wallets/
│   └── tasks.py
└── requirements/base.txt (modify)
```

---

## Task 1: Setup Celery Configuration

**Files:**
- Create: `backend/config/celery.py`
- Modify: `backend/config/__init__.py`
- Modify: `backend/config/settings/base.py`

- [ ] **Step 1: Install django-celery-beat**

Add to `backend/requirements/base.txt`:
```
django-celery-beat==2.5.0
```

Rebuild:
```bash
docker-compose build backend worker beat
```

- [ ] **Step 2: Write Celery configuration**

Create `backend/config/celery.py`:
```python
"""Celery configuration."""
import os
from celery import Celery
from celery.schedules import crontab

# Set default Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.development')

app = Celery('wit')

# Load config from Django settings
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks from all installed apps
app.autodiscover_tasks()

# Celery Beat Schedule
app.conf.beat_schedule = {
    'discovery-pipeline-daily': {
        'task': 'wallets.tasks.run_discovery_pipeline',
        'schedule': crontab(hour=2, minute=0),  # 02:00 daily
    },
    'scoring-pipeline-every-2-days': {
        'task': 'wallets.tasks.run_scoring_pipeline',
        'schedule': crontab(hour=4, minute=0, day_of_week='*/2'),  # Every 2 days at 04:00
    },
    'tracking-live-every-2-hours': {
        'task': 'wallets.tasks.run_tracking_live',
        'schedule': crontab(minute=0, hour='*/2'),  # Every 2 hours
    },
    'consensus-detection-every-2-hours': {
        'task': 'wallets.tasks.run_consensus_detection',
        'schedule': crontab(minute=30, hour='*/2'),  # Every 2h at :30
    },
}

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Paris',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes max
    task_soft_time_limit=25 * 60,  # 25 minutes soft limit
)
```

- [ ] **Step 3: Initialize Celery in Django**

Modify `backend/config/__init__.py`:
```python
"""Django initialization."""
from .celery import app as celery_app

__all__ = ('celery_app',)
```

- [ ] **Step 4: Add Celery settings to Django**

Modify `backend/config/settings/base.py`, add:
```python
# Celery Configuration
CELERY_BROKER_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = os.getenv('REDIS_URL', 'redis://redis:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'Europe/Paris'
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 30 * 60
```

Add to INSTALLED_APPS:
```python
'django_celery_beat',
```

- [ ] **Step 5: Run migrations for celery-beat**

```bash
docker-compose run --rm backend python manage.py migrate
```

Expected: django_celery_beat tables created

- [ ] **Step 6: Commit**

```bash
git add backend/config/celery.py backend/config/__init__.py backend/config/settings/base.py backend/requirements/
git commit -m "feat(celery): setup Celery configuration"
```

---

## Task 2: Create Celery Tasks

**Files:**
- Create: `backend/wallets/tasks.py`

- [ ] **Step 1: Write Celery tasks**

Create `backend/wallets/tasks.py`:
```python
"""Celery tasks for WIT pipelines."""
from celery import shared_task
from celery.utils.log import get_task_logger
from django.core.management import call_command

logger = get_task_logger(__name__)


@shared_task(bind=True, max_retries=3)
def run_discovery_pipeline(self):
    """
    Run token discovery pipeline (Dune + explosion detection).
    Currently calls old system - TODO: migrate to Django services.
    """
    try:
        logger.info("Starting discovery pipeline...")

        # TODO: Replace with Django service when migrated
        # For now, call old system or skip
        logger.warning("Discovery pipeline not yet migrated - skipping")

        logger.info("Discovery pipeline completed")
        return {'status': 'success', 'message': 'Discovery skipped (not migrated)'}

    except Exception as exc:
        logger.error(f"Discovery pipeline failed: {exc}")
        self.retry(exc=exc, countdown=300)  # Retry after 5min


@shared_task
def run_scoring_pipeline():
    """
    Run scoring pipeline (FIFO + wallet scoring).
    Calls Django management command.
    """
    try:
        logger.info("Starting scoring pipeline...")

        # Call Django management command
        call_command('run_scoring')

        logger.info("Scoring pipeline completed")
        return {'status': 'success', 'message': 'Scoring completed'}

    except Exception as exc:
        logger.error(f"Scoring pipeline failed: {exc}")
        raise


@shared_task
def run_tracking_live():
    """
    Run live tracking (Zerion balance tracking).
    Currently calls old system - TODO: migrate to Django services.
    """
    try:
        logger.info("Starting tracking live...")

        # TODO: Replace with Django service when migrated
        logger.warning("Tracking live not yet migrated - skipping")

        logger.info("Tracking live completed")
        return {'status': 'success', 'message': 'Tracking skipped (not migrated)'}

    except Exception as exc:
        logger.error(f"Tracking live failed: {exc}")
        raise


@shared_task
def run_consensus_detection():
    """
    Run consensus detection + Telegram alerts.
    Currently calls old system - TODO: migrate to Django services.
    """
    try:
        logger.info("Starting consensus detection...")

        # TODO: Replace with Django service when migrated
        logger.warning("Consensus detection not yet migrated - skipping")

        logger.info("Consensus detection completed")
        return {'status': 'success', 'message': 'Consensus skipped (not migrated)'}

    except Exception as exc:
        logger.error(f"Consensus detection failed: {exc}")
        raise


@shared_task
def send_telegram_alert(signal_id: int):
    """Send Telegram alert for consensus signal."""
    from wallets.models import ConsensusSignal
    from notifications.services.telegram_bot import send_consensus_alert

    try:
        signal = ConsensusSignal.objects.get(id=signal_id)
        send_consensus_alert(signal)
        logger.info(f"Telegram alert sent for signal {signal_id}")
        return {'status': 'success', 'signal_id': signal_id}

    except ConsensusSignal.DoesNotExist:
        logger.error(f"ConsensusSignal {signal_id} not found")
        raise

    except Exception as exc:
        logger.error(f"Failed to send Telegram alert: {exc}")
        raise
```

- [ ] **Step 2: Commit**

```bash
git add backend/wallets/tasks.py
git commit -m "feat(celery): create Celery tasks for pipelines"
```

---

## Task 3: Create Telegram Bot Service

**Files:**
- Create: `backend/notifications/services/__init__.py`
- Create: `backend/notifications/services/telegram_bot.py`

- [ ] **Step 1: Create services directory**

```bash
mkdir -p backend/notifications/services
touch backend/notifications/services/__init__.py
```

- [ ] **Step 2: Write Telegram bot service**

Create `backend/notifications/services/telegram_bot.py`:
```python
"""Telegram bot service."""
import os
import requests
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class TelegramBot:
    """Telegram bot for sending alerts."""

    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.channel_id = os.getenv('TELEGRAM_CHANNEL_ID', '')
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(self, message: str, parse_mode: str = 'HTML'):
        """Send message to Telegram channel."""
        if not self.bot_token or not self.channel_id:
            logger.warning("Telegram not configured - skipping alert")
            return False

        url = f"{self.base_url}/sendMessage"
        data = {
            'chat_id': self.channel_id,
            'text': message,
            'parse_mode': parse_mode,
        }

        try:
            response = requests.post(url, json=data, timeout=10)
            response.raise_for_status()
            logger.info("Telegram message sent successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False


def send_consensus_alert(consensus_signal):
    """Send consensus alert to Telegram."""
    bot = TelegramBot()

    message = f"""
🚨 <b>CONSENSUS DETECTED</b> 🚨

Token: <b>{consensus_signal.token_symbol}</b>
Chain: {consensus_signal.chain}
Contract: <code>{consensus_signal.contract_address}</code>

📊 Wallets: {consensus_signal.nb_wallets}
💰 Total USD: ${consensus_signal.total_usd_invested:,.0f}
📈 Market Cap: ${consensus_signal.market_cap:,.0f}

Detected: {consensus_signal.detected_at.strftime('%Y-%m-%d %H:%M')}
"""

    return bot.send_message(message)
```

- [ ] **Step 3: Commit**

```bash
git add backend/notifications/services/
git commit -m "feat(notifications): create Telegram bot service"
```

---

## Task 4: Test Celery Tasks

**Files:**
- None (testing only)

- [ ] **Step 1: Start all services**

```bash
docker-compose up -d
```

- [ ] **Step 2: Check worker logs**

```bash
docker-compose logs -f worker
```

Expected: "celery@... ready" and tasks registered

- [ ] **Step 3: Test task execution manually**

```bash
docker-compose exec backend python manage.py shell
```

```python
from wallets.tasks import run_scoring_pipeline

# Trigger task
result = run_scoring_pipeline.delay()

# Check status
print(result.status)  # 'PENDING', 'STARTED', 'SUCCESS'

# Wait for result
print(result.get(timeout=60))
```

Expected: Task executes successfully

- [ ] **Step 4: Check beat scheduler**

```bash
docker-compose logs beat
```

Expected: Celery beat scheduler started, schedules visible

- [ ] **Step 5: Inspect scheduled tasks**

```bash
docker-compose exec backend python manage.py shell
```

```python
from django_celery_beat.models import PeriodicTask
print(PeriodicTask.objects.all())
```

Or use Celery command:
```bash
docker-compose exec worker celery -A config inspect scheduled
```

Expected: 4 scheduled tasks visible

- [ ] **Step 6: Test manual task trigger**

```bash
docker-compose exec backend python manage.py shell
```

```python
from wallets.tasks import run_scoring_pipeline
run_scoring_pipeline.apply_async()
```

Check logs:
```bash
docker-compose logs -f worker
```

Expected: Task executes, logs visible

---

## Task 5: Optional - Setup Flower Monitoring

**Files:**
- Modify: `docker-compose.yml`
- Modify: `backend/requirements/development.txt`

- [ ] **Step 1: Add Flower to requirements**

Add to `backend/requirements/development.txt`:
```
flower==2.0.1
```

- [ ] **Step 2: Add Flower service to docker-compose**

Add to `docker-compose.yml`:
```yaml
  flower:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    command: celery -A config flower --port=5555
    ports:
      - "5555:5555"
    env_file:
      - .env
    depends_on:
      - redis
      - worker
    environment:
      - DJANGO_SETTINGS_MODULE=config.settings.development
      - REDIS_URL=redis://redis:6379/0
```

- [ ] **Step 3: Rebuild and start Flower**

```bash
docker-compose build flower
docker-compose up -d flower
```

- [ ] **Step 4: Access Flower dashboard**

Open: http://localhost:5555

Expected: Celery monitoring dashboard with tasks visible

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml backend/requirements/development.txt
git commit -m "feat(celery): add Flower monitoring (optional)"
```

---

## Validation Checkpoint

Before moving to Phase 5, verify:

- [ ] **Celery worker running**
```bash
docker-compose ps worker
```
Expected: "Up"

- [ ] **Celery beat running**
```bash
docker-compose ps beat
```
Expected: "Up"

- [ ] **Tasks registered**
```bash
docker-compose exec worker celery -A config inspect registered
```
Expected: 4+ tasks listed

- [ ] **Scheduled tasks configured**
```bash
docker-compose exec worker celery -A config inspect scheduled
```
Expected: 4 schedules visible

- [ ] **Manual task execution works**
```bash
docker-compose exec backend python manage.py shell
>>> from wallets.tasks import run_scoring_pipeline
>>> result = run_scoring_pipeline.delay()
>>> result.status
'SUCCESS'
```

- [ ] **Logs visible**
```bash
docker-compose logs worker | grep "Task"
```
Expected: Task execution logs

---

## Summary

**Phase 4 completed! You now have:**

✅ Celery configured with Django
✅ Celery tasks for all pipelines
✅ Celery Beat scheduler (auto-execution)
✅ Telegram bot service
✅ Flower monitoring (optional)
✅ Tasks tested and working

**Note:** Some tasks (discovery, tracking, consensus) are stubs and skip execution. They can be fully implemented later or continue using old system in parallel.

**Next phase:** Production deployment (Phase 5)

**To continue:**
```bash
cat docs/superpowers/plans/2026-05-31-phase-5-production.md
```
