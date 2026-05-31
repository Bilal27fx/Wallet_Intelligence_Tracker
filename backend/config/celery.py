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
    'scoring-pipeline-daily': {
        'task': 'wallets.tasks.run_scoring_pipeline',
        'schedule': crontab(hour=4, minute=0),  # 04:00 daily
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
