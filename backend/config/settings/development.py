"""Development settings."""
from .base import *

DEBUG = True
ALLOWED_HOSTS = ['*']

# Development-specific settings
INTERNAL_IPS = ['127.0.0.1']
