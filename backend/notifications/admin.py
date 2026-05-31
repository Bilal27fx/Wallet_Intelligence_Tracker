"""Notifications admin."""
from django.contrib import admin
from .models import NotificationLog


@admin.register(NotificationLog)
class NotificationLogAdmin(admin.ModelAdmin):
    list_display = ['created_at', 'notification_type', 'sent_successfully']
    list_filter = ['notification_type', 'sent_successfully', 'created_at']
    search_fields = ['message']
    readonly_fields = ['created_at']
