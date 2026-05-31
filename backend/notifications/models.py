"""Notification models."""
from django.db import models


class NotificationLog(models.Model):
    """Log des notifications envoyees."""
    created_at = models.DateTimeField(auto_now_add=True)
    notification_type = models.CharField(max_length=50)
    message = models.TextField()
    sent_successfully = models.BooleanField(default=False)
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Notification Log'
        verbose_name_plural = 'Notification Logs'

    def __str__(self):
        return f"{self.notification_type} - {self.created_at}"
