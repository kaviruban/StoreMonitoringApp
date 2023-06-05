from django.db import models
from django.utils import timezone

# Create your models here.

class Store(models.Model):
    store_id = models.IntegerField(primary_key=True)
    timezone_str = models.CharField(max_length=50, default='America/Chicago')

class StoreTimings(models.Model):
    Store = models.ForeignKey(Store, on_delete=models.CASCADE)
    day_of_week = models.IntegerField(choices=enumerate(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']))
    start_time_local = models.TimeField(default='00:00:00')
    end_time_local = models.TimeField(default='23:59:59')
    
class StoreStatus(models.Model):
    Store = models.ForeignKey(Store, on_delete=models.CASCADE)
    timestamp_utc = models.CharField(max_length=100,default="2023-01-25 18:13:22.47922 UTC")
    status = models.CharField(max_length=8, choices=(('active', 'active'), ('inactive', 'inactive')))
    time_created = models.DateTimeField(default=timezone.now)

class ReportData(models.Model):
    Store = models.ForeignKey(Store, on_delete=models.CASCADE)
    uptime_last_hour = models.IntegerField(default=0)
    uptime_last_day = models.IntegerField(default=0)
    uptime_last_week = models.IntegerField(default=0)
    downtime_last_hour = models.IntegerField(default=0)
    downtime_last_day = models.IntegerField(default=0)
    downtime_last_week = models.IntegerField(default=0)
