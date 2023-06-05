import csv
import os
from datetime import datetime, timedelta
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import pytz
import requests
from django.core.management import call_command
from django.db import migrations, transaction
from django.db.models import Q
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.writer import MigrationWriter

import StoreMonitoring.store_monitoring_local_settings as local
from StoreMonitoring.models import *



#class to read CSV data either from a remote URL or from a local file

class CsvFileReader:
    def __init__(self, url, file_path):
        self.url = url
        self.file_path = file_path

    #read csv from local storage
    def get_data_from_local(self):
        with open(self.file_path,"r") as f:
            data=csv.DictReader(f)
        return data
    
    #read csv from server 
    def get_data_from_server(self):
        try:
            response = requests.get(self.url, timeout=15)
            if response.status_code == 200:
                content=response.text
                data=csv.DictReader(content.splitlines())
                
                return data
        except requests.exceptions.Timeout:
            return None
        
    #returns the csv file as DictReader.Checks for new csv file from the server first then from the local storage
    def get_data(self):
        data=self.get_data_from_server()
        if not data:
            return self.get_data_from_local()
        return data
    
#parse utc_timestamp (some timestapms had missing 6 digit miliseconds value )

def parse_timestamp(timestamp_utc):
    ans=timestamp_utc.split()[0]+" "+timestamp_utc.split()[1]
    if "." not in timestamp_utc:
        return ans+".000000 UTC"
    return ans+" UTC"

#convert the UTC to Local TimeZone
def convert_utc_to_local(utc_timestamp_str, timezone_str):
    # parse the UTC timestamp string into a datetime object
    utc_timestamp = datetime.strptime(utc_timestamp_str, '%Y-%m-%d %H:%M:%S.%f %Z') if '.' in utc_timestamp_str else datetime.strptime(utc_timestamp_str, '%Y-%m-%d %H:%M:%S %Z')

    # get the timezone object for the specified timezone string
    local_timezone = pytz.timezone(timezone_str)

    # convert the UTC datetime object to the local timezone
    local_timestamp = utc_timestamp.astimezone(local_timezone)

    # format the local timestamp as a string in the specified format
    day_of_week = local_timestamp.strftime('%A')
    local_time = local_timestamp.strftime('%H:%M:%S')
    # return the day,local_time
    return day_of_week,local_time
    
#Looks into the server for the new csv file and returns [restaurant status tuples]
def get_current_store_status_data():
    store_status_data=CsvFileReader(local.STORE_STATUS_CSV_URL,local.STORE_STATUS_CSV_PATH).get_data()
    store_status=[]
    for row in store_status_data:
        store_id=row["store_id"].strip()
        status=row["status"].strip()
        timestamp_utc=row["timestamp_utc"].strip()
        store_status.append(
            (store_id,status,parse_timestamp(timestamp_utc))
        )
    return store_status

#Populate the DB with the latest polling data 
@transaction.atomic
def populate_store_status(apps,schema_editor,time_created):
    progress=0
    store_status_obj=[]
    store_data=get_current_store_status_data()
    for status_data in store_data:
        try:
            store=Store.objects.get(store_id=status_data[0])
        except Store.DoesNotExist: 
            store=Store(store_id=status_data[0])
            store.save()
            for i in range(7):
                StoreTimings(Store=store,day_of_week=i).save()
        store_status_obj.append(StoreStatus(
            Store=store,
            status=status_data[1],
            timestamp_utc=status_data[2],
            time_created=time_created,
            )
        )
        if progress%1000==0:
            print(progress)
        progress+=1
    StoreStatus.objects.bulk_create(store_status_obj)

#Removes all RestaurantStatus data based on timestamp(time when it was populated into the db)
@transaction.atomic
def reverse_migrate(apps, schema_editor,time_created):
    StoreStatus.objects.filter(Q(time_created=time_created)).delete()
    
    
#This functions creates a custom migration file as every single hour the polling is done, The data needs to be migrated to DB.
def create_custom_migration(migration_name,time_created):
    
   # define the operations
    operations = [migrations.RunPython(
        partial(populate_store_status, time_created=time_created),
        reverse_code=partial(reverse_migrate, time_created=time_created)
    )]

    # load the applied migrations
    loader = MigrationLoader(None)
    loader.load_disk()

    # find the latest migration
    previous_migration = loader.graph.leaf_nodes()[0]

    # create the migration instance
    migration = migrations.Migration(migration_name, "StoreMonitoring")
    migration.dependencies = [(previous_migration[0], previous_migration[1])]
    migration.operations = operations

    # create the migration writer
    migration_writer = MigrationWriter(migration)

    # generate the migration file
    migration_string = migration_writer.as_string()
    
    # get the filename from the migration writer
    migration_file_path = migration_writer.path
    migration_file_name = migration_file_path.split("/")[-1]
    # logger.debug(migration_file_name)
    new_migration_file_name = f"{migration_name}"
    new_migration_file_path = migration_file_path.replace(migration_file_name, new_migration_file_name)

    # write the migration file to disk
    with open(new_migration_file_path, 'w+') as f:
        f.write(migration_string)
    return new_migration_file_path

#Calls the newly created Custom Migration file for populating RestaurantStatus data into DB
def call_migration_by_name(migration_name):
    app_label = 'StoreMonitoring'
    
    # Apply the migration using the migrate management command
    call_command('migrate', app_label, migration_name[:-3])


#Utiliy function to find the difference between two timestamps
def hours_between_times(time_str1, time_str2):
    
    time1 = datetime.strptime(time_str1, "%H:%M:%S").time()
    time2 = datetime.strptime(time_str2, "%H:%M:%S").time()

    # Calculate the time difference in seconds
    time_difference_seconds = (datetime.combine(datetime.min, time2) - datetime.combine(datetime.min, time1)).seconds

    # Convert the time difference to hours
    time_difference_hours = time_difference_seconds / 3600

    return time_difference_hours

#utility  function to check if the polling time is between the restaurant timings
def is_within_business_hours(day_of_week,local_time,store_timing):
    return store_timing.start_time_local <= local_time <= store_timing.end_time_local


#This function, fill_Reportdata, is populating uptime and downtime data for a given time range (hour, day, or week) 
#for each restaurant in a database. The function takes two arguments: time_diff, which represents the time range, and type, 
#which specifies the time range as 'hour', 'day', or 'week'

def fill_Reportdata(time_diff,type):
    # logger.info(f"started populating for {type}")
    end_time=datetime.strptime(local.TIME_CREATED, "%Y-%m-%d %H:%M:%S.%f %Z")
    start_time=end_time-time_diff
    store_statuses = StoreStatus.objects.filter(timestamp_utc__range=[start_time, end_time])
    progress=0
    for store in Store.objects.all():
        store_total_operational_time={}
        report_obj=ReportData.objects.get(Store=store)
        for store_status in store_statuses.filter(Store=store):
            day_of_week,local_time=convert_utc_to_local(store_status.timestamp_utc,store.timezone_str)
            store_timing=StoreTimings(Store=store,day_of_week=day_of_week)
            store_total_operational_time[day_of_week]=hours_between_times(Store_timing.end_time_local,store_timing.start_time_local)
            if is_within_business_hours(day_of_week,local_time,store_timing):
                if type=="hour":
                    if store_status.status=='active':
                        report_obj.uptime_last_hour=60
                elif type=="day":
                    if store_status.status=='active':
                        report_obj.uptime_last_day+=1
                    else:
                        report_obj.downtime_last_day+=1
                else:
                    if store_status.status=='active':
                        report_obj.uptime_last_week+=1
                    else:
                        report_obj.downtime_last_week+=1
        total_operational_time=1+sum(store_total_operational_time.values())
        if type=="day" or type=="week":
            total_polling_observation=1+report_obj.uptime_last_day+report_obj.downtime_last_day
            total_time=min(total_operational_time,total_polling_observation)
            report_obj.uptime_last_day=(report_obj.uptime_last_day*total_operational_time)//total_time
            report_obj.downtime_last_day=(report_obj.downtime_last_day*total_operational_time)//total_time
            
        report_obj.save()
        if progress%100==0:
            print(progress)
        progress+=1
        
                        
            
#Called when trigger-report generation is called. 
# It calls another helper function fill_Reportdata(time_diff,tpye) to fill
# respective data for hour,day,week in ReportData Table

def generate_report(report_id):
    
    #it first deletes all the previous entries in the ReportData Table
    #And then creates a new fresh ReportData objects with default values
    ReportData.objects.all().delete()
    reportData_list=[]
    for store in Store.objects.all():
        if not ReportData.objects.filter(Store=store).exists():
            reportData_list.append(ReportData(Store=store))
    ReportData.objects.bulk_create(reportData_list)
    
    
    #Call the fill_Reportdata to fill hours/days/weeks uptime/downtime values in ReportData table 
    fill_Reportdata(timedelta(hours=1),type="hour")
    fill_Reportdata(timedelta(days=1),type="day")
    fill_Reportdata(timedelta(weeks=1),type="week")
    report_data=[]
    for data in ReportData.objects.all():
        report_data.append([data.Store.store_id,
                            data.uptime_last_hour,
                            data.uptime_last_day,
                            data.uptime_last_week,
                            data.downtime_last_hour,
                            data.downtime_last_day,
                            data.downtime_last_week
                            ])
    
    #checks if the Report directory Exisit or not 
    os.makedirs(local.REPORT_DIR, exist_ok=True)
    with open(os.path.join(local.REPORT_DIR, f'{report_id}.csv'), 'w') as f:
        for lines in report_data:
            f.write(",".join(str(ele) for ele in lines)+"\n")
    
    


#Called when get_report api is called. Returns the status and the report(if generated)
def check_report_status(report_id):
    report_file = os.path.join(local.REPORT_DIR, f'{report_id}.csv')
    if os.path.exists(report_file):
        with open(report_file, 'r') as f:
            # create a CSV reader object and read the CSV data
            csv_reader = csv.reader(f)
            report_data = [row for row in csv_reader]
            return 'Complete', report_data
    else:
        return 'Running', None



