# Import the required libraries
import requests
import json
import pyodbc
import sys
import os
import pandas as pd
import datetime as dt
from datetime import datetime

# Get the command line arguments
if (len(sys.argv) < 4):
    print("Usage: python <script> <site> <point type> <duration in mins>")
    print("Example: python test.py KM PlantEfficiency 1440")
    exit()

site = sys.argv[1]
point_type = sys.argv[2]
extract_duration = sys.argv[3]

# Define global variables
lc_SUCCESS = 1
lc_FAILURE = -1
headers = {
  'Content-Type': 'application/json',
}

# Define the proxy to abe able to communicate to external sources
http_proxy  = "http://proxy-dmz.intel.com:912"
https_proxy = "http://proxy-dmz.intel.com:912"
proxies = {
              "http"  : http_proxy,
              "https" : https_proxy
            }

# URL to get the authorization token and the point history
token_url = "https://api.optimumenergyco.com/v1/oauth/token"
hist_url = "https://api.optimumenergyco.com/v1/data/query"

# Payload to send to obtain the bearer token
token_payload = json.dumps({
  "grantType": "password",
  "api_key": "OWZlYTNkOTEtNGQ3My00ZDM3LThhNmYtOTZlZjE0NTA4NWVhOnpUeEZ0eGt4b2NzRGhlekVzS2JHV0IyQXpSeUVyOS1UUHEtcHg3VjN2VnM="
})

# Define the connection to the SQL Server database
server = 'tcp:sql3617-fm1-in.amr.corp.intel.com,3181'
database = 'CS_CE_Auto'
username = 'CS_CE_Auto_so'
password = 'bF6R5NpN1Ha1855'

run_start_date = datetime.now()
job_name = 'OPTIMUM_EXTRACT'
sub_config_name = 'Id'

# Get the connection to SQL server database
db_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
db_cur = db_conn.cursor()

# Function to get the last successful extract complete date
def get_load_date():
    # Get the extract end dates for the loader
    try:
        db_cur.execute("select format(ExtractEndDate, 'yyyy-dd-MM HH:mm:ss') from dbo.x_loader_progress where JobName = ? and Site = ? and SubJobName = ?", (job_name, site, point_type))
        end_date = db_cur.fetchone()
    # Handle all other exceptions
    except:
        db_cur.rollback()
        return None
    return end_date

# Function to get the list of points to extract data
def get_loader_config():
    # Get the configuration for the loader
    try:
        db_cur.execute("select ConfigValue from dbo.x_loader_config where LoaderName = ? and ConfigName = ? and Site = ? and SubConfigName = ?", (job_name, point_type, site, sub_config_name))
        config_rows = db_cur.fetchall()
    # Handle all other exceptions
    except:
        db_cur.rollback()
        return None
    return config_rows

def load_plant_values(vallist, timelist, ptid):
    # Iterate through the list of time and values and insert into the database
    for i in range(len(vallist)):
        hist_sql = "insert into dbo.optimum_pointvalue (PointNum, MeasureValue, MeasureDate) values (?, ?, ?)"
        try:
            db_cur.execute(hist_sql, (int(ptid), (0 if vallist[i] is None else float(vallist[i])), (datetime.fromisoformat(timelist[i])).replace(tzinfo=None)))
            db_cur.commit()
        # Handle if record already exists, continue
        except pyodbc.IntegrityError as err:
            db_cur.rollback()
        # Handle all other exceptions and error out
        except:
            print("Unable to load plant history")
            db_cur.rollback()
            return lc_FAILURE
    return lc_SUCCESS

def load_nonplant_values(vallist, timelist, ptid):
    # Iterate through the list of time and values and insert into the database
    for i in range(len(vallist)):
        hist_sql = "insert into optimum_pointvalue (PointNum, MeasureValue, MeasureDate) values (?, ?, ?)"
        try:
            db_cur.execute(hist_sql, (int(ptid), (0 if vallist[i] is None else int(vallist[i] == True)), (datetime.fromisoformat(timelist[i])).replace(tzinfo=None)))
            db_cur.commit()
        # Handle if record already exists, continue
        except pyodbc.IntegrityError as err:
            db_cur.rollback()
        # Handle all other exceptions and error out
        except:
            return lc_FAILURE
    return lc_SUCCESS

# Function to check for meta record
def check_for_meta_record():
    # Retrieve the point meta data if it exists
    check_sql = "select PointNum from optimum_pointmeta where site = ? and pointid = ? and pointname = ? and stationid = ?"
    try:
        db_cur.execute(check_sql,(site, histdict[0]['id'], point_type, histdict[0]['stationId']))
        PointNum = db_cur.fetchone()
    except:
        PointNum = None
        print("no meta record found")

    return PointNum

# Function to load meta record
def load_meta_record():
    # Retrieve the point meta data
    meta_sql = "SET NOCOUNT ON; " + \
            "insert into optimum_pointmeta (Site, PointId, PointName, ShortName, StationId, " + \
            "OwnerId, OwnerType, MeasureType, MinResolution, RollupAgg, SubCalcAgg, " + \
            "UnitOfMeasurement, SignalType, IsCustomized, HWIntegSource, HWIntegAddress, LastUpdatedBy) " + \
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); " + \
            "SELECT IDENT_CURRENT('optimum_pointmeta') as PointNum;"
    try:
        db_cur.execute(meta_sql, (site, histdict[0]['id'], histdict[0]['name'], histdict[0]['shortName'], histdict[0]['stationId'], \
            histdict[0]['ownerId'], histdict[0]['ownerType'], histdict[0]['type'], histdict[0]['minimumResolution'], \
            histdict[0]['rollupAggregation'], histdict[0]['subCalculationAggregation'], histdict[0]['unitOfMeasurement'], \
            histdict[0]['signalType'], int(histdict[0]['customized'] == True), histdict[0]['hardwareIntegrationSource'], \
            histdict[0]['hardwareIntegrationAddress'], os.getenv('username')))
        PointNum = db_cur.fetchone()
        db_cur.commit()
        return PointNum
    except pyodbc.IntegrityError as err:
        db_cur.rollback()
        return None
    except:
        print("Unable to load point meta")
        db_cur.rollback()
        return None

# Load the point meta and history to the database
def load_point_history():
    # Load the meta record
    PointNum = check_for_meta_record()
    if (PointNum is None):
        PointNum = load_meta_record()
        if (PointNum is None):
            return lc_FAILURE

    # Get the list of time and point values to load
    vallist = histdict[0]['values']
    timelist = histdict[0]['timestamps']
    print("done with meta")

    # Check for the type of point
    if (point_type == "Plant Efficiency"):
        ret_code = load_plant_values(vallist, timelist, PointNum[0])
    else:
        ret_code = load_nonplant_values(vallist, timelist, PointNum[0])

    return ret_code

def update_job_details():
    # Define the sql
    job_sql = "update dbo.x_loader_progress set Status = ?, RunStartDate = ?, RunEndDate = ?, ExtractStartDate = ?, ExtractEndDate = ? " + \
              " where JobName = ? and Site = ? and SubJobName = ?"
    try:
        db_cur.execute(job_sql, (('SUCCESS' if ret_code > 0 else 'FAILURE'), run_start_date.replace(microsecond=0), run_end_date.replace(microsecond=0), \
                                 (start_dt if ret_code > 0 else prev_end_dt), (end_dt if ret_code > 0 else prev_end_dt), \
                                 job_name, site, point_type))
        db_cur.commit()
    except:
        db_cur.rollback()
        return lc_FAILURE
    return lc_SUCCESS

# Get the last successful extract end date
extract_end_date = get_load_date()
if (extract_end_date is None):
    print("Unable to find the loader run info")
    exit()

# Save the previous extract end time
prev_end_dt = datetime.strptime(extract_end_date[0], '%Y-%d-%m %H:%M:%S')

# Calculate the new start and end dates
start_dt = datetime.strptime(extract_end_date[0], '%Y-%d-%m %H:%M:%S')
# We need to add 5 minutes to the start as the dates are inclusive and would have already been loaded as part of the previous runs
start_dt = start_dt + dt.timedelta(minutes=5)
end_dt = start_dt + dt.timedelta(minutes=int(extract_duration)-5)

# Check if the end date is lesser than current time
if (end_dt > datetime.now()):
    end_dt = datetime.now()

# Get the access token one time only
jsonresponse = requests.request("POST", token_url, headers=headers, data=token_payload, proxies=proxies)
bearer_token = jsonresponse.json().get('accessToken')

# Get the point Ids for which data needs to be extracted
point_ids = get_loader_config()
if (point_ids is None):
    print("Unable to find the config needed by the loader")
    exit()

# Loop for each point for the given site and point type
# Data can be extracted for multiple points together as long as they belong to the same station id
for row in point_ids:
    # Payload to send to retrieve point history
    mydict = {'accessToken':bearer_token, 'ids': [row[0]],
    'startAt':start_dt.strftime("%Y-%m-%dT%H:%M:%S%zZ"), 'endAt':end_dt.strftime("%Y-%m-%dT%H:%M:%S%zZ"), 'resolution':'fiveMinute'}
    hist_payload = json.dumps(mydict)

    # Get the history data for the point
    histresponse = requests.request("POST", hist_url, headers=headers, data=hist_payload, proxies=proxies)
    histdict = histresponse.json()

    # Load the point data to database
    ret_code = load_point_history()
    if (ret_code == lc_FAILURE):
        print("Unable to load the point history")

# Update the job details
run_end_date = datetime.now()
x = update_job_details()

print("job complete")




