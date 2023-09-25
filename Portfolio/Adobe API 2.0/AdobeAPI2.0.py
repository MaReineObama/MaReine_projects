import os
import ast
import sys
import jwt
import json
import time
import pyarrow
import numpy as np
import pandas as pd
import requests
import aanalytics2 as api2
#from aanalytics2 import ingestion
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime, timedelta, timezone
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#this file contains company's info such as organisation id, client id, secret, path to the key.
config = json.loads(open('path_to_the_config_file.json', "r").read())

report_query = json.loads(open('path_to_query.json', 'r').read())
report_suite_id = report_query['rsid']



# Encode JWT
# An important step is the encode the private key contained in the .key file which is extracted on the Adobe platform. Only an Administrator account can do so.
# The encoded key is then used in a post request to obtain the access token. The access token is required in any other request such as the report request.

private_key = open(config['pathToKey']).read() #config is a dict obtained after read the config file. The pathToKey's value is the path to the key which needs to be read.
now_plus_2h = int(time.time()) + 2 * 60 * 60
jwt_payload={
        "exp": now_plus_2h,
        "iss": config['org_id'],
        "sub": config['tech_id'],
        'https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk': True,
        'aud': f"https://ims-na1.adobelogin.com/c/{config['client_id']}",
        'meta_scopes': ['ent_analytics_bulk_ingest_sdk','read','write',f"https://analytics.adobe.io/api/{report_suite_id}/write",f"https://analytics.adobe.io/api/{report_suite_id}/read"]
        }
encoded_jwt = jwt.encode(payload=jwt_payload, key= private_key, algorithm='RS256') # perform the RS256 encoding
auth_url = "https://ims-na1.adobelogin.com/ims/exchange/jwt"


header_jwt = {
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded'
    }
payload = {
        'client_id': config['client_id'],
        'client_secret': config['secret'],
        'jwt_token': f"{encoded_jwt}",
        }

    
response = requests.post(url=auth_url, headers=header_jwt, data=payload)
if response.status_code == 200:
        access_token = response.json()['access_token']
        print('access token: {}'.format(access_token))
else:
        print('Error encoding jwt for access token')
        sys.exit()



#get company_infos
#the company id is an important piece of the url used to request the report
url_company_id = "https://analytics.adobe.io/discovery/me"
header = {"Accept": "application/json",
          "Content-Type": "application/json",
          "Authorization": f"Bearer {access_token}",
          "x-api-key": f"{config['client_id']}"
          }
r_id = requests.get(url = url_company_id, headers = header)

if r_id.status_code == 200:
    COMPANYID = r_id.json()['imsOrgs'][0]['companies'][0]['globalCompanyId']
    print('company id: {}'.format(COMPANYID))
else:

    print('Error in the request')
    sys.exit()


#relevant endpoints
_endpoint = f"https://analytics.adobe.io/api/{COMPANYID}"
_getRS = '/reportsuites/collections/suites'
_getDimensions = '/dimensions'
_getMetrics = '/metrics'
_getSegments = '/segments'
_getCalcMetrics = '/calculatedmetrics'
_getDateRanges = '/dateranges'
_getReport = '/reports'


#get report

url_report = _endpoint + _getReport
header = {"Accept": "application/json",
          "Content-Type": "application/json",
          "Authorization": f"Bearer {access_token}",
          "x-api-key": f"{config['client_id']}",
          "x-proxy-global-company-id": f"{COMPANYID}"
          }
report_post = requests.post(url = url_report, headers = header, json = report_query)
reports = report_post.json()

#cleaned the outputted report and store the data in a dataframe
report_col = ['itemId', 'Date']
for val in reports['columns']['columnIds']:
    report_col.append(val)
print('columns of the dataframe', report_col)
data = []
for i in range(len(reports['rows'])):
    data_ = [reports['rows'][i]['itemId'], reports['rows'][i]['value']]
    for val in reports['rows'][i]['data']:
        data_.append(val)
    data.append(data_)
print('data for the dataframe', data)

report_df = pd.DataFrame(columns=report_col, data=data)
df_col_map = {}
for n in range(len(report_query['metricContainer']['metrics'])):
    df_col_map[report_query['metricContainer']['metrics'][n]['columnId']] = report_query['metricContainer']['metrics'][n]['id']

print('columns mapping', df_col_map)
df_col_map['itemId'] = 'itemId'
df_col_map['Date'] = 'Date'
report_df.columns = report_df.columns.map(df_col_map)
print('report columns formatted', report_df.columns)
report_df['Date'] = pd.to_datetime(report_df['Date']).dt.date


#this may be usually to know what report you can request
#get report suites

url_report = _endpoint + _getRS
reportSuites_post = requests.get(url = url_report, headers = header, json = report_query)
reportSuites = reportSuites_post.json()

#report suites dataframe
reportSuites_df_cols = list(reportSuites['content'][0].keys())
reportSuites_list= []
for i in range(len(reportSuites['content'])):
    reportSuites_list.append([reportSuites['content'][i][reportSuites_df_cols[1]], reportSuites['content'][i][reportSuites_df_cols[2]], reportSuites['content'][i][reportSuites_df_cols[3]]])

reportSuites_df = pd.DataFrame(data=reportSuites_list, columns=reportSuites_df_cols[1:])