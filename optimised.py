# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# with io.open('hive.txt', 'a') as f:
#     for value in t.values():
#         f.write(str(value))
from __future__ import print_function

import math
import numpy
import pygsheets
import gspread
import pandas as pd
import gspread_dataframe as gd
import time
import datetime


from google.oauth2 import service_account
from AmbariViewDAO import AmbariView
from AmbariParser import AmbariParser

import io
from datetime import date
from datetime import timedelta


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def total_number_of_query_per_day(daywise_total_query, t):
    all_keys = t.keys()
    for k in all_keys:
        parsed_date = parse_key(k)

        if parsed_date in daywise_total_query:
            daywise_total_query[parsed_date] = daywise_total_query[parsed_date] + 1
        else:
            daywise_total_query[parsed_date] = 1
    print(daywise_total_query)
    return daywise_total_query


def parse_key(key):
    p = key[5:13]
    if key == 't-prod_2':
        pass
    return p


SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# service = build('sheets', 'v4', credentials=creds)
spreadsheet_id = '11AaB1fq-9a7cp3s-vNMUrUi_DYTdOOptSSw8bZxAvTY'
# sheet = service.spreadsheets()

gc = pygsheets.authorize(service_file='keys.json')

gc = gspread.authorize(creds)



def write_to_google_sheet(data):

    df = pd.DataFrame(  data)
    print(len(df.columns))

    df.columns = ['User', 'QueryStatus','SelectAllFlag','Status_aft_completion', 'hive_query_id', 'Tables_read', 'max_read_date', 'run_date',
                  'start_time', 'end_time', 'Run_time', 'Compile', 'Build_dag', 'Submit_dag',
                  'Submit_to_running', 'Run_dag', 'runtime_in_mins', 'runtime_in_Buckets', 'data_pull_time']
    gs = gc.open('Hive_query_profiling_30 days')
    # wks = sh[0]
    worksheet1 = gs.worksheet('Sheet1')
    existing = gd.get_as_dataframe(worksheet1)
    # print(float(existing.iat[0,0])==float("nan"))
    # print(existing.iat[0,0]==existing.iat[1,0])
    # print(existing.iat[0,0],existing.iat[1,0],existing.iat[1,1])
    if type(existing.iat[0,0]) != str and math.isnan(float(existing.iat[0,0])):
        gd.set_with_dataframe(worksheet1, df, include_index=False, include_column_header=True, resize=True)
        print("if")
    else:
        # existing1=existing.dropna()
        # print(existing1)
        updated = existing.append(df)
        print(len(df),len(existing),len(updated))
        # print(updated)
        gd.set_with_dataframe(worksheet1, updated,include_index=False, include_column_header=True, resize=True)




def write(data,t):
    print("in write")
    print("length of data :"+str(len(data)))
    print(time.time())
    for value in t.values():
        runtime_in_mins = (value.query_run_time) / (60000)
        runtime_in_buckets = 5
        if runtime_in_mins <= 5:
            runtime_in_buckets = 1
        elif runtime_in_mins < 10:
            runtime_in_buckets = 2
        elif runtime_in_mins < 20:
            runtime_in_buckets = 3
        elif runtime_in_mins < 30:
            runtime_in_buckets = 4

        data_tuple = [
            value.user, value.status,value.selectallflag,value.status1, value.hive_query_id, value.tables_read, value.max_scan_date, value.rundate,
            value.query_start_time, value.query_end_time, value.query_run_time, int(value.compile) / 60000,
                                                                                int(value.build_dag) / 60000,
                                                                                int(value.submit_dag) / 60000,
                                                                                int(value.submit_to_running) / 60000,
                                                                                int(value.run_dag) / 60000,
            runtime_in_mins, runtime_in_buckets, int(time.time())]
        # print(data_tuple)

        data.append(data_tuple)
    data.pop(-1)

    # df = pd.DataFrame(data)
    # print(len(df.columns))

    # df.columns = ['User', 'QueryStatus','SelectAllFlag','Status_aft_completion', 'hive_query_id', 'Tables_read', 'max_read_date', 'run_date',
    #               'start_time', 'end_time', 'Run_time', 'Compile', 'Build_dag', 'Submit_dag',
    #               'Submit_to_running', 'Run_dag', 'runtime_in_mins', 'runtime_in_Buckets', 'data_pull_time']


if __name__ == '__main__':
    print_hi('PyCharm')
    data = []
    daywise_total_query = dict()
    epoch_start = datetime.datetime(2022, 9, 23, 0, 0).timestamp()
    epoch_end=datetime.datetime(2022, 9, 26, 0, 0).timestamp()
    a = AmbariParser(int(epoch_start)*1000,int(epoch_end)*1000, 500)
    t = AmbariView(a.get_dag_summary()).parse()
    write(data,t)
    #  daywise_total_query = total_number_of_query_per_day(daywise_total_query, t)

    last_hive_query_id = list(t)[-1]
    # print(list(t))

    while len(t) != 0:
        a = AmbariParser(int(epoch_start)*1000,int(epoch_end)*1000, 500, last_hive_query_id)
        t = AmbariView(a.get_dag_summary()).parse()
        print("length of t"+str(len(t)))

        if len(t) >1 :
            write(data,t)
            last_hive_query_id = list(t)[-1]
        else:
            break
        #  total_number_of_query_per_day(daywise_total_query, t)
        #   print(t)
        # print(last_hive_query_id)


    write_to_google_sheet(data)

    print("done")