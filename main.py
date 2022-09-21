# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# with io.open('hive.txt', 'a') as f:
#     for value in t.values():
#         f.write(str(value))
from __future__ import print_function
import pygsheets
import gspread
import pandas as pd
import gspread_dataframe as gd

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

def write(t):
    data = []
    for value in t.values():
        if value.rundate >= str(date.today() - timedelta(days=15)):
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
            value.user, value.status, value.hive_query_id, value.tables_read, value.max_scan_date, value.rundate,
            value.query_start_time, value.query_end_time, value.query_run_time, value.compile, value.build_dag,
            value.submit_dag, value.submit_to_running, value.run_dag, runtime_in_mins, runtime_in_buckets]

            data.append(data_tuple)

    df = pd.DataFrame(data)
    # print(len(df.columns))

    df.columns =['User', 'QueryStatus', 'hive_query_id', 'Tables_read', 'max_read_date', 'run_date',
                               'start_time','end_time', 'Run_time', 'Compile', 'Build_dag', 'Submit_dag',
                               'Submit_to_running', 'Run_dag', 'runtime_in_mins', 'runtime_in_Buckets']


    # # initialize list of lists
    # data2 = [['Geeks', 10], ['for', 15], ['geeks', 20]]
    #
    # # Create the pandas DataFrame
    # df2 = pd.DataFrame(data2, columns = ['Name', 'Age'])
    # print(df)

    # sh = gc.open('Hive_Query_Profiling')
    gs = gc.open('Hive_Query_Profiling')
    # wks = sh[0]
    worksheet1 = gs.worksheet('Sheet1')
    existing = gd.get_as_dataframe(worksheet1)
    print(existing)
    updated = existing.append(df)
    print(f'updating')
    print(updated)
    gd.set_with_dataframe(worksheet1, updated)



    # set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
    #                    include_column_header=True, resize=True)


    # res = sheet.values().append(spreadsheetId=spreadsheet_id,
    #                     range="Sheet1!a1:i1",
    #                     valueInputOption="USER_ENTERED",
    #                     insertDataOption="INSERT_ROWS",
    #                     body={"values": data}).execute()

    # f.write(str(value))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    daywise_total_query = dict()
    a = AmbariParser(1, 50)
    t = AmbariView(a.get_dag_summary()).parse()
    write(t)
    #  daywise_total_query = total_number_of_query_per_day(daywise_total_query, t)

    last_hive_query_id = list(t)[-1]
    # print(list(t))

    while len(t) != 0:
        a = AmbariParser(1, 50, last_hive_query_id)
        t = AmbariView(a.get_dag_summary()).parse()
        write(t)
        #  total_number_of_query_per_day(daywise_total_query, t)
        #   print(t)
        # print(last_hive_query_id)
        last_hive_query_id = list(t)[-1]

    print("done")
