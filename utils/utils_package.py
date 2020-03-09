
import datetime
import glob
import io
import logging
import os
import smtplib
import ssl
import sys
import time

import pandas as pd
import pygsheets
import requests
from simple_salesforce import Salesforce
from sqlalchemy import create_engine, types

import pyodbc

g_connect = f"{os.environ['USERPROFILE']}/Documents/future_cat.json"
pwd_path = f"{os.environ['USERPROFILE']}/Documents"
log_time = str(datetime.datetime.fromtimestamp(
    time.time()).strftime('%Y-%m-%d %H-%M-%S'))


def logger_utils():
    """Just a simple logger outputing to stdr and log file.

    Parameters
    ----------
    None

    Returns
    -------
    logger
    """
    log_time = str(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime()))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    c_handler = logging.StreamHandler(stream=sys.stdout)
    f_handler = logging.FileHandler('logs/'+log_time+'.log')
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    formatter = logging.Formatter(
        '%(asctime)s\t%(module)s\t%(lineno)s\t%(levelname)s\t%(message)s')
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        logger.error("Uncaught exception", exc_info=(
            exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception
    return logger


logger = logger_utils()


def send_mail(subject, text, receiver_email="jslomkowski@groupon.com"):
    """
    Sends email using Google generated password skipping authentication

    Example:

    u.send_email('subject', 'text')
    """

    smtp_server = "smtp.gmail.com"
    port = 587
    sender_email = "jslomkowski@groupon.com"
    message = 'Subject: {}\n\n{}'.format(subject, text)
    with open(pwd_path + '/nb1nk3o5ur.txt', 'r') as txt:
        file = txt.readline()
    PWD_m = file.split(' ')[3]
    context = ssl.create_default_context()
    server = smtplib.SMTP(smtp_server, port)
    server.starttls(context=context)
    server.login(sender_email, PWD_m)
    server.sendmail(sender_email, receiver_email, message)
    logger.debug(f'Email {subject} send')
    server.quit()


def log_to_email(subject):
    """
    Grabs latest log from folder and sends it via send_email function.
    Should be used at the end of a file.

    Example:

    u.log_to_email('subject')

    """

    list_of_files = glob.glob('logs//*')
    latest_file = max(list_of_files, key=os.path.getctime)
    log = pd.read_csv(latest_file, sep='\t')
    log = log.to_csv(sep='\t')
    send_mail(subject, log)


def connect():
    """
    Connect to teradata. Make sure to have pwd.txt in your Documents folder.
    Format should be:
    username<space>password

    example:

    import utils.utils_package as u
    c = u.connect()

    after you are done use c.close() to close connection and free table
    """

    logger.debug('connecting to EDW...')
    # Below is a path where you store your credentials. I store them in two
    # locations - on my private machine and on Numbercrunch, thats why Try
    # and except are used
    with open(pwd_path + '/nb1nk3o5ur.txt', 'r') as txt:
        file = txt.readline()
    start_t = time.time()
    UID = file.split(' ')[0]  # in txt first one should be login
    PWD = file.split(' ')[1]  # and second one should be password
    connect = pyodbc.connect('DSN=tdwd;UID=' + UID + ';PWD=' + PWD)
    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Done. Run time: {} seconds'.format(run_time))
    return connect


def sql_import(query, connect=connect, path='SQL/'):
    """
    imports sql table to df

    query:      (str) is name of txt file with SQL query you want to download
    connect:    passed by connect function
    path:       (str) path to queries folder ie. path = 'SQL/'

    Example: a = s.sql_import('df', c)
    """

    logger.debug('Downloading ' + query)
    with open(path + query + '.txt', 'r') as txt:
        query = txt.read()
    start_t = time.time()
    df = pd.read_sql(query, connect)
    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Done. Run time: {} seconds'.format(run_time))
    return df


def sql_import_df(query, df_with_objects, connect=connect, path='SQL/'):
    """
    imports sql table to df

    query:              (str) is name of txt file with SQL query you want to
                        download
    df_with_objects:    (df) one column dataframe with objects ex. contract_id
    connect:            passed by connect function
    path:               (str) path to queries folder ie. path = 'SQL/'

    Example: df = u.sql_import_df('dp', contracts, cnxn)
    """

    logger.debug('Downloading ' + query)
    with open(path + query + '.txt', 'r') as txt:
        query = txt.read()

    header = df_with_objects.columns[0]
    df_with_objects[header] = "'" + df_with_objects[header].astype(str) + "',"
    df_with_objects = df_with_objects.to_string(index=False)
    df_with_objects = df_with_objects.replace(header, '')
    query = query+df_with_objects
    query = query[:len(query)-1] + ')'
    start_t = time.time()
    df = pd.read_sql(query, connect)
    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Done. Run time: {} seconds'.format(run_time))
    return df


def sql_import_s(query, connect=connect):
    """
    imports sql table to df. Query has to be inside script as an str object

    query:      (str) is name of txt file with SQL query you want to download
    connect:    passed by connect function

    Example: a = s.sql_import(query, c)
    """

    logger.debug('Downloading query from script')
    start_t = time.time()
    df = pd.read_sql(query, connect)
    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Done. Run time: {} seconds'.format(run_time))
    return df


def sql_export(upload, where, connect=connect):
    """
    uploadSQL(upload, where, connect)

    upload:     (object) object to upload
    where:      (str) table name in sandbox schema
    connect:    passed by connect function

    Upload Pandas DataFrame to SQL

    Example: sql_upload(df, 'fcd_slaes')
    """
    with open(pwd_path + '/nb1nk3o5ur.txt', 'r') as txt:
        file = txt.readline()
    UID = file.split(' ')[0]
    PWD = file.split(' ')[1]
    td_engine = create_engine('teradata://' + UID +
                              ':' + PWD + '@TDWD:22/%s' % 'sandbox')
    conn = td_engine.connect()
    logger.debug('Uploading to EDW')
    start_t = time.time()
    correct_dtypes = upload.select_dtypes(include=['object'])
    correct_dtypes = upload.to_dict('list')
    for key, val in correct_dtypes.items():
        correct_dtypes[key] = max([len(str(x)) for x in val])

    upload.to_sql(where, conn, if_exists='append',
                  index=False,
                  dtype={key: types.VARCHAR(val) for (key, val) in
                         correct_dtypes.items()})

    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Upload complete. Run time: {} seconds'.format(run_time))


def sql_execute(query, connect=connect, path='SQL/'):
    """
    Executes SQL query (without downloading data to Python)

    cnxn:   passed by connect function
    path:   (str) path to queries folder ie. path = 'queries/'
    query:  (str) is name of txt file with SQL query you want to excute

    Example: sql_execute('query', c)
    """

    logger.debug('Executing ' + query)
    start_t = time.time()
    cursor = connect.cursor()
    cursor.execute(query).commit()
    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Execute complete. Run time: {} seconds'.format(run_time))
    return None


def sf_import(report_number):
    """
    imports Sales Force report.
    Save PWD to Salesforce in the same PWD file but as 3rd string after space
    security token also has to be there
    report_number:(str) is SF report number found in a browser link

    example: report = u.sf_import('00OC0000007EgNU')
    """
    logger.debug('importing SF report '+report_number)
    with open(pwd_path + '/nb1nk3o5ur.txt', 'r') as txt:
        file = txt.readline()
    start_t = time.time()
    UID = file.split(' ')[0]
    PWD = file.split(' ')[2]
    STK = file.split(' ')[5]
    Salesforce(username=UID+'@groupon.com', password=PWD,
               security_token=STK)
    login_data = {'un': UID+'@groupon.com',
                  'pw': PWD, 'st': STK}
    s = requests.session()
    s.get('https://groupon-dev.my.salesforce.com/', params=login_data)
    d = requests.get("https://groupon-dev.my.salesforce.com//{}?export=1&enc=UTF-8&xf=csv".format(
        report_number), headers=s.headers, cookies=s.cookies)
    rawData = pd.read_csv(io.StringIO(d.content.decode('utf-8')))
    rawData = rawData.iloc[:-5]
    end_t = time.time()
    run_time = int(round((end_t - start_t), 0))
    logger.debug('Download complete. Run time: {} seconds'.format(run_time))
    return rawData


def gdoc_import(gdoc_url, tab_name, start_row=1, start_col=10000, end_row=10000, end_col=1, header=True):
    """
    imports gdoc to df
    warning - share gdoc with: future-cat@future-cat-183920.iam.gserviceaccount.com
    or any other google service you use
    api console - https://console.developers.google.com/apis/dashboard?project=future-cat-183920&folder&organizationId=145261633048&duration=PT1H

    gdoc_url -      (str) url of gdoc you want to import
    tab_name -      (str) tab name you want to import
    start_row -     (int) start_row
    start_col -     (int) start_col
    end_row -       (int) end_row
    end_col -       (int) end_col
    header -        (bol) do you want first row to be a header?

    Example: hcns = gdoc_import('https://docs.google.com/spreadsheets/d/1DtFNJM5IOw_eRbRO8pb3WtbPow9QZcBXrKGQLWUQqYc/edit#gid=10774715', 'hcn', start_row = 3, start_col = 1, end_row=4, end_col = 2, header = False)
    """

    logger.debug('importing gdoc '+gdoc_url)
    gc = pygsheets.authorize(service_file=g_connect)
    wks = gc.open_by_url(gdoc_url).worksheet_by_title(tab_name)
    df = pd.DataFrame(wks.get_values(start=(start_row, start_col),
                                     end=(end_row, end_col), returnas='matrix'))
    if header is True:
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)
    logger.debug('Import complete.')
    return df


def gdoc_export(gdoc_url, tab_name, object_name, row=1, col=1, copy_head=True):
    """
    Exports df to gdoc
    warning - share gdoc with: future-cat@future-cat-183920.iam.gserviceaccount.com
    api console - https://console.developers.google.com/apis/dashboard?project=future-cat-183920&folder&organizationId=145261633048&duration=PT1H
    gdoc_url - (str) url of a gdoc
    tab_name - (str) name of tab
    object_name - Pandas object
    row - (int) starting row in gdoc. def = 1
    col - (int) starting col in gdoc. def = 1
    copy_head - (bol) send header with dataframe? def = True
    Example: export_df('jurek_test', 1, x, 1, 1, True)

    """
    logger.debug('Exporting gdoc '+gdoc_url)
    gc = pygsheets.authorize(service_file=g_connect)
    wks = gc.open_by_url(gdoc_url).worksheet_by_title(tab_name)
    wks.set_dataframe(object_name, (row, col), copy_head=copy_head)
    logger.debug('Export complete.')
    return None


def gdoc_append(gdoc_url, tab_name, object_name):
    """
    Appends df to gdoc
    warning - share gdoc with: future-cat@future-cat-183920.iam.gserviceaccount.com
    api console - https://console.developers.google.com/apis/dashboard?project=future-cat-183920&folder&organizationId=145261633048&duration=PT1H
    gdoc_name - (str) name of gdoc in you gdrive
    sheet - (int) number of sheet to open starting from left ie. 1, 2, 3
    object_name - Pandas object to append
    Example: append_gdoc('jurek_test', 1, x)

    """
    logger.debug('Appending gdoc '+gdoc_url)
    gc = pygsheets.authorize(service_file=g_connect)
    wks = gc.open_by_url(gdoc_url).worksheet_by_title(tab_name)
    x = pd.DataFrame(wks.get_values(start=(1, 10000),
                                    end=(10000, 1), returnas='matrix'))
    x.columns = x.iloc[0]
    x = x.iloc[1:].reset_index(drop=True)
    lenght = len(x) + 2
    wks = gc.open_by_url(gdoc_url).worksheet_by_title(tab_name)
    wks.set_dataframe(object_name, (lenght, 1), copy_head=False)
    logger.debug('Append complete.')
    return None


def csv_import(file, path='datasets/'):
    """
    Imports csv
    file    - csv file you want to import
    path    - path to where you store files. Default - 'datasets/'
    Example: df = u.csv_import('df')
    """
    logger.debug('Importing csv '+file)
    df = pd.read_csv(path + file + '.csv', engine='python')
    logger.debug('Import complete.')
    return df


def csv_export(df, file, path='datasets/'):
    """
    Exports csv
    df      - object to be writen
    file    - name of csv file you want to write
    path    - path to where you store files. Default - 'datasets/'
    Example: u.csv_export('df', df)
    """
    logger.debug('Exporting csv '+file)
    pd.DataFrame.to_csv(df, path + str(file) + '.csv',
                        index=False, encoding='utf-8')
    logger.debug('Export complete.')


def xlsx_import(file, tab_name='Sheet1', path='datasets/'):
    """
    Imports xlsx
    file        - xlsx file you want to import
    tab_name    - excel tab_name *duhh...* Default - 'Sheet1'
    path        - path to where you store files. Default - 'datasets/'
    Example:    df = s.xlsx_import('file_name')
    """
    logger.debug('Importing xlsx '+file)
    df = pd.read_excel(path + file + '.xlsx', sheet_name=tab_name)
    logger.debug('Importing complete.')
    return df


def xlsx_export(df, file, tab_name='Sheet1', path='datasets/'):
    """
    Exports csv
    df          - object to be writen
    file        - xlsx file you want to write
    tab_name    - excel tab_name *duhh...*
    path        - path to where you store files. Default - 'datasets/'
    Example:    s.xlsx_export(df, 'file_name')
    """
    logger.debug('Exporting xlsx '+file)
    writer = pd.ExcelWriter(path + file + '.xlsx', engine='xlsxwriter')
    df.to_excel(writer, tab_name, index=False)
    writer.save()
    logger.debug('Export complete.')


def pickle_import(file, path='datasets/'):
    """
    Imports pickle file
    file    - pickle file you want to import
    path    - path to where you store files. Default - 'datasets/'
    Example: df = u.pickle_import('df')
    """
    df = pd.read_pickle(path + file + '.pickle')
    return df


def pickle_export(df, file, path='datasets/'):
    """
    Exports object to pickle
    df      - object to be writen
    file    - name of pickle file you want to write
    path    - path to where you store files. Default - 'datasets/'
    Example: u.pickle_export('df', df)

    """
    pd.DataFrame.to_pickle(df, path + str(file) + '.pickle')
