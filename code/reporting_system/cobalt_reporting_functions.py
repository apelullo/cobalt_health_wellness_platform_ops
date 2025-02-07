#!/usr/bin/env python
# coding: utf-8

#### Cobalt Reporting Functions ####
#### Reporting system function definitions ####

#### Modules ####
from cobalt_reporting_parameters import *

import pandas as pd
import numpy as np

import re
import datetime
from collections import defaultdict

import glob
import os
import psycopg2
from sqlalchemy import create_engine

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns; 

#### Project ####
def proj_init(purge_existing=False):
    dir_dict = {item[0]:item[1] for item in globals().items() if 'PATH' in item[0]}
    for item in dir_dict.items():
        name = item[0]
        value = item[1]
        # Create directories
        if not os.path.exists(value):
            print(f'Creating {name:s} directory at location {value:s}')
            os.mkdir(value)
        # Purge old output - NEEDS TO BE RESTRICTED TO DATA AND OUTPUT FOLDERS, ARCHIVE FOR 30 DAYS THEN DELETE
        elif purge_existing:
            print(f'Purging files in {name:s} directory at location {value:s}')
            for file in glob.glob(value + '*'):
                if os.path.isfile(file):
                    os.remove(file)


#### Database ####
def database_connect():
    # Read-only database connection
    read_conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port= '5432')
    read_conn.autocommit = True
    read_cursor = read_conn.cursor()
    
    # Reporting database connection
    reporting_conn = psycopg2.connect(database=DB_REPORTING_NAME, user=DB_REPORTING_USER, password=DB_REPORTING_PASSWORD, host=DB_REPORTING_HOST, port='5432')
    reporting_conn.autocommit = True
    reporting_cursor = reporting_conn.cursor()
    
    return read_cursor,reporting_cursor

def get_table_data(db_cursor, table_name, limit='ALL'):
    # check user input - allows string or numeric input (may change to string only to simpllify error checking)
    if type(limit) == str: 
        if limit != 'ALL':
            try:
                int(limit)
            except:
                print("Please enter an integer value for limit.")
                return
    elif type(limit) == int:
        limit = str(limit)
    else:
        print("Please enter an integer value for limit.")
        return
    
    query = """SELECT * FROM """ + table_name + """ LIMIT """ + limit + """;"""
    db_cursor.execute(query)
    result = db_cursor.fetchall()
    colnames = [desc[0] for desc in db_cursor.description]
    dataframe = pd.DataFrame(result, columns=colnames)
    
    return dataframe


#### Data Utility ####
def get_date_str(date, formatted=False):
    year = str(date.year)
    month = str(date.month)
    if len(month)==1:
        month = '0'+month
    day = str(date.day)
    if len(day)==1:
        day = '0'+day
        
    if formatted:
        date = year+'/'+month+'/'+day
    else:
        date = year+month+day
    return date

def get_appt_provider_role(row):
    ambiguous_appt_names = ['Initial Visit','Followup Visit','Return Visit']
    ambiguous_appt_names_roles = ['CLINICIAN','PSYCHIATRIST']
    appt_type_name = row['appointment_type_name']
    provider_id = row['provider_id']
    
    if appt_type_name in appt_provider_role_dict.keys():
        appt_role = appt_provider_role_dict[appt_type_name]
    elif appt_type_name in ambiguous_appt_names:
        appt_role = provider_support_role[(provider_support_role['provider_id']==provider_id) & 
                                          (provider_support_role['support_role_id'].isin(ambiguous_appt_names_roles))].support_role_id.values[0]
    else:
        appt_role = 'UNDEFINED'
    
    return appt_role

def get_appt_provider_role_df(data, provider_support_role):    
    mult_role = provider_support_role.groupby(['provider_id']).filter(lambda x: len(x)>1)
    mult_role_provID = mult_role.provider_id.unique()

    appt_single_role = data[~data['provider_id'].isin(mult_role_provID)].copy()
    appt_mult_role = data[data['provider_id'].isin(mult_role_provID)].copy()

    appt_single_role = appt_single_role.merge(provider_support_role, how='inner', left_on='provider_id', right_on='provider_id')
    appt_mult_role['support_role_id'] = appt_mult_role.apply(lambda x: get_appt_provider_role(x), axis=1)

    data = pd.concat([appt_single_role,appt_mult_role]).sort_index()
    return data

def meta_data_init(names,properties):
    def get_meta_template(name):
        template = {key:properties[key]['default'] for key in properties}
        template['name']=name
        return template
    
    meta_data = {name:get_meta_template(name) for name in names}
    return meta_data


#### Data Extraction and Manipulation ####
def get_master_data(table_list, data_dict=dict()):
    # Connect to database
    read_cursor,reporting_cursor = database_connect()
    # List all Cobalt tables
    query = """SELECT table_name FROM information_schema.tables WHERE table_schema='cobalt'"""
    reporting_cursor.execute(query)
    db_tables = [item[0] for item in reporting_cursor.fetchall()]
    
    # Get master data
    for table in table_list:
        data = get_table_data(cursor, table)

def get_db_meta():
    db_meta = {}
    
    # Account data
    get_account_meta(db_meta)
    
    # Provider data
    get_provider_meta(db_meta)
    get_provider_support_role_meta(db_meta)

    # Continue adding functions here...
    
    return db_meta


#### Accounts ####
def get_account(cursor, data_dict=dict()):
    
    # Get account data
    account = get_table_data(cursor, 'account')

    # Adjust columns
    account['year'] = account['created'].dt.year
    account['month'] = account['created'].dt.month
    account['week'] = account['created'].dt.week
    account['day'] = account['created'].dt.day
    account['year_month'] = account['created'].values.astype('datetime64[M]')
    account['year_month_week'] = account['created'].values.astype('datetime64[W]')
    account['dayofyear'] = account['created'].apply(lambda x: get_date_str(x))

    # Filter for relevant data
    account = account[(account['institution_id']=='PENN') & (account['role_id']=='PATIENT')]

    # Time series data
    acct_monthly_ts_data = pd.DataFrame(account.groupby(['year','month']).count().account_id)
    acct_weekly_ts_data = pd.DataFrame(account.groupby(['year_month_week']).count().account_id)
    acct_weekly_ts_data.index = pd.MultiIndex.from_arrays([acct_weekly_ts_data.index.year, 
                                                                acct_weekly_ts_data.index.month, 
                                                                acct_weekly_ts_data.index.day], 
                                                                names=['Year','Month','Week'])

    # Time series data by account source
    acct_src_ts_data = pd.DataFrame(account.groupby(['year','month','account_source_id']).count().account_id)
    acct_src_ts_data = acct_src_ts_data.unstack().fillna(0).account_id

    acct_src_weekly_ts_data = pd.DataFrame(account.groupby(['year_month_week', 'account_source_id']).count()).account_id
    acct_src_weekly_ts_data = acct_src_weekly_ts_data.unstack().fillna(0)
    acct_src_weekly_ts_data.index = pd.MultiIndex.from_arrays([acct_src_weekly_ts_data.index.year, 
                                                          acct_src_weekly_ts_data.index.month, 
                                                          acct_src_weekly_ts_data.index.day], 
                                                          names=['Year','Month','Week'])
    
    data_dict['account'] = account
    data_dict['acct_monthly_ts_data'] = acct_monthly_ts_data
    data_dict['acct_weekly_ts_data'] = acct_weekly_ts_data
    data_dict['acct_src_ts_data'] = acct_src_ts_data
    data_dict['acct_src_weekly_ts_data'] = acct_src_weekly_ts_data
    
    return data_dict

def get_accounts_for_stats(cursor, data_dict=dict()):
    
    # Get account data
    accounts_for_stats = get_table_data(cursor, 'v_accounts_for_stats')

    # Adjust columns
    accounts_for_stats['year'] = accounts_for_stats['created'].dt.year
    accounts_for_stats['month'] = accounts_for_stats['created'].dt.month
    accounts_for_stats['week'] = accounts_for_stats['created'].dt.week
    accounts_for_stats['day'] = accounts_for_stats['created'].dt.day
    accounts_for_stats['year_month'] = accounts_for_stats['created'].values.astype('datetime64[M]')
    accounts_for_stats['year_month_week'] = accounts_for_stats['created'].values.astype('datetime64[W]')
    accounts_for_stats['dayofyear'] = accounts_for_stats['created'].apply(lambda x: get_date_str(x))

    # Filter for relevant data
    accounts_for_stats = accounts_for_stats[(accounts_for_stats['institution_id']=='PENN') & (accounts_for_stats['role_id']=='PATIENT')]

    # Time series data
    acct_stats_ts_data = pd.DataFrame(accounts_for_stats.groupby(['year','month']).count().account_id)
    acct_stats_weekly_ts_data = pd.DataFrame(accounts_for_stats.groupby(['year_month_week']).count().account_id)
    acct_stats_weekly_ts_data.index = pd.MultiIndex.from_arrays([acct_stats_weekly_ts_data.index.year, 
                                                                acct_stats_weekly_ts_data.index.month, 
                                                                acct_stats_weekly_ts_data.index.day], 
                                                                names=['Year','Month','Week'])
    
    data_dict['accounts_for_stats'] = accounts_for_stats
    data_dict['acct_stats_ts_data'] = acct_stats_ts_data
    data_dict['acct_stats_weekly_ts_data'] = acct_stats_weekly_ts_data
    
    return data_dict

def get_account_data(cursor, data_dict=dict()):
    
    data_dict = get_account(cursor=cursor, data_dict=data_dict)
    data_dict = get_accounts_for_stats(cursor=cursor, data_dict=data_dict)
    
    return data_dict

#### Providers ####
def get_provider(cursor):
    
    provider_dict = dict()
    
    return provider_dict

def get_provider_support_role(cursor):
    
    provider_support_role_dict = dict()
    
    return provider_support_role_dict

def get_provider_appointment_type(cursor):
    
    provider_appointment_type_dict = dict()
    
    return provider_appointment_type_dict

def get_provider_data(cursor):
    
    provider_dict = get_provider(cursor=cursor)
    provider_support_role_dict = get_provider_support_role(cursor=cursor)
    provider_appointment_type_dict = get_provider_appointment_type(cursor=cursor)
    
    provider_data = {**provider_dict, **provider_support_role_dict, **provider_appointment_type_dict}
    
    return provider_data

#### Appointments ####
def get_appointment_type(cursor):
    
    appointment_type_dict = dict()
    
    return appointment_type_dict

def get_appointment(cursor):
    
    appointment_dict = dict()
    
    return appointment_dict

def get_provider_availability(cursor):
    
    provider_availability_dict = dict()
    
    return provider_availability_dict

def get_appointment_data(cursor):
    
    appointment_type_dict = get_appointment_type(cursor=cursor)
    appointment_dict = get_appointment(cursor=cursor)
    provider_availability_dict = get_provider_availability(cursor=cursor)
    
    appointment_data = {**appointment_type_dict, **appointment_dict, **provider_availability_dict}
    
    return appointment_data

#### Assessments ####
def get_assessment(cursor):
    
    assessment_dict = dict()
    
    return assessment_dict

def get_assessment_type(cursor):
    
    assessment_type_dict = dict()
    
    return assessment_type_dict

def get_answer(cursor):
    
    answer_dict = dict()
    
    return answer_dict

def get_answer_category(cursor):
    
    answer_category_dict = dict()
    
    return answer_category_dict

def get_category(cursor):
    
    category_dict = dict()
    
    return category_dict

def get_question(cursor):
    
    question_dict = dict()
    
    return question_dict

def get_question_type(cursor):
    
    question_type_dict = dict()
    
    return question_type_dict

def get_assessment_data(cursor):
    
    assessment_dict = get_assessment(cursor=cursor)
    assessment_type_dict = get_assessment_type(cursor=cursor)
    answer_dict = get_answer(cursor=cursor)
    answer_category_dict = get_answer_category(cursor=cursor)
    category_dict = get_category(cursor=cursor)
    question_dict = get_question(cursor=cursor)
    question_type_dict = get_question_type(cursor=cursor)
    
    assessment_data = {**assessment_dict, **assessment_type_dict, **answer_dict, **answer_category_dict, 
                       **category_dict, **question_dict, **question_type_dict}
    
    return assessment_data

#### Engagement ####
def get_content(cursor):
    
    content_dict = dict()
    
    return content_dict

def get_activity_tracking(cursor):
    
    activity_tracking_dict = dict()
    
    return activity_tracking_dict

def get_engagement_data(cursor):
    
    content_dict = get_content(cursor=cursor)
    activity_tracking_dict = get_activity_tracking(cursor=cursor)
    
    engagement_data = {**content_dict, **activity_tracking_dict}
    
    return engagement_data

#### Sessions ####
def get_account_session(cursor):
    
    account_session_dict = dict()
    
    return account_session_dict

def get_account_session_answer(cursor):
    
    account_session_answer_dict = dict()
    
    return account_session_answer_dict

def get_session_data(cursor):
    
    account_session_dict = get_account_session(cursor=cursor)
    account_session_answer_dict = get_account_session_answer(cursor=cursor)
    
    session_data = {**account_session_dict, **account_session_answer_dict}
    
    return session_data

#### Outcomes ####
def get_PHQ9(cursor):
    
    PHQ9_dict = dict()
    
    return PHQ9_dict

def get_GAD7(cursor):
    
    GAD7_dict = dict()
    
    return GAD7_dict

def get_PHQ4(cursor):
    
    PHQ4_dict = dict()
    
    return PHQ4_dict

def get_PCPTSD(cursor):
    
    PCPTSD_dict = dict()
    
    return PCPTSD_dict

def get_outcome_data(cursor):
    
    PHQ9_dict = get_PHQ9(cursor=cursor)
    GAD7_dict = get_GAD7(cursor=cursor)
    PHQ4_dict = get_PHQ4(cursor=cursor)
    PCPTSD_dict = get_PCPTSD(cursor=cursor)
    
    outcome_data = {**PHQ9_dict, **GAD7_dict, **PHQ4_dict, **PCPTSD_dict}
    
    return outcome_data

#### Output ####
def save_data(save_path):
    pass

def get_data(reporting_only=False, save=False, save_path=''):
    cobalt_data = dict()
    
    # Connect to database
    read_cursor,reporting_cursor = database_connect()
    
    account_data = get_account_data(cursor=read_cursor)
    provider_data = get_provider_data(cursor=read_cursor)
    appointment_data = get_appointment_data(cursor=read_cursor)
    assessment_data = get_assessment_data(cursor=read_cursor)
    engagement_data = get_engagement_data(cursor=read_cursor)
    session_data = get_session_data(cursor=read_cursor)
    outcome_data = get_outcome_data(cursor=read_cursor)
    
    cobalt_data = {**account_data, **provider_data, **appointment_data, **assessment_data,
                   **engagement_data, **session_data, **outcome_data}
    
    if save:
        save_data(save_path=save_path)
    
    return cobalt_data

#### Visualization ####
def get_ts_xlabels(index, time): 
    xlabels = []
    if type(index) == pd.MultiIndex:
        if time == 'weekly':
            for item in index:
                year = str(item[0])
                month = str(item[1])
                if len(month) == 1:
                    month = '0' + month
                day = str(item[2])
                if len(day) == 1:
                    day = '0' + day
                xlabels.append(year + '-' + month + '-' + day)
        if time == 'monthly':
            xlabels = [month_dict[item[1]] + '\n' + str(item[0]) for item in index]
    return xlabels

def get_appointment_heatmap(data, apt_type, grouping, date_col, id_col, date_offset=0, save_fig=False, save_path=''):
    day_names = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday',]
    day_names_dict = dict({0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'})
    
    # Set date range and filename
    if date_offset < 0: # past
        data = data[(data[date_col] >= pd.to_datetime(datetime.datetime.now() + pd.Timedelta(days=date_offset)).tz_localize(tz='US/Eastern')) & 
                    (data[date_col] <= pd.to_datetime(datetime.datetime.now()).tz_localize(tz='US/Eastern'))]
        filename = apt_type + '_appointment_' + grouping + '_last' + str(abs(date_offset)) + 'days_'
        offset_title = 'Last ' + str(abs(date_offset)) + ' Days'
    elif date_offset > 0: # future
        data = data[(data[date_col] <= pd.to_datetime(datetime.datetime.now() + pd.Timedelta(days=date_offset)).tz_localize(tz='US/Eastern')) & 
                    (data[date_col] >= pd.to_datetime(datetime.datetime.now()).tz_localize(tz='US/Eastern'))]
        filename = apt_type + '_appointment_' + grouping + '_next' + str(date_offset) + 'days_'
        offset_title = 'Next ' + str(date_offset) + ' Days'
    else: 
        filename = apt_type + '_appointment_' + grouping + '_allTime_'
        offset_title = 'All Time'
    
    # Prep figure data and labels
    data = data.groupby([grouping,'support_role_id']).count()[id_col]
    data = data.unstack().fillna(0)
    data = data.transpose()
    
    if grouping == 'dayofweek':
        data.columns = [day_names_dict[item] for item in data.columns]
        grouping_title = 'Day of Week'
    elif grouping == 'hourofday':
        grouping_title = 'Hour of Day'
        
    # Plot
    fig, ax = plt.subplots(figsize=(16,8)) 
    sns.heatmap(data, annot=True, linewidths=.5, ax=ax)
    ax.set_title(apt_type.capitalize() + ' Appointments by ' + grouping_title + ': ' + offset_title, fontsize=18)
    
    if save_fig:
        save_figure(fig, save_path, filename)
        
    return

def save_figure(fig, path, filename):
    name = path + filename + str(datetime.datetime.now().date()).replace('-','') + '.png'
    fig.savefig(name, bbox_inches='tight', pad_inches=0, dpi=SAVE_DPI, transparent=True)
    
    return