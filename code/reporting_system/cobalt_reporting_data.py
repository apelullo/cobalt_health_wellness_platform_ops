#!/usr/bin/env python
# coding: utf-8

#### Cobalt Reporting Data ####

#### Modules ####
from cobalt_reporting_functions import *

import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
from scipy import stats

import re
import copy
import datetime
from collections import defaultdict
import itertools
from itertools import cycle

import glob
import os

import psycopg2
from sqlalchemy import create_engine

# Pandas view options
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 200)
pd.set_option('precision', 4)


#### Program Parameters ####

#### Master Data ####
# Connect to database
read_cursor,reporting_cursor = database_connect()

# List all Cobalt tables
query = """SELECT table_name FROM information_schema.tables WHERE table_schema='cobalt'"""
reporting_cursor.execute(query)
result = reporting_cursor.fetchall()

print(len(result))
sorted(result)


#### Accounts ####
# * Note 08/25/2021: 
#     * How many employees at Penn?
#     * Are all employees eligible?
#     * Other groups (students) eligible in future?

#### Account ####
# Get account data
account = get_table_data(reporting_cursor, 'account')

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
account = account[account['phone_number']!='+12157777777'] # added 11/10/2021

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

# Save master data
account.to_csv(MASTER_DATA_PATH + 'account_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
acct_monthly_ts_data.to_csv(MASTER_DATA_PATH + 'acct_monthly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
acct_src_ts_data.to_csv(MASTER_DATA_PATH + 'acct_src_monthly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
print(len(account))
account.head(2)


#### Accounts for Stats ####
# Get accounts for stats data
accounts_for_stats = get_table_data(reporting_cursor, 'v_accounts_for_stats')

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

# Save master data
accounts_for_stats.to_csv(MASTER_DATA_PATH + 'accounts_for_stats_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
accounts_for_stats.head(2)


#### Providers ####
# * NOTE 08/31/2021: Removing providers with system affinity id PIC changes counts of future appts and appt availability
#     * Removing these providers also drastically changes the historic availability counts 4.4k --> 2.7k
#     * Why is this? and do we still want to remove these providers?

#### Provider ####
# Get provider data
provider = get_table_data(reporting_cursor, 'provider')

# Adjust columns
provider['year'] = provider['created'].dt.year
provider['month'] = provider['created'].dt.month
provider['day'] = provider['created'].dt.day
provider['year_month'] = provider['created'].values.astype('datetime64[M]')
provider['year_month_week'] = provider['created'].values.astype('datetime64[W]')
provider['dayofyear'] = provider['created'].apply(lambda x: get_date_str(x))

# Filter for relevant data
provider = provider[provider['institution_id']=='PENN']
provider = provider[provider['system_affinity_id'] != 'PIC']

# Time series data
prov_ts_data = pd.DataFrame(provider.groupby(['year','month']).count().provider_id)
prov_ts_data = prov_ts_data.merge(month_index_df, how='outer', left_index=True, right_index=True) # Get missing index values
prov_ts_data = pd.DataFrame(prov_ts_data.fillna(0)['provider_id'])

prov_weekly_ts_data = pd.DataFrame(provider.groupby(['year_month_week']).count().provider_id)
prov_weekly_ts_data.index = pd.MultiIndex.from_arrays([prov_weekly_ts_data.index.year, 
                                                      prov_weekly_ts_data.index.month, 
                                                      prov_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])
prov_weekly_ts_data = prov_weekly_ts_data.merge(week_index_df, how='outer', left_index=True, right_index=True)# Get missing index values
prov_weekly_ts_data = pd.DataFrame(prov_weekly_ts_data.fillna(0)['provider_id'])

# Save master data
provider.to_csv(MASTER_DATA_PATH + 'provider_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
provider.head(2)


#### Provider Support Role ####

# Get provider role data
provider_support_role = get_table_data(reporting_cursor, 'provider_support_role')

# Filter for relevant data
provider_support_role = provider_support_role[provider_support_role['provider_id'].isin(provider['provider_id'])]

# Save master data
provider_support_role.to_csv(MASTER_DATA_PATH + 'provider_role_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
provider_support_role.head(2)


#### Provider Appointment Type ####

# Get provider appointment type data
provider_appointment_type = get_table_data(reporting_cursor, 'provider_appointment_type')

# Filter for relevant data
provider_appointment_type = provider_appointment_type[provider_appointment_type['provider_id'].isin(provider['provider_id'])]

# Display adjusted data
provider_appointment_type.head(2)


#### Appointments ####

#### Appointment Type ####
# Get appointment type data
appointment_type = get_table_data(reporting_cursor, 'appointment_type')

appointment_type_dict = dict(zip(appointment_type.appointment_type_id, appointment_type.name))

# Display adjusted data
appointment_type.head(2)


#### Booked Appointments ####
# Get appointment data
appointment = get_table_data(reporting_cursor, 'appointment')

# Adjust columns
appointment['start_time'] = appointment['start_time'].dt.tz_localize(tz='US/Eastern')
appointment['created'] = appointment['created'].dt.tz_convert(tz='US/Eastern')

appointment['created_year'] = appointment['created'].dt.year
appointment['created_month'] = appointment['created'].dt.month
appointment['created_day'] = appointment['created'].dt.day
appointment['created_year_month'] = appointment['created'].values.astype('datetime64[M]')
appointment['created_year_month_week'] = appointment['created'].values.astype('datetime64[W]')
appointment['created_dayofyear'] = appointment['created'].apply(lambda x: get_date_str(x))

appointment['apt_year'] = appointment['start_time'].dt.year
appointment['apt_month'] = appointment['start_time'].dt.month
appointment['apt_day'] = appointment['start_time'].dt.day
appointment['apt_year_month'] = appointment['start_time'].values.astype('datetime64[M]')
appointment['apt_year_month_week'] = appointment['start_time'].values.astype('datetime64[W]')
appointment['apt_dayofyear'] = appointment['start_time'].apply(lambda x: get_date_str(x))

appointment['created_completed_time']=((appointment['start_time'] - appointment['created'])/np.timedelta64(1, 'D')).round()
appointment['appointment_type_name'] = appointment['appointment_type_id'].map(appointment_type_dict)
appointment = get_appt_provider_role_df(appointment, provider_support_role)

# Filter for relevant data
appointment = appointment[appointment['created'] < appointment['start_time']]
appointment = appointment.groupby(['account_id']).filter(lambda x: len(x)<50)
appointment = appointment[appointment['account_id'].isin(account['account_id'])]
appointment = appointment[appointment['provider_id'].isin(provider['provider_id'])]
appointment = appointment[appointment['acuity_class_id'].isnull()]

# Appointment Subsets
appointment_completed = appointment[appointment['canceled']==False]
appointment_canceled = appointment[appointment['canceled']==True]

appointment_future = appointment[appointment['start_time'] >= current_date]
appointment_future_30day = appointment_future[appointment_future['start_time'] <= future_30day]
appointment_future_90day = appointment_future[appointment_future['start_time'] <= future_90day]

appointment_past = appointment[appointment['start_time'] <= current_date]
appointment_past_30day = appointment_past[appointment_past['start_time'] >= past_30day]
appointment_past_90day = appointment_past[appointment_past['start_time'] >= past_90day]

# Time series data
apt_ts_data = pd.DataFrame(appointment.groupby(['apt_year','apt_month']).count().appointment_id)
apt_ts_data.index.names = ['year','month']
apt_weekly_ts_data = pd.DataFrame(appointment.groupby(['apt_year_month_week']).count().appointment_id)
apt_weekly_ts_data.index = pd.MultiIndex.from_arrays([apt_weekly_ts_data.index.year, 
                                                      apt_weekly_ts_data.index.month, 
                                                      apt_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])

apt_completed_ts_data = pd.DataFrame(appointment_completed.groupby(['apt_year','apt_month']).count().appointment_id)
apt_completed_ts_data.index.names = ['year','month']
apt_weekly_completed_ts_data = pd.DataFrame(appointment_completed.groupby(['apt_year_month_week']).count().appointment_id)
apt_weekly_completed_ts_data.index = pd.MultiIndex.from_arrays([apt_weekly_completed_ts_data.index.year, 
                                                                apt_weekly_completed_ts_data.index.month, 
                                                                apt_weekly_completed_ts_data.index.day],
                                                                names=['Year','Month','Week'])

apt_canceled_ts_data = pd.DataFrame(appointment_canceled.groupby(['apt_year','apt_month']).count().appointment_id)
apt_canceled_ts_data.index.names = ['year','month']
apt_weekly_canceled_ts_data = pd.DataFrame(appointment_canceled.groupby(['apt_year_month_week']).count().appointment_id)
apt_weekly_canceled_ts_data.index = pd.MultiIndex.from_arrays([apt_weekly_canceled_ts_data.index.year, 
                                                                apt_weekly_canceled_ts_data.index.month, 
                                                                apt_weekly_canceled_ts_data.index.day],
                                                                names=['Year','Month','Week'])

# Time series data by provider role
aptRole_ts_data = pd.DataFrame(appointment.groupby(['apt_year','apt_month','support_role_id']).count().appointment_id)
aptRole_ts_data = aptRole_ts_data.unstack().fillna(0).appointment_id
aptRole_ts_data.index.names = ['year','month']

aptRole_weekly_ts_data = pd.DataFrame(appointment.groupby(['apt_year_month_week', 'support_role_id']).count()).appointment_id
aptRole_weekly_ts_data = aptRole_weekly_ts_data.unstack().fillna(0)
aptRole_weekly_ts_data.index = pd.MultiIndex.from_arrays([aptRole_weekly_ts_data.index.year, 
                                                      aptRole_weekly_ts_data.index.month, 
                                                      aptRole_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])

# Save master data
appointment.to_csv(MASTER_DATA_PATH + 'appointment_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
apt_ts_data.to_csv(MASTER_DATA_PATH + 'appt_monthly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
aptRole_ts_data.to_csv(MASTER_DATA_PATH + 'appt_role_monthly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
appointment.head(2)

# Display adjusted future data
appointment_future.head(2)

# Display adjusted past data
appointment_past.head(2)


#### Available Appointments ####
# Get provider availability data
provider_availability = get_table_data(reporting_cursor, 'provider_availability')

# Adjust columns
provider_availability['date_time'] = provider_availability['date_time'].dt.tz_localize(tz='US/Eastern')
provider_availability['created'] = provider_availability['created'].dt.tz_convert(tz='US/Eastern')

provider_availability['created_year'] = provider_availability['created'].dt.year
provider_availability['created_month'] = provider_availability['created'].dt.month
provider_availability['created_day'] = provider_availability['created'].dt.day
provider_availability['created_year_month'] = provider_availability['created'].values.astype('datetime64[M]')
provider_availability['created_year_month_week'] = provider_availability['created'].values.astype('datetime64[W]')
provider_availability['created_dayofyear'] = provider_availability['created'].apply(lambda x: get_date_str(x))

provider_availability['apt_year'] = provider_availability['date_time'].dt.year
provider_availability['apt_month'] = provider_availability['date_time'].dt.month
provider_availability['apt_day'] = provider_availability['date_time'].dt.day
provider_availability['apt_year_month'] = provider_availability['date_time'].values.astype('datetime64[M]')
provider_availability['apt_year_month_week'] = provider_availability['date_time'].values.astype('datetime64[W]')
provider_availability['apt_dayofyear'] = provider_availability['date_time'].apply(lambda x: get_date_str(x))

provider_availability['appointment_type_name'] = provider_availability['appointment_type_id'].map(appointment_type_dict)
provider_availability = get_appt_provider_role_df(provider_availability, provider_support_role)

# Filter for relevant data
provider_availability = provider_availability[provider_availability['provider_id'].isin(provider['provider_id'])]

# Provider availability subsets
provider_availability_future = provider_availability[provider_availability['date_time'] >= current_date]
provider_availability_future_30day = provider_availability_future[provider_availability_future['date_time'] <= future_30day]
provider_availability_future_90day = provider_availability_future[provider_availability_future['date_time'] <= future_90day]

provider_availability_past = provider_availability[provider_availability['date_time'] <= current_date]
provider_availability_past_30day = provider_availability_past[provider_availability_past['date_time'] >= past_30day]
provider_availability_past_90day = provider_availability_past[provider_availability_past['date_time'] >= past_90day]

# Time series data
apt_avail_ts_data = pd.DataFrame(provider_availability.groupby(['apt_year','apt_month']).count().provider_availability_id)
apt_avail_ts_data.index.names = ['year','month']
apt_avail_weekly_ts_data = pd.DataFrame(provider_availability.groupby(['apt_year_month_week']).count().provider_availability_id)
apt_avail_weekly_ts_data.index = pd.MultiIndex.from_arrays([apt_avail_weekly_ts_data.index.year, 
                                                            apt_avail_weekly_ts_data.index.month, 
                                                            apt_avail_weekly_ts_data.index.day], 
                                                            names=['Year','Month','Week'])

# Time series data by provider role
aptRole_avail_ts_data = pd.DataFrame(provider_availability.groupby(['apt_year','apt_month','support_role_id']).count().provider_availability_id)
aptRole_avail_ts_data = aptRole_avail_ts_data.unstack().fillna(0).provider_availability_id
aptRole_avail_ts_data.index.names = ['year','month']

aptRole_avail_weekly_ts_data = pd.DataFrame(provider_availability.groupby(['apt_year_month_week', 'support_role_id']).count()).provider_availability_id
aptRole_avail_weekly_ts_data = aptRole_avail_weekly_ts_data.unstack().fillna(0)
aptRole_avail_weekly_ts_data.index = pd.MultiIndex.from_arrays([aptRole_avail_weekly_ts_data.index.year, 
                                                      aptRole_avail_weekly_ts_data.index.month, 
                                                      aptRole_avail_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])

# Save master data
provider_availability.to_csv(MASTER_DATA_PATH + 'provider_availability_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
provider_availability.head(2)

# Display adjusted future data
provider_availability_future.head(2)

# Display adjusted past data
provider_availability_past.head(2)


#### Group Sessions ####

#### Group Session ####
# Get group session data
group_session = get_table_data(reporting_cursor, 'group_session')

# Display adjusted data
group_session.head(2)


#### Group Session Requests ####
# Get group session request data
group_session_request = get_table_data(reporting_cursor, 'group_session_request')

# Display adjusted data
group_session_request.head(2)


#### Group Session Reservations ####
# Get group session reservation data
group_session_reservation = get_table_data(reporting_cursor, 'group_session_reservation')

# Display adjusted data
group_session_reservation.head(2)


#### Assessments ####

#### Assessment ####
# Get assessment data
assessment = get_table_data(reporting_cursor, 'assessment')
assessment_dict = dict(zip(assessment.assessment_id, assessment.assessment_type_id))

# Assessment IDs
PHQ4_id = assessment[assessment['assessment_type_id']=='PHQ4'].assessment_id.values[0]
PHQ9_id = assessment[assessment['assessment_type_id']=='PHQ9'].assessment_id.values[0]
GAD7_id = assessment[assessment['assessment_type_id']=='GAD7'].assessment_id.values[0]
PCPTSD_id = assessment[assessment['assessment_type_id']=='PCPTSD'].assessment_id.values[0]
RCT_ids = assessment[(assessment['assessment_type_id']=='PHQ9') | (assessment['assessment_type_id']=='GAD7')].assessment_id.values

# Save master data
assessment.to_csv(MASTER_DATA_PATH + 'assessment_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
assessment.head(2)


#### Assessment Type ####
# Get assessment type data
assessment_type = get_table_data(reporting_cursor, 'assessment_type')

# Save master data
assessment_type.to_csv(MASTER_DATA_PATH + 'assessment_type_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
assessment_type.head(2)


#### Answer ####
# Get answer data
answer = get_table_data(reporting_cursor, 'answer')

# Save master data
answer.to_csv(MASTER_DATA_PATH + 'answer_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
answer.head(2)


#### Answer Category ####
# Get answer category data
answer_category = get_table_data(reporting_cursor, 'answer_category')

# Save master data
answer_category.to_csv(MASTER_DATA_PATH + 'answer_category_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
answer_category.head(2)


#### Category ####
# Get category data
category = get_table_data(reporting_cursor, 'category')

# Save master data
category.to_csv(MASTER_DATA_PATH + 'category_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
category.head(2)


#### Question ####
## Overlapping phq4 questions excluded from phq9 and gad7
# Get question data
question = get_table_data(reporting_cursor, 'question')
question_text_dict = dict(zip(question.question_id, question.question_text))

# Adjust columns
question['assessment_name'] = question['assessment_id'].map(assessment_dict)

# Get corrected assessment question lists
PHQ4_questions = question[question['assessment_id']==PHQ4_id].question_id.values

PHQ9_q1q2 = question[question['assessment_id']==PHQ4_id].loc[[20,19],'question_id'].to_list()
PHQ9_q3q9 = question[question['assessment_id']==PHQ9_id].question_id.to_list()
PHQ9_questions = PHQ9_q1q2 + PHQ9_q3q9

GAD7_q1q2 = question[question['assessment_id']==PHQ4_id].loc[[17,18],'question_id'].to_list()
GAD7_q3q7 = question[question['assessment_id']==GAD7_id].question_id.to_list()
GAD7_questions = GAD7_q1q2 + GAD7_q3q7

PCPTSD_questions = question[question['assessment_id']==PCPTSD_id].question_id.values

RCT_questions = PHQ9_questions + GAD7_questions

# Save master data
question.to_csv(MASTER_DATA_PATH + 'question_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
question.head(2)


#### Question Type ####
# Get question type data
question_type = get_table_data(reporting_cursor, 'question_type')

# Save master data
question_type.to_csv(MASTER_DATA_PATH + 'question_type_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
question_type.head(2)


#### Engagment ####

#### Content ####
# Get content data
content = get_table_data(reporting_cursor, 'content')

# Filter for relevant data
content = content[content['owner_institution_id']=='PENN']

# Save master data
content.to_csv(MASTER_DATA_PATH + 'content_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
content.head(2)


#### Activity Tracking ####
## To find the content users are consuming: activity_tracking.activity_key --> content.content_id (DEPRECATED - 01/11/2022)
## content[content['content_id']=='0e997dda-15e1-446c-bcda-bea3a2271c60']
# Get activity tracking data
activity_tracking = get_table_data(reporting_cursor, 'activity_tracking')

# Filter for relevant data
activity_tracking = activity_tracking[activity_tracking['account_id'].isin(account['account_id'])]

# Activity tracking subsets
activity_tracking_past = activity_tracking[activity_tracking['created'] <= current_date]
activity_tracking_past_30day = activity_tracking_past[activity_tracking_past['created'] >= past_30day]
activity_tracking_past_90day = activity_tracking_past[activity_tracking_past['created'] >= past_90day]

# Save master data
activity_tracking.to_csv(MASTER_DATA_PATH + 'activity_tracking_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
activity_tracking.head(2)


#### Popular Content ####
## Ordered by view_count for last 30 days
popular_content_use_cols = ['content_id','created','content_type_id','title','description','author','duration_in_minutes','view_count']

# Get content activity for last 30 days
content_activity_past_30day = activity_tracking_past_30day[activity_tracking_past_30day['activity_type_id']=='CONTENT'].copy()
content_activity_past_30day.loc[:,'context'] = content_activity_past_30day['context'].apply(lambda x: x['contentId']).copy()

# Get content view counts
popular_content_past_30day = content_activity_past_30day.groupby(['context'])[['activity_tracking_id']].count()
popular_content_past_30day = popular_content_past_30day.rename(columns={'activity_tracking_id':'view_count'})
popular_content_past_30day = content.merge(popular_content_past_30day, how='inner', left_on='content_id', right_index=True)

# popular content
popular_content_past_30day = popular_content_past_30day.sort_values(['view_count'], ascending=False)[popular_content_use_cols].reset_index(drop=True)

# Save 30-day popular content
popular_content_past_30day.to_csv(CHART_PATH + 'popular_content_past_30day_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# popular content grouped by type
popular_content_past_30day_grouped = popular_content_past_30day.sort_values(['content_type_id','view_count'], ascending=False)[popular_content_use_cols].reset_index(drop=True)

# Save 30-day popular content grouped by type
popular_content_past_30day_grouped.to_csv(CHART_PATH + 'popular_content_past_30day_grouped_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
popular_content_past_30day.head(2)

# Display adjuted grouped data
popular_content_past_30day_grouped.head(2)


#### Popular Content Summary
# get popular content summary
popular_content_past_30day_summary = pd.concat([popular_content_past_30day.groupby(['content_type_id'])['view_count'].count(),
                                     popular_content_past_30day.groupby(['content_type_id'])['view_count'].sum()], axis=1)
popular_content_past_30day_summary.columns = ['content_count', 'view_count']
popular_content_past_30day_summary['views_per_content'] = popular_content_past_30day_summary['view_count']/popular_content_past_30day_summary['content_count']

# Save popular content summary
popular_content_past_30day_summary.to_csv(CHART_PATH + 'popular_content_past_30day_summary' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Dispay adjusted data
popular_content_past_30day_summary.head()


#### Sessions ####

#### Account Session ####
# Get account session data
account_session = get_table_data(reporting_cursor, 'account_session')

# Filter for relevant data
account_session = account_session[account_session['account_id'].isin(account['account_id'])]

# Adjust columns
account_session['assessment_name'] = account_session['assessment_id'].map(assessment_dict)

# Save master data
account_session.to_csv(MASTER_DATA_PATH + 'account_session_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
account_session.head(2)


#### Account Session Answer ####
# Get account session answer data
account_session_answer = get_table_data(reporting_cursor, 'account_session_answer')

# Merge account session and account session answer on account_session_id
account_session_answer = account_session_answer.merge(account_session, how='inner', 
                                                             left_on='account_session_id', 
                                                             right_on='account_session_id',
                                                             suffixes=['_session_answer','_session'])
use_cols = ['account_session_answer_id','account_session_id','account_id','assessment_id','answer_id',
            'complete_flag','created_session_answer','created_session']
account_session_answer = account_session_answer[use_cols]
account_session_answer['assessment_name'] = account_session_answer['assessment_id'].map(assessment_dict)

# Merge account session answer and account on account_id
use_cols = ['account_id','account_source_id','sso_id','first_name','last_name','email_address','phone_number','created']
account_merge = account[use_cols].copy()
account_merge = account_merge.rename(columns={'created':'created_account'})
account_session_answer = account_session_answer.merge(account_merge,how='inner',left_on='account_id',right_on='account_id')

# Merge account session answer and answer on answer_id
use_cols = ['answer_id','question_id','answer_text','display_order','answer_value','crisis','call']
answer_merge = answer[use_cols].copy()
account_session_answer = account_session_answer.merge(answer_merge,how='inner',left_on='answer_id', right_on='answer_id')

# Save master data
account_session_answer.to_csv(MASTER_DATA_PATH + 'account_session_answer_master_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Display adjusted data
account_session_answer.head(2)


#### Derived Data ####
#### Account Instances ####
# * An **account instance** represents the set of *linked* account **session outcomes** associated with an **escalation decision**. 
#     * Instances may be comprised the following session outcomes: PHQ4, PHQ9, GAD7, and PCPTSD scores
#     * A complete instance has scores for all required session outcomes, according the to escalation logic
#         * Complete instances will not all have the same number of session outcomes
#             * Example 1: A patient scoring mild on the PHQ4 completes the instance and is assigned an escalation decision with just one session outcome score
#             * Example 2: A patient scoring severe on the PHQ4 must continue the session chain with additional assessments to complete the instance and be assigned an escalation decision
#         * Complete instances are assigned an **escalation decision** and a date that the escalation decision was made
#     * An incomplete instance has scores for >=0 session outcomes, but has not completed all required asssessments according to the escalation logic
#         * Incomplete instances may have >=0 individual session outcomes
#             * Example 1: A patient may start the PHQ4 but not finish - this patient instance has no session outcomes and the escalation decision is inconclusive
#             * Example 2: A patient finishes the PHQ4 with a severe score but does not complete any additional sessions required by the escalation logic - this patient instance has one session outcome but the *escalation decision is inconclusive*
#         * Individual session outcomes may be used for aggregate analysis but will not be used for calculation of escalation metrics

#### Define Instance Parameters ####
def get_escalation(instance_session_outcomes):
    START_SESSION_score = instance_session_outcomes[INSTANCE_START_SESSION]['score']
    PHQ4_score = instance_session_outcomes['PHQ4']['score']
    PHQ9_score = instance_session_outcomes['PHQ9']['score']
    GAD7_score = instance_session_outcomes['GAD7']['score']
    PCPTSD_score = instance_session_outcomes['PCPTSD']['score']
    
    if START_SESSION_score < START_SESSION_SCORE_THRESHOLD:
        escalation = 'mild'
        escalation_provider = 'resilience_coach'
    else:
        if PHQ9_score<10 and GAD7_score<10 and PCPTSD_score<3:
            escalation = 'mild'
            escalation_provider = 'resilience_coach'
        elif PHQ9_score>19 or GAD7_score>19:
            escalation = 'severe'
            escalation_provider = 'psychiatrist'
        else:
            escalation = 'moderate'
            escalation_provider = 'psychotherapist'
    
    return escalation,escalation_provider

INSTANCE_START_SESSION = 'PHQ4'
START_SESSION_SCORE_THRESHOLD = 3

PHQ4_MAX = 12
PHQ9_MAX = 27
GAD7_MAX = 21
PCPTSD_MAX = 5

PHQ4_SCORE_LABELS = ['no_symptoms']*3 + ['mild']*3 + ['moderate']*3 + ['severe']*4
PHQ9_SCORE_LABELS = ['no_symptoms']*5 + ['mild']*5 + ['moderate']*5 + ['moderately_severe']*5 + ['severe']*8
GAD7_SCORE_LABELS = ['no_symptoms']*5 + ['mild']*5 + ['moderate']*5 + ['severe']*7
PCPTSD_SCORE_LABELS = ['no_symptoms']*3 + ['followup_sensitivity']*1 + ['followup_efficiency']*2

assessment_severity_dict = {'PHQ4':{score:label for score,label in zip(range(PHQ4_MAX+1),PHQ4_SCORE_LABELS)},
                           'PHQ9':{score:label for score,label in zip(range(PHQ9_MAX+1),PHQ9_SCORE_LABELS)},
                           'GAD7':{score:label for score,label in zip(range(GAD7_MAX+1),GAD7_SCORE_LABELS)},
                           'PCPTSD':{score:label for score,label in zip(range(PCPTSD_MAX+1),PCPTSD_SCORE_LABELS)}}

# Instance parameters (subject to change - will go in config/param file eventually)
instance_session_names = ['PHQ4','PHQ9','GAD7','PCPTSD']
instance_session_ids = [PHQ4_id,PHQ9_id,GAD7_id,PCPTSD_id]
instance_session_questions = [PHQ4_questions,PHQ9_questions,GAD7_questions,PCPTSD_questions]
instance_session_meta = {'PHQ4':{'assessment_id':PHQ4_id,'questions':PHQ4_questions},
                         'PHQ9':{'assessment_id':PHQ9_id,'questions':PHQ9_questions,
                                 'initial_questions':PHQ9_q1q2,'continuing_questions':PHQ9_q3q9},
                         'GAD7':{'assessment_id':GAD7_id,'questions':GAD7_questions,
                                 'initial_questions':GAD7_q1q2,'continuing_questions':GAD7_q3q7},
                         'PCPTSD':{'assessment_id':PCPTSD_id,'questions':PCPTSD_questions}}

instance_session_outcomes_template = {'PHQ4':{'attempts':0,'complete':0,'score':-1,'severity':'inconclusive',
                                              'PHQ9_data':pd.DataFrame(columns=account_session_answer.columns),
                                              'GAD7_data':pd.DataFrame(columns=account_session_answer.columns)},
                                      'PHQ9':{'attempts':0,'complete':0,'score':-1,'severity':'inconclusive'},
                                      'GAD7':{'attempts':0,'complete':0,'score':-1,'severity':'inconclusive'},
                                      'PCPTSD':{'attempts':0,'complete':0,'score':-1,'severity':'inconclusive',}}

# Data columns (subject to change - will go in config/param file eventually)
account_cols = ['account_id','num_instances','num_sessions'] # add to account table
account_instance_cols = ['instance_id','account_id','num_instance_sessions', # cols for NEW account_instance table
                         'PHQ4_attempts','PHQ9_attempts','GAD7_attempts','PCPTSD_attempts',
                         'PHQ4_complete','PHQ9_complete','GAD7_complete','PCPTSD_complete',
                         'PHQ4_score','PHQ9_score','GAD7_score','PCPTSD_score',
                         'PHQ4_severity','PHQ9_severity','GAD7_severity','PCPTSD_severity',
                         'crisis','crisis_text','crisis_value','instance_complete','escalation','escalation_provider',
                         'start_time','complete_time','last_updated']
account_instance_session_cols = ['account_session_id','instance_id','account_id', # add to account_session table
                                 'assessment_id','assessment_name','num_questions','num_questions_answered',
                                 'complete_flag','created','outcome_complete','score','severity']


#### Define Instance Datasets ####

#### Account Instance Sessions ####
account_instance_sessions = account_session.copy()
account_instance_sessions = account_instance_sessions[account_instance_sessions['assessment_id'].isin(instance_session_ids)]

# Display account instance sessions
print(len(account_instance_sessions))
account_instance_sessions.head(2)


#### Account Instance Session Answers
account_instance_session_answers = account_session_answer.copy()
account_instance_session_answers = account_instance_session_answers[account_instance_session_answers['assessment_id'].isin(instance_session_ids)]

# Display account instance session answers
print(len(account_instance_session_answers))
account_instance_session_answers.head(2)


#### Extract Instance Data ####
account_data = [] # new columns
account_instance_data = [] # new dataframe
account_instance_session_data = [] # new columns
account_session_anomaly_data = []
count=0

## Get account-level info ##
for account_id,sessions in account_instance_sessions.groupby(['account_id']):
    sessions = sessions.sort_values(['created'])
    sessions = sessions.reset_index()
    num_sessions = len(sessions)
    
    # Find unique instances via starting session assessment
    instance_start = sessions[sessions['assessment_name']==INSTANCE_START_SESSION]
    instance_start_idx = instance_start.index.to_list() + [None]
    instance_start_idx_ranges = [[instance_start_idx[i],instance_start_idx[i+1]] for i in range(len(instance_start_idx)-1)]
    num_instances = len(instance_start)
    
    ## Get instance-level info ##
    for instance_idx_range in instance_start_idx_ranges:
        instance = sessions.iloc[instance_idx_range[0]:instance_idx_range[1],:]
        
        # Set static instance values
        instance_id = account_id + '_' + str(instance_idx_range[0])
        num_instance_sessions = len(instance)
        start_time = instance.iloc[0].created
        last_updated = instance.iloc[-1].created
        
        # Initialize conditional instance values
        instance_session_outcomes = copy.deepcopy(instance_session_outcomes_template)
        crisis = 0
        crisis_text = ''
        crisis_value = 0
        instance_complete = 0
        escalation = 'inconclusive'
        escalation_provider = 'inconclusive'
        complete_time = pd.NaT
    
        ## Get session-level info ##
        for idx,session in instance.iterrows():
            # Set static session values
            account_session_id = session.account_session_id
            assessment_id = session.assessment_id
            assessment_name = session.assessment_name
            assessment_questions = instance_session_meta[assessment_name]['questions']
            num_questions = len(assessment_questions)
            complete_flag = session.complete_flag
            created = session.created
            
            # Get session answers with session_id
            session_answers = account_instance_session_answers[account_instance_session_answers['account_session_id'] == account_session_id]
            session_answers = session_answers.drop_duplicates(['account_session_answer_id'])
            
            # Use PHQ4 answers to update PHQ9 and GAD7 answers
            if assessment_name == 'PHQ4':
                instance_session_outcomes['PHQ4']['PHQ9_data'] = session_answers[session_answers['question_id'].isin(instance_session_meta['PHQ9']['questions'])]
                instance_session_outcomes['PHQ4']['GAD7_data'] = session_answers[session_answers['question_id'].isin(instance_session_meta['GAD7']['questions'])]
            if assessment_name == 'PHQ9':
                session_answers = pd.concat([instance_session_outcomes['PHQ4']['PHQ9_data'],session_answers])
                if len(session_answers[session_answers['crisis']==True]) > 0: # check q9 answer
                    crisis = 1
                    crisis_text = session_answers[session_answers['crisis']==True]['answer_text'].values[0]
                    crisis_value = session_answers[session_answers['crisis']==True]['answer_value'].values[0]
            if assessment_name == 'GAD7':
                session_answers = pd.concat([instance_session_outcomes['PHQ4']['GAD7_data'],session_answers])
            
            # check for complete session and update values
            num_questions_answered = len(session_answers)
            instance_session_outcomes[assessment_name]['attempts'] += 1
            if sorted(session_answers.question_id.to_list()) == sorted(assessment_questions):
                # Check for duplicate completed assessment ***within the same instance***
                if instance_session_outcomes[assessment_name]['complete'] == 1:
                    print('Assessment', assessment_name, 'is already complete! Saving session info for further evaluation.')
                    account_session_anomaly_row = dict()
                    account_session_anomaly_row['account_id'] = account_id
                    account_session_anomaly_row['account_session_id'] = account_session_id
                    account_session_anomaly_row['assessment_name'] = assessment_name
                    account_session_anomaly_row['complete_flag'] = complete_flag
                    account_session_anomaly_row['created'] = created
                    account_session_anomaly_data.append(account_session_anomaly_row)
                else:
                    num_questions_answered = num_questions
                    instance_session_outcomes[assessment_name]['complete'] = 1
                    instance_session_outcomes[assessment_name]['score'] = session_answers.answer_value.sum()
                    instance_session_outcomes[assessment_name]['severity'] = assessment_severity_dict[assessment_name][instance_session_outcomes[assessment_name]['score']]
            
            # new account instance session row
            account_instance_session_row = dict()
            account_instance_session_row['account_session_id'] = account_session_id
            account_instance_session_row['instance_id'] = instance_id
            account_instance_session_row['account_id'] = account_id
            account_instance_session_row['assessment_id'] = assessment_id
            account_instance_session_row['assessment_name'] = assessment_name
            account_instance_session_row['num_questions'] = num_questions
            account_instance_session_row['num_questions_answered'] = num_questions_answered
            account_instance_session_row['complete_flag'] = complete_flag
            account_instance_session_row['created'] = created
            account_instance_session_row['outcome_complete'] = instance_session_outcomes[assessment_name]['complete']
            account_instance_session_row['score'] = instance_session_outcomes[assessment_name]['score']
            account_instance_session_row['severity'] = instance_session_outcomes[assessment_name]['severity']
            account_instance_session_data.append(account_instance_session_row)
            
        # Update conditional instance values from instance session info
        if instance_session_outcomes['PHQ4']['complete']==1:
            complete_count = sum([instance_session_outcomes[outcome]['complete'] for outcome in instance_session_outcomes.keys()])
            if complete_count == 4 or instance_session_outcomes['PHQ4']['score'] < START_SESSION_SCORE_THRESHOLD:
                instance_complete = 1
                escalation,escalation_provider = get_escalation(instance_session_outcomes)
                complete_time = instance.iloc[-1].created
        
        # new account instance row
        account_instance_row = dict()
        account_instance_row['instance_id'] = instance_id
        account_instance_row['account_id'] = account_id
        account_instance_row['num_instance_sessions'] = num_instance_sessions
        
        account_instance_row['PHQ4_attempts'] = instance_session_outcomes['PHQ4']['attempts']
        account_instance_row['PHQ9_attempts'] = instance_session_outcomes['PHQ9']['attempts']
        account_instance_row['GAD7_attempts'] = instance_session_outcomes['GAD7']['attempts']
        account_instance_row['PCPTSD_attempts'] = instance_session_outcomes['PCPTSD']['attempts']
        
        account_instance_row['PHQ4_complete'] = instance_session_outcomes['PHQ4']['complete']
        account_instance_row['PHQ9_complete'] = instance_session_outcomes['PHQ9']['complete']
        account_instance_row['GAD7_complete'] = instance_session_outcomes['GAD7']['complete']
        account_instance_row['PCPTSD_complete'] = instance_session_outcomes['PCPTSD']['complete']
        
        account_instance_row['PHQ4_score'] = instance_session_outcomes['PHQ4']['score']
        account_instance_row['PHQ9_score'] = instance_session_outcomes['PHQ9']['score']
        account_instance_row['GAD7_score'] = instance_session_outcomes['GAD7']['score']
        account_instance_row['PCPTSD_score'] = instance_session_outcomes['PCPTSD']['score']
        
        account_instance_row['PHQ4_severity'] = instance_session_outcomes['PHQ4']['severity']
        account_instance_row['PHQ9_severity'] = instance_session_outcomes['PHQ9']['severity']
        account_instance_row['GAD7_severity'] = instance_session_outcomes['GAD7']['severity']
        account_instance_row['PCPTSD_severity'] = instance_session_outcomes['PCPTSD']['severity']
        
        account_instance_row['crisis'] = crisis
        account_instance_row['crisis_text'] = crisis_text
        account_instance_row['crisis_value'] = crisis_value
        account_instance_row['instance_complete'] = instance_complete
        account_instance_row['escalation'] = escalation
        account_instance_row['escalation_provider'] = escalation_provider
        account_instance_row['start_time'] = start_time
        account_instance_row['complete_time'] = complete_time
        account_instance_row['last_updated'] = last_updated
        account_instance_data.append(account_instance_row)
        
    # new account row
    account_row = dict()
    account_row['account_id'] = account_id
    account_row['num_instances'] = num_instances
    account_row['num_sessions'] = num_sessions
    account_data.append(account_row)
    
    count+=1
    if count%500 == 0:
        print(count, 'accounts processed...')
        
print('Processing complete - see you next time!')

# Create dataframes
account_update = pd.DataFrame(account_data)
account_update = account.merge(account_update, how='inner', left_on='account_id', right_on='account_id')

account_instance = pd.DataFrame(account_instance_data)

account_instance_session_update = pd.DataFrame(account_instance_session_data)
session_merge_cols = [col for col in account_instance_session_update.columns if col in account_instance_sessions.columns]
account_instance_session_update = account_instance_sessions.merge(account_instance_session_update, 
                                                                  how='inner', 
                                                                  left_on=session_merge_cols, 
                                                                  right_on=session_merge_cols)

account_session_anomalies = pd.DataFrame(account_session_anomaly_data)

# Save data 
account_update.to_csv(MASTER_DATA_PATH + 'account_update_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
account_instance.to_csv(MASTER_DATA_PATH + 'account_instance_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
account_instance_session_update.to_csv(MASTER_DATA_PATH + 'account_instance_session_update_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
account_session_anomalies.to_csv(MASTER_DATA_PATH + 'account_session_anomalies_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')


#### Data Output and Validation ####

print(len(account_update))
account_update.head(2)

print(len(account_instance))
account_instance.head(2)

print(len(account_instance_session_update))
account_instance_session_update.head(2)

print(len(account_session_anomalies))
account_session_anomalies.head(2)


#### Instance Summary ####
sum_cols = ['num_instance_sessions',
            'PHQ4_attempts','PHQ9_attempts','GAD7_attempts','PCPTSD_attempts',
            'PHQ4_complete','PHQ9_complete','GAD7_complete','PCPTSD_complete',
            'crisis','instance_complete']
stat_cols = ['PHQ4_score','PHQ9_score','GAD7_score','PCPTSD_score']
cat_cols = ['PHQ4_severity','PHQ9_severity','GAD7_severity','PCPTSD_severity']

incomplete_instance = account_instance[account_instance['instance_complete']==0]
complete_instance = account_instance[account_instance['instance_complete']==1]
mild_instance = complete_instance[complete_instance['escalation']=='mild']
moderate_instance = complete_instance[complete_instance['escalation']=='moderate']
severe_instance = complete_instance[complete_instance['escalation']=='severe']

print('Instances Initiated:', len(account_instance))
print()
print('Incomplete Instances:',len(incomplete_instance))
print()
print('Complete Instances:',len(complete_instance))
print('\tMild (resilience_coach):',len(mild_instance))
print('\tModerate (psychotherapist):',len(moderate_instance))
print('\tSevere (psychiatrist):',len(severe_instance))


#### Instances with Complete PHQ4 ####
# Note: PHQ4 is the initial assessment
account_phq4 = account_instance[account_instance['PHQ4_complete']==1][['PHQ4_complete','PHQ9_complete','GAD7_complete','PCPTSD_complete']].sum(axis=1).value_counts().sort_index()
print('All Instances')
print(account_instance[sum_cols].sum(),'\n')
print('All Instances - Complete PHQ4')
print(account_phq4.sum(),'\n')
print('All Instances - Complete PHQ4 by number of complete assessments')
print(account_phq4)

incomplete_phq4 = incomplete_instance[incomplete_instance['PHQ4_complete']==1][['PHQ4_complete','PHQ9_complete','GAD7_complete','PCPTSD_complete']].sum(axis=1).value_counts().sort_index()
print('Incomplete Instances')
print(incomplete_instance[sum_cols].sum(),'\n')
print('Incomplete Instances - Complete PHQ4')
print(incomplete_phq4.sum(),'\n')
print('Incomplete Instances - Complete PHQ4 by number of complete assessments')
print(incomplete_phq4)

complete_phq4 = complete_instance[complete_instance['PHQ4_complete']==1][['PHQ4_complete','PHQ9_complete','GAD7_complete','PCPTSD_complete']].sum(axis=1).value_counts().sort_index()
print('Complete Instances')
print(complete_instance[sum_cols].sum(),'\n')
print('Complete Instances - Complete PHQ4')
print(complete_phq4.sum(),'\n')
print('Complete Instances - Complete PHQ4 by number of complete assessments')
print(complete_phq4)


#### Assessment Summary ####
#### All Scores ####
data = account_instance[stat_cols]
data = data[data!=-1]
print('Complete assessments:')
print('Assessment score count:',data.count())
print('Assessment score mean:',data.mean())
print('Assessment score median:',data.median())
print('Assessment score std. dev.:',data.std())
data.hist()

data = account_instance[account_instance['instance_complete']==1][stat_cols]
data = data[data!=-1]
print('Complete assessments associated with a complete instance:')
print('Assessment score count:',data.count())
print('Assessment score mean:',data.mean())
print('Assessment score median:',data.median())
print('Assessment score std. dev.:',data.std())
data.hist()


#### All Severities ####
instance_data = account_instance[account_instance['PHQ4_complete']==1]
instance_data_noSymptoms = instance_data[instance_data['PHQ4_severity']!= 'no_symptoms']

instance_data.head(2)

PHQ4_summary = pd.concat([instance_data['PHQ4_severity'].value_counts(0),instance_data['PHQ4_severity'].value_counts(1)], axis=1)
PHQ9_summary = pd.concat([instance_data_noSymptoms['PHQ9_severity'].value_counts(0),instance_data_noSymptoms['PHQ9_severity'].value_counts(1)], axis=1)
GAD7_summary = pd.concat([instance_data_noSymptoms['GAD7_severity'].value_counts(0),instance_data_noSymptoms['GAD7_severity'].value_counts(1)], axis=1)
PCPTSD_summary = pd.concat([instance_data_noSymptoms['PCPTSD_severity'].value_counts(0),instance_data_noSymptoms['PCPTSD_severity'].value_counts(1)], axis=1)
escalation_summary = pd.concat([instance_data['escalation'].value_counts(0),instance_data['escalation'].value_counts(1)], axis=1)

assessment_summary = pd.concat([PHQ4_summary,PHQ9_summary,GAD7_summary,PCPTSD_summary,escalation_summary],axis=1)
assessment_summary.columns = ['_'.join(item) for item in list(zip(assessment_summary.columns,['count','pct']*5))]

assessment_summary

assessment_summary.sum()

assessment_summary.to_csv(CHART_PATH + 'assessment_severity_summary_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')


#### PHQ4 ####
data = account_instance[account_instance['PHQ4_complete']==1]
print('Complete PHQ4:')
print('Scores:') 
print(data.PHQ4_score.value_counts().sort_index())
print('Severity:')
print(data.PHQ4_severity.value_counts())

data = account_instance[account_instance['instance_complete']==1]
print('Complete PHQ4 associated with a complete instance:')
print('Scores:')
print(data.PHQ4_score.value_counts().sort_index())
print('Severity:')
print(data.PHQ4_severity.value_counts())


#### PHQ4 --> PHQ9
print(account_instance[account_instance['PHQ4_severity']=='mild']['PHQ9_severity'].value_counts())
print(account_instance[account_instance['PHQ4_severity']=='moderate']['PHQ9_severity'].value_counts())
print(account_instance[account_instance['PHQ4_severity']=='severe']['PHQ9_severity'].value_counts())


#### Instance Flow - Sankey Diagram ####
## All Instances ##
#     * Comprehensive Flow Diagram: start-->PHQ4-->PHQ9-->GAD7-->PCPTSD-->escalation-->recommendation-->booking
#         * phq4-->severity[no_symptoms]-->escalation[mild]
#         * phq4-->severity[!no_symptoms]-->phq9-->severity[...]-->severity[...]-->gad7-->severity[...]-->pcptsd-->escalation[mild,moderate,severe]
#         * phq9-->q9[>0]-->crisis[True]
#         * phq9-->q9[==0]-->crisis[False]
#     
# * **Analysis Ideas - Last Updated 12/10/2021**
#     * look at attempts and scores
#     * unstack and/or aggregate at different levels
#     * explore heatmaps / clustermaps
#     * Frequent itemset could be interesting here
#     * correlation of assessment severities/scores with escalation
#         * can predict escalation?
#         * can predict escalation from individual question level?
#     * Similar severity/score rows
#         * jaccard dist; cosine dist
#         * clustering and looking at cluster agreement / properties
# 
#     * crisis (**COMPLETE 12/08/2021**)
#         * crisis ratio: crisis/count 
#         * crisis value group mean: q9_value_sum/count
#         * crisis value flag mean: q9_value_sum/crisis
#     * other session/instance/account columns: session_attempts, instance_complete, etc.
#     * account- and appointment-level info
#         * num appointments 
#         * num appointments by provider
#         * recommendation provider vs first-appointment-provider match (binary)
#             * low availability recommendation?
#             * can check for actual availability with historic supply data (may help identify demand)
#         * recommendation-provider appointment-provider alignment 
#             * ordinal based on distance +-1 from reccommendation
#             * can be one time, sum of all deviations, avg. of deviations, median deviation, etc.
#                 * can weight by availability level

# Instance flow parameters
subset_cols = ['PHQ4_severity','PHQ9_severity','GAD7_severity','PCPTSD_severity','escalation','crisis','crisis_value']
group_cols = ['PHQ4_severity','PHQ9_severity','GAD7_severity','PCPTSD_severity','escalation']
agg_dict= {'count':'sum','crisis':'sum','crisis_value':'sum'}

# Categorical index custom sort order
PHQ4_cat_order = ['no_symptoms', 'mild', 'moderate', 'severe','inconclusive']
PHQ9_cat_order = ['no_symptoms', 'mild', 'moderate', 'moderately_severe', 'severe','inconclusive']
GAD7_cat_order = ['no_symptoms', 'mild', 'moderate', 'severe','inconclusive']
PCPTSD_cat_order = ['no_symptoms', 'followup_sensitivity', 'followup_efficiency', 'inconclusive']
ESC_cat_order = ['mild', 'moderate', 'severe','inconclusive']

cat_order_dict = {0:PHQ4_cat_order, 1:PHQ9_cat_order, 2:GAD7_cat_order, 3:PCPTSD_cat_order, 4:ESC_cat_order}

# Get instance flow via grouping by severity
def get_instance_flow(subset_cols, group_cols, agg_dict):
    instance_subset = account_instance[subset_cols].copy()
    instance_subset['count'] = 1

    instance_flow = instance_subset.groupby(group_cols, sort=False).agg(agg_dict)
    instance_flow = instance_flow.rename(columns={'crisis_value':'crisis_value_sum'})
    instance_flow['crisis_value_group_mean'] = instance_flow['crisis_value_sum']/instance_flow['count']
    instance_flow['crisis_value_flag_mean'] = instance_flow['crisis_value_sum']/instance_flow['crisis']
    instance_flow['crisis_ratio'] = instance_flow['crisis']/instance_flow['count']
    
    for idx_level in range(len(group_cols)):
        level_cat_idx = pd.CategoricalIndex(instance_flow.index.levels[idx_level].values, 
                                            categories=cat_order_dict[idx_level], 
                                            ordered=True)
        instance_flow.index = instance_flow.index.set_levels(level_cat_idx, level=idx_level)
    
    instance_flow = instance_flow.sort_index()
    
    return instance_subset,instance_flow

instance_subset,instance_flow = get_instance_flow(subset_cols, group_cols, agg_dict)

instance_subset.to_csv(MASTER_DATA_PATH + 'instance_subset_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
instance_flow.to_csv(MASTER_DATA_PATH + 'instance_flow_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(instance_flow))
print(instance_flow[['count','crisis']].sum())
instance_flow.head()

#### Sankey: Nodes and Links ####
# * Development tasks:
#     * add new level 0 index to clean up node code
#     * make node and link separate functions
#     * rewrite links function to handle phq4 = no symptoms
#     * set node position in function (as separate function) by flow_data.sum() at each level
#     * make nodes selectable to filter pathway highlighting
#         * highliht **all** paths going through selected/hovered node
#     * add edge color modes (crisis flag only, labels/colors for esc estimate at each level - might make groups larger)
#     
# * Custom Filtering:
#     * **account_instance:** 
#         * add filtering cols: account_source_id, dayofweek, timeofday, yearmonth, etc.
#         * rows represent individual **instances** attempted/completed by *individual patients*
#             * use **indicator cols** (pd.get_dummies) to allow for aggregation to instance_flow data
#             * each **filtering col** corresponds to an **indicator col group**
#             * each **indicator col** in an indicator col group corresponds to a specific **filtering criteria**
#             * Example: filtering col: dayofweek; indicator col group: [mon,tue,wed,thu,fri,sat,sun]; indicator col=fri: filter for instances occurring on Friday
#     * **instance_flow:** 
#         * extract assessment pathways by grouping account_instance data by assessment severities; aggregate count and indicator columns
#         * rows represent individual **assessment pathways** flowed through by *groups of patients*
#             * **instances** correspond to specific **assessment pathways** (sequences of assessment severities)
#             * each **assessment pathway** (row) will have **values** for all indicator cols
#             * indicator col **values** represent the number of *individual patients* flowing through that **assessment pathway** 
#             * indicator col **groups** (for example, all dayofweek cols) should sum to the total count for that **assessment pathway**
#     * **sankey_data (nodes, links):** 
#         * extract node and link data at each level of instance_flow; each level will have the same number of links and the same link values
#         * rows represent individual **nodes** or **links** corresponding to one or more *assessment pathways*
#             * each **link** will have the same indicator col values as its **assessment pathway**
#             * indicator col **values** represent the number of *individual patients* flowing through that **link** in the **assessment pathway**
#             
#     * Notes:
#         * single col filtering cn be done at the instance_flow or node/link level
#         * multi-col filtering must be done at the account_instance level
#         * code needs to be able to handle account_instance level filtering (i.e. functions must work together in one call)
#         

import plotly.graph_objects as go

NODE_ALPHA = '0.8'
LINK_ALPHA = '0.2'
DEFAULT_ALPHA = '0.5'

RGB_GREY = '192,192,192'
RGB_RED = '255,0,0'
RGB_GREEN = '0,255,0'
RGB_BLUE = '0,0,255'
RGB_ORANGE = '255,128,0'
RGB_MAGENTA = '204,0,102'

color_dict = {'start':RGB_GREY,'no_symptoms':RGB_BLUE,'mild':RGB_GREEN,
              'moderate':RGB_ORANGE,'moderately_severe':RGB_MAGENTA,'severe':RGB_RED,
              'inconclusive':RGB_GREY,'followup_sensitivity':RGB_ORANGE,'followup_efficiency':RGB_RED}
link_label_dict = {'path':1, 'escalation':1, 'cohort':1}

def get_element_color(data, element):
    # Get element type
    if element == 'node':
        alpha = NODE_ALPHA
    elif element == 'link':
        alpha = LINK_ALPHA
    else:
        print('Invalid element type! Accepted types are node or link - setting default alpha value.')
        alpha = DEFAULT_ALPHA
        
    # get color string    
    if type(data) == str:
        color = 'rgba(' + color_dict[data] + ',' + alpha + ')'
    elif type(data) == list:
        color = ['rgba(' + color_dict[item] + ',' + alpha + ')' for item in data]
    else:
        print('Invalid data type! Accepted types are string or list - setting default color.')
        color = ''
    
    return color

def get_link_labels(flow_data, link_paths, link_label):
    # Check for valid label
    try:
        link_label_dict[link_label]
        
        if link_label == 'path':
            link_labels = link_paths
        elif link_label == 'escalation':
            link_labels = flow_data.index.get_level_values('escalation').to_list()
        elif link_label == 'cohort':
            link_labels = link_paths #UPDATE TO PT COHORTS VIA COS SIMILARITY OF ASSESSMENT SCORES
    except:
        print('Invalid link label mode - using default path labeling')
        link_labels = link_paths
        
    return link_labels

#### NOTE: MAKE PATH LABELING DEFAULT (TO HANDLE NO_SYMPTOMS) AND ADD COL TO DATAFRAME LINKS['PATH']
#### NOTE: IN DATAFRAME, LINKS['LABEL'] WILL BE ASSIGNED BASED ON LINK_LABEL PARAM VALUE

"""
# Description:  Column-wise node and link extraction from multi-index dataframe
# Pros: Faster, vectorized implementation compared to row-wise extraction
# Limitations: Assumes nodes are grouped into levels (flow can only pass through one node per level); levels have a strict, global order (flow is directed); each level must have a value (flow can not skip a level by default)
# Notes: a row-wise extraction can relax any/all of the above assumptions 
"""

def get_sankey_data(flow_data, complete_PHQ4=True, link_label='path', link_color='target'):
    # Initialize data structures
    node_idx_dict = dict()
    node_dict = {'severity':[],'label':[],'color':[],'idx':[],'level':[]}
    link_dict = {'source':[],'target':[],'color':[],'path':[],'label':[],'value':[],'level':[]}
    
    # Filter PHQ4
    if complete_PHQ4:
        flow_data = flow_data[flow_data.index.get_level_values(0) !='inconclusive']
    # Static flow data
    link_paths = ['-->'.join(item) for item in flow_data.index.to_list()]
    link_labels = get_link_labels(flow_data, link_paths, link_label)   
    link_values = flow_data['count'].to_list()

    # Process each level to extract nodes and links
    num_levels = len(flow_data.index.levels)
    for level in range(num_levels):
        assessment = flow_data.index.levels[level].name.split('_')[0]

        # Nodes
        if level == 0:
            node_dict['severity'].append('start')
            node_dict['label'].append('Assessment Start')
            node_dict['color'].append(get_element_color('start','node'))
            node_dict['idx'].append(0)
            node_dict['level'].append(level)
            node_idx_dict['Assessment Start'] = 0

        node_severity = flow_data.index.levels[level].categories.to_list()
        node_label = [assessment + ': ' + item for item in node_severity]
        node_color = get_element_color(node_severity, 'node')
        node_idx = [item + len(node_dict['idx']) for item in range(len(node_severity))]
        node_levels = [level]*len(node_severity)

        node_dict['severity'] += node_severity
        node_dict['label'] += node_label
        node_dict['color'] += node_color
        node_dict['idx'] += node_idx
        node_dict['level'] += node_levels
        node_idx_dict.update(dict(zip(node_label,node_idx)))


        # Links
        link_data = flow_data.index.get_level_values(level).to_list()
        link_node_labels = [assessment + ': ' + item for item in link_data]
        link_levels = [level]*len(link_data)

        if level == 0:
            # Assessment start --> PHQ4
            link_dict['source'] += [node_idx_dict[item] for item in ['Assessment Start']*len(link_node_labels)]
        if level != num_levels - 1:
            # current assessment --> next assessment
            link_dict['source'] += [node_idx_dict[item] for item in link_node_labels]

        link_dict['target'] += [node_idx_dict[item] for item in link_node_labels]
        link_dict['color'] += get_element_color(link_data,'link')
        link_dict['path'] += link_paths
        link_dict['label'] += link_labels
        link_dict['value'] += link_values
        link_dict['level'] += link_levels

    # Create dataframes to hold node and link data
    nodes = pd.DataFrame(node_dict)
    links = pd.DataFrame(link_dict)

    # shorten no_symptoms PHQ4 path (skip "inconclusive" labels for intermediate assessments)
    links_no_symptoms = links[links['path'].str.startswith('no_symptoms')]
    links_other = links[~links['path'].str.startswith('no_symptoms')]
    num_unique_pathways = len(links_no_symptoms.path.unique())

    links_no_symptoms_start = links_no_symptoms.iloc[:num_unique_pathways].copy()
    links_no_symptoms_end = links_no_symptoms.iloc[-num_unique_pathways:].copy()
    links_no_symptoms_end.loc[:,'source'] = links_no_symptoms_start.loc[:,'target'].values
    links_no_symptoms = pd.concat([links_no_symptoms_start,links_no_symptoms_end])
    #links_no_symptoms['path'] = 'no_symptoms-->mild'

    # Get final links
    links = pd.concat([links_no_symptoms,links_other]).reset_index(drop=True)

    return nodes,links

def get_sankey_diagram(nodes, links, title):
    fig = go.Figure(
        data = [go.Sankey(
            valueformat = "4d",
            valuesuffix = " Patients",
            # Define nodes
            node = dict(
              pad = 15,
              thickness = 15,
              line = dict(color = "black", width = 0.5),
              label =  nodes['label'].to_list(),
              color =  nodes['color'].to_list()
            ),
            # Add links
            link = dict(
              source = links['source'].to_list(),
              target = links['target'].to_list(),
              value = links['value'].to_list(),
              label = links['label'].to_list(),
              color = links['color'].to_list()))],
        layout = dict(
            height = 800,
            width = 1000
        )
    )

    fig.update_layout(title_text=title,
                      font_size=10)
    fig.show()


#### Sankey Diagrams ####

#### Diagram 1 ####
# link labels: full path
# link colors: target node

# Get sankey data
nodes,links = get_sankey_data(instance_flow,complete_PHQ4=True,link_label='path',link_color='target')

# Save data
nodes.to_csv(SANKEY_DATA_PATH + 'nodes_path_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
links.to_csv(SANKEY_DATA_PATH + 'links_path_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(nodes))
nodes.head()

print(len(links))
links.head()

title = "Cobalt: Patient Assessment Flow and Escalation"
get_sankey_diagram(nodes, links, title)


#### Diagram 2 ####
# * link labels: escalation
# * link colors: target node

# Get sankey data
nodes,links = get_sankey_data(instance_flow,complete_PHQ4=True,link_label='escalation',link_color='target')

# Save data
nodes.to_csv(SANKEY_DATA_PATH + 'nodes_esc_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
links.to_csv(SANKEY_DATA_PATH + 'links_esc_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(nodes))
nodes.head()

print(len(links))
links.head()

title = "Cobalt: Patient Assessment Flow and Escalation"
get_sankey_diagram(nodes, links, title)


#### Sankey Summary Stats ####
# Instances and included data, instance flow and link properties, account source distribution

# Account instance data (account_instance-->instance_flow-->sankey[nodes,links])
print('All instances:',len(account_instance.account_id))
print('\tUnique accounts associated with all instances:',len(account_instance.account_id.unique()))

print('Instances with complete PHQ4',len(account_instance[account_instance['PHQ4_complete']==1].account_id))
print('\tUnique accounts associated with complete PHQ4',len(account_instance[account_instance['PHQ4_complete']==1].account_id.unique()))

print('Instances with escalation:',len(account_instance[account_instance['instance_complete']==1].account_id))
print('\tUnique accounts associated with instances with escalations:',len(account_instance[account_instance['instance_complete']==1].account_id.unique()))

# Instance flow and Sankey data
print('Total flow volume (patients):',instance_flow[instance_flow.index.get_level_values(0) != 'inconclusive']['count'].sum())
print('Total link wieght per level (patients):',links[links['source']==0]['value'].sum())
print('\tTotal links per level (unique assessment pathways):',len(links[links['source']==0]['value']))


#### Session Outcomes ####
# A session outcome is a score on a **completed** session assessment. 

#### PHQ4 Score Distribution ####
# Score distribution
PHQ4_data = account_instance[account_instance['PHQ4_score']!= -1]['PHQ4_score'].value_counts().sort_index()
PHQ4_data.to_csv(MASTER_DATA_PATH + 'PHQ4_hist_allTime_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(PHQ4_data))
PHQ4_data.head(2)


#### Time Series ####
# Get score dates
PHQ4_scores_dates = account_instance[account_instance['PHQ4_score']!= -1][['account_id','PHQ4_score','crisis','last_updated']].copy()
PHQ4_scores_dates = PHQ4_scores_dates.rename(columns={'PHQ4_score':'answer_value','last_updated':'created_session_answer',})

# Adjust columns
PHQ4_scores_dates['year'] = PHQ4_scores_dates['created_session_answer'].dt.year
PHQ4_scores_dates['month'] = PHQ4_scores_dates['created_session_answer'].dt.month
PHQ4_scores_dates['week'] = PHQ4_scores_dates['created_session_answer'].dt.week
PHQ4_scores_dates['day'] = PHQ4_scores_dates['created_session_answer'].dt.day
PHQ4_scores_dates['year_month'] = PHQ4_scores_dates['created_session_answer'].values.astype('datetime64[M]')
PHQ4_scores_dates['year_month_week'] = PHQ4_scores_dates['created_session_answer'].values.astype('datetime64[W]')
PHQ4_scores_dates['dayofyear'] = PHQ4_scores_dates['created_session_answer'].apply(lambda x: get_date_str(x))

# Save data
PHQ4_scores_dates.to_csv(MASTER_DATA_PATH + 'PHQ4_scores_dates_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Monthly time series data
PHQ4_ts_count = pd.DataFrame(PHQ4_scores_dates.groupby(['year','month']).count().answer_value)
PHQ4_ts_mean = pd.DataFrame(PHQ4_scores_dates.groupby(['year','month']).mean().answer_value)
PHQ4_ts_median = pd.DataFrame(PHQ4_scores_dates.groupby(['year','month']).median().answer_value)
PHQ4_ts_sum = pd.DataFrame(PHQ4_scores_dates.groupby(['year','month']).sum().crisis)
PHQ4_ts_data = PHQ4_ts_count.merge(
    PHQ4_ts_mean, how='inner', left_index=True, right_index=True).merge(
    PHQ4_ts_median, how='inner', left_index=True, right_index=True).merge(
    PHQ4_ts_sum, how='inner', left_index=True, right_index=True)
PHQ4_ts_data.columns = ['PHQ4_score_count', 'PHQ4_score_mean', 'PHQ4_score_median', 'PHQ4_crisis_count']

# Save data
PHQ4_ts_data.to_csv(MASTER_DATA_PATH + 'PHQ4_monthly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Weekly time series data
PHQ4_weekly_ts_count = pd.DataFrame(PHQ4_scores_dates.groupby(['year_month_week']).count().answer_value)
PHQ4_weekly_ts_count.index = pd.MultiIndex.from_arrays([PHQ4_weekly_ts_count.index.year, 
                                                            PHQ4_weekly_ts_count.index.month, 
                                                            PHQ4_weekly_ts_count.index.day], 
                                                            names=['Year','Month','Week'])
PHQ4_weekly_ts_mean = pd.DataFrame(PHQ4_scores_dates.groupby(['year_month_week']).mean().answer_value)
PHQ4_weekly_ts_mean.index = pd.MultiIndex.from_arrays([PHQ4_weekly_ts_mean.index.year, 
                                                            PHQ4_weekly_ts_mean.index.month, 
                                                            PHQ4_weekly_ts_mean.index.day], 
                                                            names=['Year','Month','Week'])
PHQ4_weekly_ts_median = pd.DataFrame(PHQ4_scores_dates.groupby(['year_month_week']).median().answer_value)
PHQ4_weekly_ts_median.index = pd.MultiIndex.from_arrays([PHQ4_weekly_ts_median.index.year, 
                                                            PHQ4_weekly_ts_median.index.month, 
                                                            PHQ4_weekly_ts_median.index.day], 
                                                            names=['Year','Month','Week'])
PHQ4_weekly_ts_sum = pd.DataFrame(PHQ4_scores_dates.groupby(['year_month_week']).sum().crisis)
PHQ4_weekly_ts_sum.index = pd.MultiIndex.from_arrays([PHQ4_weekly_ts_sum.index.year, 
                                                            PHQ4_weekly_ts_sum.index.month, 
                                                            PHQ4_weekly_ts_sum.index.day], 
                                                            names=['Year','Month','Week'])

PHQ4_weekly_ts_data = PHQ4_weekly_ts_count.merge(
    PHQ4_weekly_ts_mean, how='inner', left_index=True, right_index=True).merge(
    PHQ4_weekly_ts_median, how='inner', left_index=True, right_index=True).merge(
    PHQ4_weekly_ts_sum, how='inner', left_index=True, right_index=True)
PHQ4_weekly_ts_data.columns = ['PHQ4_score_count', 'PHQ4_score_mean', 'PHQ4_score_median', 'PHQ4_crisis_count']

# Save data
PHQ4_weekly_ts_data.to_csv(MASTER_DATA_PATH + 'PHQ4_weekly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(PHQ4_ts_data))
PHQ4_ts_data.head(2)

print(len(PHQ4_weekly_ts_data))
PHQ4_weekly_ts_data.head(2)


#### PHQ9 ####

# Crisis question
PHQ9_crisis_response = account_session_answer[account_session_answer['crisis']==True].answer_text.value_counts()


#### PHQ9 Score Distribution ####
# Score distribution
PHQ9_data = account_instance[account_instance['PHQ9_score']!= -1]['PHQ9_score'].value_counts().sort_index()
PHQ9_data.to_csv(MASTER_DATA_PATH + 'PHQ9_hist_allTime_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(PHQ9_data))
PHQ9_data.head(2)


#### Time Series ####
# Get score dates
PHQ9_scores_dates = account_instance[account_instance['PHQ9_score']!= -1][['account_id','PHQ9_score','crisis','last_updated']].copy()
PHQ9_scores_dates = PHQ9_scores_dates.rename(columns={'PHQ9_score':'answer_value','last_updated':'created_session_answer',})

# Adjust columns
PHQ9_scores_dates['year'] = PHQ9_scores_dates['created_session_answer'].dt.year
PHQ9_scores_dates['month'] = PHQ9_scores_dates['created_session_answer'].dt.month
PHQ9_scores_dates['week'] = PHQ9_scores_dates['created_session_answer'].dt.week
PHQ9_scores_dates['day'] = PHQ9_scores_dates['created_session_answer'].dt.day
PHQ9_scores_dates['year_month'] = PHQ9_scores_dates['created_session_answer'].values.astype('datetime64[M]')
PHQ9_scores_dates['year_month_week'] = PHQ9_scores_dates['created_session_answer'].values.astype('datetime64[W]')
PHQ9_scores_dates['dayofyear'] = PHQ9_scores_dates['created_session_answer'].apply(lambda x: get_date_str(x))

# Save data
PHQ9_scores_dates.to_csv(MASTER_DATA_PATH + 'PHQ9_scores_dates_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Monthly time series data
PHQ9_ts_count = pd.DataFrame(PHQ9_scores_dates.groupby(['year','month']).count().answer_value)
PHQ9_ts_mean = pd.DataFrame(PHQ9_scores_dates.groupby(['year','month']).mean().answer_value)
PHQ9_ts_median = pd.DataFrame(PHQ9_scores_dates.groupby(['year','month']).median().answer_value)
PHQ9_ts_sum = pd.DataFrame(PHQ9_scores_dates.groupby(['year','month']).sum().crisis)
PHQ9_ts_data = PHQ9_ts_count.merge(
    PHQ9_ts_mean, how='inner', left_index=True, right_index=True).merge(
    PHQ9_ts_median, how='inner', left_index=True, right_index=True).merge(
    PHQ9_ts_sum, how='inner', left_index=True, right_index=True)
PHQ9_ts_data.columns = ['PHQ9_score_count', 'PHQ9_score_mean', 'PHQ9_score_median', 'PHQ9_crisis_count']

# Save data
PHQ9_ts_data.to_csv(MASTER_DATA_PATH + 'PHQ9_monthly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Weekly time series data
PHQ9_weekly_ts_count = pd.DataFrame(PHQ9_scores_dates.groupby(['year_month_week']).count().answer_value)
PHQ9_weekly_ts_count.index = pd.MultiIndex.from_arrays([PHQ9_weekly_ts_count.index.year, 
                                                            PHQ9_weekly_ts_count.index.month, 
                                                            PHQ9_weekly_ts_count.index.day], 
                                                            names=['Year','Month','Week'])
PHQ9_weekly_ts_mean = pd.DataFrame(PHQ9_scores_dates.groupby(['year_month_week']).mean().answer_value)
PHQ9_weekly_ts_mean.index = pd.MultiIndex.from_arrays([PHQ9_weekly_ts_mean.index.year, 
                                                            PHQ9_weekly_ts_mean.index.month, 
                                                            PHQ9_weekly_ts_mean.index.day], 
                                                            names=['Year','Month','Week'])
PHQ9_weekly_ts_median = pd.DataFrame(PHQ9_scores_dates.groupby(['year_month_week']).median().answer_value)
PHQ9_weekly_ts_median.index = pd.MultiIndex.from_arrays([PHQ9_weekly_ts_median.index.year, 
                                                            PHQ9_weekly_ts_median.index.month, 
                                                            PHQ9_weekly_ts_median.index.day], 
                                                            names=['Year','Month','Week'])
PHQ9_weekly_ts_sum = pd.DataFrame(PHQ9_scores_dates.groupby(['year_month_week']).sum().crisis)
PHQ9_weekly_ts_sum.index = pd.MultiIndex.from_arrays([PHQ9_weekly_ts_sum.index.year, 
                                                            PHQ9_weekly_ts_sum.index.month, 
                                                            PHQ9_weekly_ts_sum.index.day], 
                                                            names=['Year','Month','Week'])

PHQ9_weekly_ts_data = PHQ9_weekly_ts_count.merge(
    PHQ9_weekly_ts_mean, how='inner', left_index=True, right_index=True).merge(
    PHQ9_weekly_ts_median, how='inner', left_index=True, right_index=True).merge(
    PHQ9_weekly_ts_sum, how='inner', left_index=True, right_index=True)
PHQ9_weekly_ts_data.columns = ['PHQ9_score_count', 'PHQ9_score_mean', 'PHQ9_score_median', 'PHQ9_crisis_count']

# Save data
PHQ9_weekly_ts_data.to_csv(MASTER_DATA_PATH + 'PHQ9_weekly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(PHQ9_ts_data))
PHQ9_ts_data.head(2)

print(len(PHQ9_weekly_ts_data))
PHQ9_weekly_ts_data.head(2)


#### GAD7 Score Distribution ####
# Score distribution
GAD7_data = account_instance[account_instance['GAD7_score']!= -1]['GAD7_score'].value_counts().sort_index()
GAD7_data.to_csv(MASTER_DATA_PATH + 'GAD7_hist_allTime_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(GAD7_data))
GAD7_data.head(2)


#### Time Series ####

# Get score dates
GAD7_scores_dates = account_instance[account_instance['GAD7_score']!= -1][['account_id','GAD7_score','crisis','last_updated']].copy()
GAD7_scores_dates = GAD7_scores_dates.rename(columns={'GAD7_score':'answer_value','last_updated':'created_session_answer',})

# Adjust columns
GAD7_scores_dates['year'] = GAD7_scores_dates['created_session_answer'].dt.year
GAD7_scores_dates['month'] = GAD7_scores_dates['created_session_answer'].dt.month
GAD7_scores_dates['week'] = GAD7_scores_dates['created_session_answer'].dt.week
GAD7_scores_dates['day'] = GAD7_scores_dates['created_session_answer'].dt.day
GAD7_scores_dates['year_month'] = GAD7_scores_dates['created_session_answer'].values.astype('datetime64[M]')
GAD7_scores_dates['year_month_week'] = GAD7_scores_dates['created_session_answer'].values.astype('datetime64[W]')
GAD7_scores_dates['dayofyear'] = GAD7_scores_dates['created_session_answer'].apply(lambda x: get_date_str(x))

# Save data
GAD7_scores_dates.to_csv(MASTER_DATA_PATH + 'GAD7_scores_dates_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Monthly time series data
GAD7_ts_count = pd.DataFrame(GAD7_scores_dates.groupby(['year','month']).count().answer_value)
GAD7_ts_mean = pd.DataFrame(GAD7_scores_dates.groupby(['year','month']).mean().answer_value)
GAD7_ts_median = pd.DataFrame(GAD7_scores_dates.groupby(['year','month']).median().answer_value)
GAD7_ts_sum = pd.DataFrame(GAD7_scores_dates.groupby(['year','month']).sum().crisis)
GAD7_ts_data = GAD7_ts_count.merge(
    GAD7_ts_mean, how='inner', left_index=True, right_index=True).merge(
    GAD7_ts_median, how='inner', left_index=True, right_index=True).merge(
    GAD7_ts_sum, how='inner', left_index=True, right_index=True)
GAD7_ts_data.columns = ['GAD7_score_count', 'GAD7_score_mean', 'GAD7_score_median', 'GAD7_crisis_count']

# Save data
GAD7_ts_data.to_csv(MASTER_DATA_PATH + 'GAD7_monthly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Weekly time series data
GAD7_weekly_ts_count = pd.DataFrame(GAD7_scores_dates.groupby(['year_month_week']).count().answer_value)
GAD7_weekly_ts_count.index = pd.MultiIndex.from_arrays([GAD7_weekly_ts_count.index.year, 
                                                            GAD7_weekly_ts_count.index.month, 
                                                            GAD7_weekly_ts_count.index.day], 
                                                            names=['Year','Month','Week'])
GAD7_weekly_ts_mean = pd.DataFrame(GAD7_scores_dates.groupby(['year_month_week']).mean().answer_value)
GAD7_weekly_ts_mean.index = pd.MultiIndex.from_arrays([GAD7_weekly_ts_mean.index.year, 
                                                            GAD7_weekly_ts_mean.index.month, 
                                                            GAD7_weekly_ts_mean.index.day], 
                                                            names=['Year','Month','Week'])
GAD7_weekly_ts_median = pd.DataFrame(GAD7_scores_dates.groupby(['year_month_week']).median().answer_value)
GAD7_weekly_ts_median.index = pd.MultiIndex.from_arrays([GAD7_weekly_ts_median.index.year, 
                                                            GAD7_weekly_ts_median.index.month, 
                                                            GAD7_weekly_ts_median.index.day], 
                                                            names=['Year','Month','Week'])
GAD7_weekly_ts_sum = pd.DataFrame(GAD7_scores_dates.groupby(['year_month_week']).sum().crisis)
GAD7_weekly_ts_sum.index = pd.MultiIndex.from_arrays([GAD7_weekly_ts_sum.index.year, 
                                                            GAD7_weekly_ts_sum.index.month, 
                                                            GAD7_weekly_ts_sum.index.day], 
                                                            names=['Year','Month','Week'])

GAD7_weekly_ts_data = GAD7_weekly_ts_count.merge(
    GAD7_weekly_ts_mean, how='inner', left_index=True, right_index=True).merge(
    GAD7_weekly_ts_median, how='inner', left_index=True, right_index=True).merge(
    GAD7_weekly_ts_sum, how='inner', left_index=True, right_index=True)
GAD7_weekly_ts_data.columns = ['GAD7_score_count', 'GAD7_score_mean', 'GAD7_score_median', 'GAD7_crisis_count']

# Save data
GAD7_weekly_ts_data.to_csv(MASTER_DATA_PATH + 'GAD7_weekly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(GAD7_ts_data))
GAD7_ts_data.head(2)

print(len(GAD7_weekly_ts_data))
GAD7_weekly_ts_data.head(2)


#### PC-PTSD Score Distribution ####
# Score distribution
PCPTSD_data = account_instance[account_instance['PCPTSD_score']!= -1]['PCPTSD_score'].value_counts().sort_index()
PCPTSD_data.to_csv(MASTER_DATA_PATH + 'PCPTSD_hist_allTime_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(PCPTSD_data))
PCPTSD_data.head(2)


#### Time Series ####
# Get score dates
PCPTSD_scores_dates = account_instance[account_instance['PCPTSD_score']!= -1][['account_id','PCPTSD_score','crisis','last_updated']].copy()
PCPTSD_scores_dates = PCPTSD_scores_dates.rename(columns={'PCPTSD_score':'answer_value','last_updated':'created_session_answer',})

# Adjust columns
PCPTSD_scores_dates['year'] = PCPTSD_scores_dates['created_session_answer'].dt.year
PCPTSD_scores_dates['month'] = PCPTSD_scores_dates['created_session_answer'].dt.month
PCPTSD_scores_dates['week'] = PCPTSD_scores_dates['created_session_answer'].dt.week
PCPTSD_scores_dates['day'] = PCPTSD_scores_dates['created_session_answer'].dt.day
PCPTSD_scores_dates['year_month'] = PCPTSD_scores_dates['created_session_answer'].values.astype('datetime64[M]')
PCPTSD_scores_dates['year_month_week'] = PCPTSD_scores_dates['created_session_answer'].values.astype('datetime64[W]')
PCPTSD_scores_dates['dayofyear'] = PCPTSD_scores_dates['created_session_answer'].apply(lambda x: get_date_str(x))

# Save data
PCPTSD_scores_dates.to_csv(MASTER_DATA_PATH + 'PCPTSD_scores_dates_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Monthly time series data
PCPTSD_ts_count = pd.DataFrame(PCPTSD_scores_dates.groupby(['year','month']).count().answer_value)
PCPTSD_ts_mean = pd.DataFrame(PCPTSD_scores_dates.groupby(['year','month']).mean().answer_value)
PCPTSD_ts_median = pd.DataFrame(PCPTSD_scores_dates.groupby(['year','month']).median().answer_value)
PCPTSD_ts_sum = pd.DataFrame(PCPTSD_scores_dates.groupby(['year','month']).sum().crisis)
PCPTSD_ts_data = PCPTSD_ts_count.merge(
    PCPTSD_ts_mean, how='inner', left_index=True, right_index=True).merge(
    PCPTSD_ts_median, how='inner', left_index=True, right_index=True).merge(
    PCPTSD_ts_sum, how='inner', left_index=True, right_index=True)
PCPTSD_ts_data.columns = ['PCPTSD_score_count', 'PCPTSD_score_mean', 'PCPTSD_score_median', 'PCPTSD_crisis_count']

# Save data
PCPTSD_ts_data.to_csv(MASTER_DATA_PATH + 'PCPTSD_monthly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Weekly time series data
PCPTSD_weekly_ts_count = pd.DataFrame(PCPTSD_scores_dates.groupby(['year_month_week']).count().answer_value)
PCPTSD_weekly_ts_count.index = pd.MultiIndex.from_arrays([PCPTSD_weekly_ts_count.index.year, 
                                                            PCPTSD_weekly_ts_count.index.month, 
                                                            PCPTSD_weekly_ts_count.index.day], 
                                                            names=['Year','Month','Week'])
PCPTSD_weekly_ts_mean = pd.DataFrame(PCPTSD_scores_dates.groupby(['year_month_week']).mean().answer_value)
PCPTSD_weekly_ts_mean.index = pd.MultiIndex.from_arrays([PCPTSD_weekly_ts_mean.index.year, 
                                                            PCPTSD_weekly_ts_mean.index.month, 
                                                            PCPTSD_weekly_ts_mean.index.day], 
                                                            names=['Year','Month','Week'])
PCPTSD_weekly_ts_median = pd.DataFrame(PCPTSD_scores_dates.groupby(['year_month_week']).median().answer_value)
PCPTSD_weekly_ts_median.index = pd.MultiIndex.from_arrays([PCPTSD_weekly_ts_median.index.year, 
                                                            PCPTSD_weekly_ts_median.index.month, 
                                                            PCPTSD_weekly_ts_median.index.day], 
                                                            names=['Year','Month','Week'])
PCPTSD_weekly_ts_sum = pd.DataFrame(PCPTSD_scores_dates.groupby(['year_month_week']).sum().crisis)
PCPTSD_weekly_ts_sum.index = pd.MultiIndex.from_arrays([PCPTSD_weekly_ts_sum.index.year, 
                                                            PCPTSD_weekly_ts_sum.index.month, 
                                                            PCPTSD_weekly_ts_sum.index.day], 
                                                            names=['Year','Month','Week'])

PCPTSD_weekly_ts_data = PCPTSD_weekly_ts_count.merge(
    PCPTSD_weekly_ts_mean, how='inner', left_index=True, right_index=True).merge(
    PCPTSD_weekly_ts_median, how='inner', left_index=True, right_index=True).merge(
    PCPTSD_weekly_ts_sum, how='inner', left_index=True, right_index=True)
PCPTSD_weekly_ts_data.columns = ['PCPTSD_score_count', 'PCPTSD_score_mean', 'PCPTSD_score_median', 'PCPTSD_crisis_count']

# Save data
PCPTSD_weekly_ts_data.to_csv(MASTER_DATA_PATH + 'PCPTSD_weekly_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(PCPTSD_ts_data))
PCPTSD_ts_data.head(2)

print(len(PCPTSD_weekly_ts_data))
PCPTSD_weekly_ts_data.head(2)


#### Escalation Outcomes ####
# Develop platform-specific constructs: 
#     * "treatment inertia": time to initiate treatment events 
#     * "treatment density/spread": time between treatment events
#     * "treatment scope/breadth": modalities of treatment events
#     * "treatment endurance/longevity": lifetime of treatment events
#     * "treatment journey/flow": evolution of treatment behavior 
#     * "treatment efficacy"

#### Pie chart ####
def get_pie_chart(data, title, save_name, explode_str=''):
    def format_text(pct, counts):
        absolute = int(np.round(pct/100.*np.sum(counts)))
        return "{:d}\n({:.1f}%)".format(absolute,pct)
    
    labels = data.index
    pie_data = data.iloc[:,0]
    explode = [0 if item!=explode_str else 0.1 for item in data.index.values]

    fig, ax = plt.subplots(figsize=(8,8), facecolor='whitesmoke')
    ax.pie(pie_data, explode=explode, labels=labels, autopct=lambda pct: format_text(pct, pie_data), 
           textprops=dict(color="black",size=14), shadow=False, startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    fig.suptitle(title, fontsize=18)

    save_figure(fig,FIGURE_PATH,save_name)
    plt.show()

#### Get instance data ####
instance_data = account_instance[account_instance['instance_complete']==1].copy()

# Adjust columns
instance_data['year'] = instance_data['complete_time'].dt.year.copy()
instance_data['month'] = instance_data['complete_time'].dt.month
instance_data['week'] = instance_data['complete_time'].dt.week
instance_data['day'] = instance_data['complete_time'].dt.day
instance_data['year_month'] = instance_data['complete_time'].values.astype('datetime64[M]')
instance_data['year_month_week'] = instance_data['complete_time'].values.astype('datetime64[W]')
instance_data['dayofyear'] = instance_data['complete_time'].apply(lambda x: get_date_str(x))

# Time series data
instance_monthly_ts_data = pd.DataFrame(instance_data.groupby(['year','month']).count().instance_id)
instance_weekly_ts_data = pd.DataFrame(instance_data.groupby(['year_month_week']).count().instance_id)
instance_weekly_ts_data.index = pd.MultiIndex.from_arrays([instance_weekly_ts_data.index.year, 
                                                            instance_weekly_ts_data.index.month, 
                                                            instance_weekly_ts_data.index.day], 
                                                            names=['Year','Month','Week'])

# Time series data by escalation
instance_esc_monthly_ts_data = pd.DataFrame(instance_data.groupby(['year','month','escalation']).count().instance_id)
instance_esc_monthly_ts_data = instance_esc_monthly_ts_data.unstack().fillna(0).instance_id
instance_esc_monthly_ts_data.to_csv(CHART_PATH + 'instance_esc_monthly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

instance_esc_weekly_ts_data = pd.DataFrame(instance_data.groupby(['year_month_week', 'escalation']).count()).instance_id
instance_esc_weekly_ts_data = instance_esc_weekly_ts_data.unstack().fillna(0)
instance_esc_weekly_ts_data.index = pd.MultiIndex.from_arrays([instance_esc_weekly_ts_data.index.year, 
                                                      instance_esc_weekly_ts_data.index.month, 
                                                      instance_esc_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])
instance_esc_weekly_ts_data.to_csv(CHART_PATH + 'instance_esc_weekly_ts_data' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

print(len(instance_data))
instance_data.head(2)

print(len(instance_monthly_ts_data))
instance_monthly_ts_data.head(2)

print(len(instance_weekly_ts_data))
instance_weekly_ts_data.head(2)

print(len(instance_esc_monthly_ts_data))
instance_esc_monthly_ts_data.head(2)

print(len(instance_esc_weekly_ts_data))
instance_esc_weekly_ts_data.head(2)


#### Escalation Distribution ####
esc_dist = pd.concat([instance_data['escalation'].value_counts(0), 
                      instance_data['escalation'].value_counts(1)], axis=1)
esc_dist.columns = ['esc_count', 'esc_pct']
esc_dist = esc_dist.sort_index()
esc_dist

get_pie_chart(esc_dist, 'Escalation Distribution', 'esc_dist_', 'severe')


#### Escalation-Account Distribution ####

#### All Accounts for All Time ####
# Account source data
account_source = pd.concat([account['account_source_id'].value_counts(0), 
                                 account['account_source_id'].value_counts(1)], axis=1)
account_source.columns = ['user_count', 'user_pct']
account_source = account_source.iloc[0:3]
account_source = account_source.sort_index()
account_source

get_pie_chart(account_source, 'Account Source', 'account_source_', 'ANONYMOUS')


## All Accounts in January 2022 ##

## Account source data - jan2022 ##
temp_account = account[(account['year']==2022)&(account['month']==1)]

temp_account_source = pd.concat([temp_account['account_source_id'].value_counts(0), 
                                 temp_account['account_source_id'].value_counts(1)], axis=1)
temp_account_source.columns = ['user_count', 'user_pct']
temp_account_source = temp_account_source.sort_index()
temp_account_source

get_pie_chart(temp_account_source, 'Account Source January 2022', 'account_source_jan2022_', 'ANONYMOUS')


#### Accounts with Escalation in January 2022 ####
# Account source data, complete escalation - jan2022
temp_ids = instance_data[(instance_data['year']==2022)&(instance_data['month']==1)].account_id.unique()
temp_account = account[account['account_id'].isin(temp_ids)]

temp_account_source = pd.concat([temp_account['account_source_id'].value_counts(0), 
                                 temp_account['account_source_id'].value_counts(1)], axis=1)
temp_account_source.columns = ['user_count', 'user_pct']
temp_account_source = temp_account_source.sort_index()
temp_account_source

get_pie_chart(temp_account_source, 'Account Source January 2022', 'account_source_esc_jan2022_', 'ANONYMOUS')


#### Escalation-Account Time Series ####

# Get relative monthly proportions
esc_monthly_ts_data = instance_esc_monthly_ts_data.copy()
esc_monthly_ts_data['total'] = esc_monthly_ts_data[['mild','moderate','severe']].sum(axis=1).values
esc_monthly_ts_data[['mild_rel_prop','moderate_rel_prop','severe_rel_prop']] = esc_monthly_ts_data[['mild','moderate','severe']].div(esc_monthly_ts_data['total'], axis=0)

# Get mnthly rates based on different "denominators"
esc_monthly_ts_data = esc_monthly_ts_data.merge(acct_monthly_ts_data, how='inner', left_index=True, right_index=True)
esc_monthly_ts_data[['mild_acct_rate', 'moderate_acct_rate', 'severe_acct_rate','total_acct_rate']] = esc_monthly_ts_data[['mild','moderate','severe','total']].div(esc_monthly_ts_data['account_id'], axis=0)
esc_monthly_ts_data[['mild_acct_rate100', 'moderate_acct_rate100', 'severe_acct_rate100','total_acct_rate100']] = esc_monthly_ts_data[['mild_acct_rate', 'moderate_acct_rate', 'severe_acct_rate','total_acct_rate']]*100

# Save monthly data
esc_monthly_ts_data.to_csv(CHART_PATH + 'esc_monthly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
esc_monthly_ts_data.describe().to_csv(CHART_PATH + 'esc_monthly_ts_stats_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Get relative weekly proportions
esc_weekly_ts_data = instance_esc_weekly_ts_data.copy()
esc_weekly_ts_data['total'] = esc_weekly_ts_data[['mild','moderate','severe']].sum(axis=1).values
esc_weekly_ts_data[['mild_rel_prop','moderate_rel_prop','severe_rel_prop']] = esc_weekly_ts_data[['mild','moderate','severe']].div(esc_weekly_ts_data['total'], axis=0)

# Get weekly rates based on different "denominators"
esc_weekly_ts_data = esc_weekly_ts_data.merge(acct_weekly_ts_data, how='inner', left_index=True, right_index=True)
esc_weekly_ts_data[['mild_acct_rate', 'moderate_acct_rate', 'severe_acct_rate','total_acct_rate']] = esc_weekly_ts_data[['mild','moderate','severe','total']].div(esc_weekly_ts_data['account_id'], axis=0)
esc_weekly_ts_data[['mild_acct_rate100', 'moderate_acct_rate100', 'severe_acct_rate100','total_acct_rate100']] = esc_weekly_ts_data[['mild_acct_rate', 'moderate_acct_rate', 'severe_acct_rate','total_acct_rate']]*100

# Save weekly data
esc_weekly_ts_data.to_csv(CHART_PATH + 'esc_weekly_ts_data_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')
esc_weekly_ts_data.describe().to_csv(CHART_PATH + 'esc_weekly_ts_stats_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

#### Time from account creation to escalation ####
acct_esc_time = instance_data.merge(account[['account_id', 'created']], 
                                    how='inner', 
                                    left_on='account_id', 
                                    right_on='account_id')
acct_esc_time['acct_esc_tdelta'] = (acct_esc_time['complete_time'] - acct_esc_time['created'])
acct_esc_time_data = acct_esc_time['acct_esc_tdelta']
acct_esc_time_dist = pd.concat([acct_esc_time_data.value_counts(0,bins=100),
                                acct_esc_time_data.value_counts(1,bins=100)], 
                               axis=1).sort_index()

# Format dist data
new_idx = [str(abs(left))+'-'+str(right)+' days' for left,right in zip(acct_esc_time_dist.index.left.days,acct_esc_time_dist.index.right.days)]
acct_esc_time_dist.index = new_idx
acct_esc_time_dist.columns = ['acct_esc_time_count','acct_esc_time_pct']

print(len(esc_monthly_ts_data))
esc_monthly_ts_data.head(2)

print(len(esc_weekly_ts_data))
esc_weekly_ts_data.head(2)

print(len(acct_esc_time))
acct_esc_time.head(2)

print(len(acct_esc_time_dist),'\n')
print(acct_esc_time_dist.sum(),'\n')
print(acct_esc_time_dist.iloc[0:2].sum(),'\n')
print(acct_esc_time_dist.iloc[0:4].sum())
acct_esc_time_dist.head(2)


#### Monthly Values ####
# Escalation counts
text_pad = 8
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

use_cols = ['mild','moderate','severe']
plt_main = esc_monthly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
ax.set_title('Monthly Counts of Escalations', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(len(esc_monthly_ts_data)))
ax.set_xticklabels(xlabels, rotation=90)
ax.set_ylabel('Escalation Count', fontsize=14)
ax.set_xlim(-1,len(esc_monthly_ts_data))
ax.set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_dist_monthly_')


# Escalation and Account counts
text_pad = 8
bar_width = 0.35
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

ax_temp = ax.twinx()
plt_twin = esc_monthly_ts_data['account_id'].plot(ax=ax_temp, kind='bar', width=bar_width, color='grey', alpha=0.5, position=0)
ax_temp.set_ylabel('Account Count', fontsize=14)
ax_temp.set_ylim(0,esc_monthly_ts_data['account_id'].max()*1.1)
ax_temp.legend(['accounts'], loc='upper right')

use_cols = ['mild','moderate','severe']
plt_main = esc_monthly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
ax.set_title('Monthly Counts of Escalations and Accounts', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(len(esc_monthly_ts_data)))
ax.set_xticklabels(xlabels, rotation=90)
ax.set_ylabel('Escalation Count', fontsize=14)
ax.set_xlim(-1,len(esc_monthly_ts_data))
ax.set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_dist_acct_monthly_')


# Rates of escalations per account
text_pad = 8
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

use_cols = ['mild_acct_rate','moderate_acct_rate','severe_acct_rate']
plt_main = esc_monthly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
ax.set_title('Monthly Rates of Escalations per Account', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(len(esc_monthly_ts_data)))
ax.set_xticklabels(xlabels, rotation=90)
ax.set_ylabel('Escalations per Account', fontsize=14)
ax.set_xlim(-1,len(esc_monthly_ts_data))
ax.set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_acct_rate_monthly_')


#### Combined fig (previous three) ####
# Define fig
suptitle = 'Cobalt Escalations and Accounts'
fig, axes = plt.subplots(3,1,figsize=(16,24), facecolor='whitesmoke')
fig.suptitle(suptitle, fontsize=20, y=0.92)
plt.grid(True, linestyle='--')

# Plot 1: Escalation counts
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

use_cols = ['mild','moderate','severe']
plt1 = esc_monthly_ts_data[use_cols].plot(ax=axes[0], kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
axes[0].set_title('Monthly Counts of Escalations', fontsize=18)
axes[0].set_xlabel('Date', fontsize=14)
axes[0].set_xticks(np.arange(len(esc_monthly_ts_data)))
axes[0].set_xticklabels(xlabels, rotation=90)
axes[0].set_ylabel('Escalation Count', fontsize=14)
axes[0].set_xlim(-1,len(esc_monthly_ts_data))
axes[0].set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
axes[0].legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

# Plot 2: Escalation and account counts
bar_width = 0.35
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

ax2_twin = axes[1].twinx()
plt2_twin = esc_monthly_ts_data['account_id'].plot(ax=ax2_twin, kind='bar', width=bar_width, color='grey', alpha=0.5, position=0)
ax2_twin.set_ylabel('Account Count', fontsize=14)
ax2_twin.set_ylim(0,esc_monthly_ts_data['account_id'].max()*1.1)
ax2_twin.legend(['accounts'], loc='upper right')

use_cols = ['mild','moderate','severe']
plt2 = esc_monthly_ts_data[use_cols].plot(ax=axes[1], kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
axes[1].set_title('Monthly Counts of Escalations and Accounts', fontsize=18)
axes[1].set_xlabel('Date', fontsize=14)
axes[1].set_xticks(np.arange(len(esc_monthly_ts_data)))
axes[1].set_xticklabels(xlabels, rotation=90)
axes[1].set_ylabel('Escalation Count', fontsize=14)
axes[1].set_xlim(-1,len(esc_monthly_ts_data))
axes[1].set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
axes[1].legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

# Plot 3: Escalation per account rates
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

use_cols = ['mild_acct_rate','moderate_acct_rate','severe_acct_rate']
plt_main = esc_monthly_ts_data[use_cols].plot(ax=axes[2], kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
axes[2].set_title('Monthly Rates of Escalations per Account', fontsize=18)
axes[2].set_xlabel('Date', fontsize=14)
axes[2].set_xticks(np.arange(len(esc_monthly_ts_data)))
axes[2].set_xticklabels(xlabels, rotation=90)
axes[2].set_ylabel('Escalations per Account', fontsize=14)
axes[2].set_xlim(-1,len(esc_monthly_ts_data))
axes[2].set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
axes[2].legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_acct_comb_monthly_')


#### By Account Source ####

text_pad = 8
bar_width = 0.35
xlabels = get_ts_xlabels(index=esc_monthly_ts_data.index, time='monthly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

ax_temp = ax.twinx()
colors = plt.cm.seismic(np.linspace(0, 0.45, 5))
plt_twin = acct_src_ts_data.plot(ax=ax_temp, kind='bar', width=bar_width, stacked=True, color=colors, alpha=0.7, position=0)
ax_temp.set_ylabel('Account Count', fontsize=14)
ax_temp.set_ylim(0,esc_monthly_ts_data['account_id'].max()*1.1)
ax_temp.legend([item.lower() for item in acct_src_ts_data.columns], title="Account Source", loc='upper right')

use_cols = ['mild','moderate','severe']
plt_main = esc_monthly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], alpha=0.8, position=1)
ax.set_title('Monthly Counts of Escalations and Accounts', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(len(esc_monthly_ts_data)))
ax.set_xticklabels(xlabels, rotation=90)
ax.set_ylabel('Escalation Count', fontsize=14)
ax.set_xlim(-1,len(esc_monthly_ts_data))
ax.set_ylim(0,esc_monthly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_monthly_ts_data[use_cols].columns, title='Severity Level', ncol=3, loc='upper left')

save_figure(fig,FIGURE_PATH,'escDist_acctSrc_monthly_')

(acct_src_ts_data['ANONYMOUS']/acct_src_ts_data.sum(axis=1)).plot()

(esc_monthly_ts_data['total']/acct_src_ts_data.sum(axis=1)).plot()

(esc_monthly_ts_data['mild']/acct_src_ts_data.sum(axis=1)).plot()

(esc_monthly_ts_data['moderate']/acct_src_ts_data.sum(axis=1)).plot()

(esc_monthly_ts_data['severe']/acct_src_ts_data.sum(axis=1)).plot()


#### Weekly Values ####
# Escalation counts
text_pad = 8
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_weekly_ts_data.index, time='weekly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

use_cols = ['mild','moderate','severe']
plt_main = esc_weekly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
ax.set_title('Weekly Counts of Escalations', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(0,len(esc_weekly_ts_data),2))
ax.set_xticklabels(xlabels[::2], rotation=90)
ax.set_ylabel('Escalation Count', fontsize=14)
ax.set_xlim(-1,len(esc_weekly_ts_data))
ax.set_ylim(0,esc_weekly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_weekly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_dist_weekly_')


# Escalation and Account counts
text_pad = 8
bar_width = 0.35
xlabels = get_ts_xlabels(index=esc_weekly_ts_data.index, time='weekly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

ax_temp = ax.twinx()
plt_twin = esc_weekly_ts_data['account_id'].plot(ax=ax_temp, kind='bar', width=bar_width, color='grey', alpha=0.5, position=0)
ax_temp.set_ylabel('Account Count', fontsize=14)
ax_temp.set_ylim(0,esc_weekly_ts_data['account_id'].max()*1.1)
ax_temp.legend(['accounts'], loc='upper right')

use_cols = ['mild','moderate','severe']
plt_main = esc_weekly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
ax.set_title('Weekly Counts of Escalations and Accounts', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(0,len(esc_weekly_ts_data),2))
ax.set_xticklabels(xlabels[::2], rotation=90)
ax.set_ylabel('Escalation Count', fontsize=14)
ax.set_xlim(-1,len(esc_weekly_ts_data))
ax.set_ylim(0,esc_weekly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_weekly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_dist_acct_weekly_')


# Rates of escalations per account
text_pad = 8
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_weekly_ts_data.index, time='weekly')

fig, ax = plt.subplots(figsize=(16,8), facecolor='whitesmoke')
plt.grid(True, linestyle='--')

use_cols = ['mild_acct_rate','moderate_acct_rate','severe_acct_rate']
plt_main = esc_weekly_ts_data[use_cols].plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
ax.set_title('Weekly Rates of Escalations per Account', fontsize=18)
ax.set_xlabel('Date', fontsize=14)
ax.set_xticks(np.arange(0,len(esc_weekly_ts_data),2))
ax.set_xticklabels(xlabels[::2], rotation=90)
ax.set_ylabel('Escalations per Account', fontsize=14)
ax.set_xlim(-1,len(esc_weekly_ts_data))
ax.set_ylim(0,esc_weekly_ts_data[use_cols].sum(axis=1).max()*1.1)
ax.legend(esc_weekly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_acct_rate_weekly_')


#### Combined fig (previous three weekly) ####
# Define fig
suptitle = 'Cobalt Escalations and Accounts'
fig, axes = plt.subplots(3,1,figsize=(16,24), facecolor='whitesmoke')
fig.suptitle(suptitle, fontsize=20, y=0.92)
plt.grid(True, linestyle='--')

# Plot 1: Escalation counts
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_weekly_ts_data.index, time='weekly')

use_cols = ['mild','moderate','severe']
plt1 = esc_weekly_ts_data[use_cols].plot(ax=axes[0], kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
axes[0].set_title('Weekly Counts of Escalations', fontsize=18)
axes[0].set_xlabel('Date', fontsize=14)
axes[0].set_xticks(np.arange(0,len(esc_weekly_ts_data),2))
axes[0].set_xticklabels(xlabels[::2], rotation=90)
axes[0].set_ylabel('Escalation Count', fontsize=14)
axes[0].set_xlim(-1,len(esc_weekly_ts_data))
axes[0].set_ylim(0,esc_weekly_ts_data[use_cols].sum(axis=1).max()*1.1)
axes[0].legend(esc_weekly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

# Plot 2: Escalation and account counts
bar_width = 0.35
xlabels = get_ts_xlabels(index=esc_weekly_ts_data.index, time='weekly')

ax2_twin = axes[1].twinx()
plt2_twin = esc_weekly_ts_data['account_id'].plot(ax=ax2_twin, kind='bar', width=bar_width, color='grey', alpha=0.5, position=0)
ax2_twin.set_ylabel('Account Count', fontsize=14)
ax2_twin.set_ylim(0,esc_weekly_ts_data['account_id'].max()*1.1)
ax2_twin.legend(['accounts'], loc='upper right')

use_cols = ['mild','moderate','severe']
plt2 = esc_weekly_ts_data[use_cols].plot(ax=axes[1], kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
axes[1].set_title('Weekly Counts of Escalations and Accounts', fontsize=18)
axes[1].set_xlabel('Date', fontsize=14)
axes[1].set_xticks(np.arange(0,len(esc_weekly_ts_data),2))
axes[1].set_xticklabels(xlabels[::2], rotation=90)
axes[1].set_ylabel('Escalation Count', fontsize=14)
axes[1].set_xlim(-1,len(esc_weekly_ts_data))
axes[1].set_ylim(0,esc_weekly_ts_data[use_cols].sum(axis=1).max()*1.1)
axes[1].legend(esc_weekly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

# Plot 3: Escalation per account rates
bar_width = 0.5
xlabels = get_ts_xlabels(index=esc_weekly_ts_data.index, time='weekly')

use_cols = ['mild_acct_rate','moderate_acct_rate','severe_acct_rate']
plt_main = esc_weekly_ts_data[use_cols].plot(ax=axes[2], kind='bar', width=bar_width, stacked=True, color=['green','orange','red'], position=1)
axes[2].set_title('Weekly Rates of Escalations per Account', fontsize=18)
axes[2].set_xlabel('Date', fontsize=14)
axes[2].set_xticks(np.arange(0,len(esc_weekly_ts_data),2))
axes[2].set_xticklabels(xlabels[::2], rotation=90)
axes[2].set_ylabel('Escalations per Account', fontsize=14)
axes[2].set_xlim(-1,len(esc_weekly_ts_data))
axes[2].set_ylim(0,esc_weekly_ts_data[use_cols].sum(axis=1).max()*1.1)
axes[2].legend(esc_weekly_ts_data[use_cols].columns, title='Severity Level', loc='upper left')

save_figure(fig,FIGURE_PATH,'esc_acct_comb_weekly_')


#### Escalation and Appointment Distributions ####
## All Appointments, All Time ##
aptRole_dist = pd.concat([appointment['support_role_id'].value_counts(0), 
                                 appointment['support_role_id'].value_counts(1)], axis=1)
aptRole_dist.columns = ['appt_count', 'appt_pct']
aptRole_dist = aptRole_dist.sort_index()
aptRole_dist

get_pie_chart(aptRole_dist, 'Distribution of All Appointments - All Time', 
              'aptRole_dist_', '')


## Appointments last 90 Days
aptRole_dist_past90 = pd.concat([appointment_past_90day['support_role_id'].value_counts(0), 
                                 appointment_past_90day['support_role_id'].value_counts(1)], axis=1)
aptRole_dist_past90.columns = ['appt_count', 'appt_pct']
aptRole_dist_past90 = aptRole_dist_past90.sort_index()
aptRole_dist_past90

get_pie_chart(aptRole_dist_past90, 'Distribution of All Appointments - Past 90 Days', 
              'aptRole_dist_past90_', '')


#### Appointments With Escalation by Severity ####
# Appointments by escalation bracket
# Note: 1-2 patients have two escalations (somehow) and one apt that is being double counted
# need escalation datetimes to connect appointments to the appropriate escalation event
# this will be common once repeat assessment sequences (instances leanding to escalations) are implemented
instance_all = instance_data.copy()
account_id_all = instance_all['account_id'].unique()
appointment_all = appointment[appointment['account_id'].isin(account_id_all)]

instance_mild = instance_data[instance_data['escalation']=='mild']
account_id_mild = instance_mild['account_id'].unique()
appointment_mild = appointment[appointment['account_id'].isin(account_id_mild)]

instance_moderate = instance_data[instance_data['escalation']=='moderate']
account_id_moderate = instance_moderate['account_id'].unique()
appointment_moderate = appointment[appointment['account_id'].isin(account_id_moderate)]

instance_severe = instance_data[instance_data['escalation']=='severe']
account_id_severe = instance_severe['account_id'].unique()
appointment_severe = appointment[appointment['account_id'].isin(account_id_severe)]


#### Appointments with escalation, All Time ####
aptRole_dist_escAll = pd.concat([appointment_all['support_role_id'].value_counts(0), 
                                 appointment_all['support_role_id'].value_counts(1)], axis=1)
aptRole_dist_escAll.columns = ['appt_count', 'appt_pct']
aptRole_dist_escAll = aptRole_dist_escAll.sort_index()
aptRole_dist_escAll

get_pie_chart(aptRole_dist_escAll, 'Distribution of Appointments with Associated Escalation - All Time', 
              'aptRole_dist_escAll_', '')


#### Appointments with escalation, Last 90 Days ####
appointment_all_past90 = appointment_past_90day[appointment_past_90day['account_id'].isin(account_id_all)]

aptRole_dist_escAll_past90 = pd.concat([appointment_all_past90['support_role_id'].value_counts(0), 
                                 appointment_all_past90['support_role_id'].value_counts(1)], axis=1)
aptRole_dist_escAll_past90.columns = ['appt_count', 'appt_pct']
aptRole_dist_escAll_past90 = aptRole_dist_escAll_past90.sort_index()
aptRole_dist_escAll_past90

get_pie_chart(aptRole_dist_escAll_past90, 'Distribution of Appointments with Associated Escalation - Past 90 Days', 
              'aptRole_dist_escAll_past90_', '')


#### Summary Tables ####
# Appointments with and without escalation, for all time and in the past 90 days, by provider role
aptRole_dist_source = pd.concat({'All Appts':aptRole_dist, 
                              'All Appts - Past 90 Days':aptRole_dist_past90, 
                              'Appts with Escalation':aptRole_dist_escAll, 
                              'Appts with Escalation - Past 90 Days':aptRole_dist_escAll_past90}, names=['Appt Data', 'Provider Role'])
aptRole_dist_source

# Appointments with and without escalation, aggregated for all time and in the past 90 days
aptRole_dist_source.groupby(level=0).sum()

# Save chart
aptRole_dist_source.to_csv(CHART_PATH + 'aptRole_dist_source_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')


#### Escalation Appointment Distribution by Escalation Severity ####
## Appointments With Mild Escalation ##
aptRole_dist_escMild = pd.concat([appointment_mild['support_role_id'].value_counts(0), 
                                  appointment_mild['support_role_id'].value_counts(1)], axis=1)
aptRole_dist_escMild.columns = ['appt_count', 'appt_pct']
aptRole_dist_escMild = aptRole_dist_escMild.sort_index()
aptRole_dist_escMild

get_pie_chart(aptRole_dist_escMild, 'Distribution of Appointments with Mild Escalation', 
              'aptRole_dist_escMild_', '')


## Appointments With Moderate Escalation ##
aptRole_dist_escModerate = pd.concat([appointment_moderate['support_role_id'].value_counts(0), 
                                  appointment_moderate['support_role_id'].value_counts(1)], axis=1)
aptRole_dist_escModerate.columns = ['appt_count', 'appt_pct']
aptRole_dist_escModerate = aptRole_dist_escModerate.sort_index()
aptRole_dist_escModerate

get_pie_chart(aptRole_dist_escModerate, 'Distribution of Appointments with Moderate Escalation', 
              'aptRole_dist_escModerate_', '')


## Appointments With Severe Escalation ##
aptRole_dist_escSevere = pd.concat([appointment_severe['support_role_id'].value_counts(0), 
                                  appointment_severe['support_role_id'].value_counts(1)], axis=1)
aptRole_dist_escSevere.columns = ['appt_count', 'appt_pct']
aptRole_dist_escSevere = aptRole_dist_escSevere.sort_index()
aptRole_dist_escSevere

get_pie_chart(aptRole_dist_escSevere, 'Distribution of Appointments with Severe Escalation', 
              'aptRole_dist_escSevere_', '')


#### Summary Tables ####
# Appointments by escalation severity and provider role
aptRole_dist_esc = pd.concat({'All':aptRole_dist_escAll, 
                              'Mild':aptRole_dist_escMild, 
                              'Moderate':aptRole_dist_escModerate, 
                              'Severe':aptRole_dist_escSevere}, names=['Escalation', 'Provider Role'])
aptRole_dist_esc
aptRole_dist_esc.to_csv(CHART_PATH + 'aptRole_dist_esc_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# Same as above, but in wide format
aptRole_dist_esc_unstacked = aptRole_dist_esc.unstack().fillna(0)
aptRole_dist_esc_unstacked
aptRole_dist_esc_unstacked.to_csv(CHART_PATH + 'aptRole_dist_esc_unstacked_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')


#### Escalation Appointment Time Series ####
# Number of appointments booked per month by escalation bracket and provider role
# Mild
instanceMild_aptRole_ts_data = pd.DataFrame(appointment_mild.groupby(['apt_year','apt_month','support_role_id']).count().appointment_id)
instanceMild_aptRole_ts_data = instanceMild_aptRole_ts_data.unstack().fillna(0).appointment_id
instanceMild_aptRole_ts_data.index.names = ['year','month']
instanceMild_aptRole_ts_data = instanceMild_aptRole_ts_data.merge(instance_esc_monthly_ts_data['mild'], 
                                                                  how='outer', 
                                                                  left_index=True, 
                                                                  right_index=True).fillna(0)
instanceMild_aptRole_ts_data = instanceMild_aptRole_ts_data.rename(columns={'mild':'ESCALATION'})

instanceMild_aptRole_weekly_ts_data = pd.DataFrame(appointment_mild.groupby(['apt_year_month_week', 'support_role_id']).count()).appointment_id
instanceMild_aptRole_weekly_ts_data = instanceMild_aptRole_weekly_ts_data.unstack().fillna(0)
instanceMild_aptRole_weekly_ts_data.index = pd.MultiIndex.from_arrays([instanceMild_aptRole_weekly_ts_data.index.year, 
                                                      instanceMild_aptRole_weekly_ts_data.index.month, 
                                                      instanceMild_aptRole_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])


# Moderate
instanceModerate_aptRole_ts_data = pd.DataFrame(appointment_moderate.groupby(['apt_year','apt_month','support_role_id']).count().appointment_id)
instanceModerate_aptRole_ts_data = instanceModerate_aptRole_ts_data.unstack().fillna(0).appointment_id
instanceModerate_aptRole_ts_data.index.names = ['year','month']
instanceModerate_aptRole_ts_data = instanceModerate_aptRole_ts_data.merge(instance_esc_monthly_ts_data['moderate'], 
                                                                  how='outer', 
                                                                  left_index=True, 
                                                                  right_index=True).fillna(0)
instanceModerate_aptRole_ts_data = instanceModerate_aptRole_ts_data.rename(columns={'moderate':'ESCALATION'})

instanceModerate_aptRole_weekly_ts_data = pd.DataFrame(appointment_moderate.groupby(['apt_year_month_week', 'support_role_id']).count()).appointment_id
instanceModerate_aptRole_weekly_ts_data = instanceModerate_aptRole_weekly_ts_data.unstack().fillna(0)
instanceModerate_aptRole_weekly_ts_data.index = pd.MultiIndex.from_arrays([instanceModerate_aptRole_weekly_ts_data.index.year, 
                                                      instanceModerate_aptRole_weekly_ts_data.index.month, 
                                                      instanceModerate_aptRole_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])


# Severe
instanceSevere_aptRole_ts_data = pd.DataFrame(appointment_severe.groupby(['apt_year','apt_month','support_role_id']).count().appointment_id)
instanceSevere_aptRole_ts_data = instanceSevere_aptRole_ts_data.unstack().fillna(0).appointment_id
instanceSevere_aptRole_ts_data.index.names = ['year','month']
instanceSevere_aptRole_ts_data = instanceSevere_aptRole_ts_data.merge(instance_esc_monthly_ts_data['severe'], 
                                                                  how='outer', 
                                                                  left_index=True, 
                                                                  right_index=True).fillna(0)
instanceSevere_aptRole_ts_data = instanceSevere_aptRole_ts_data.rename(columns={'severe':'ESCALATION'})

instanceSevere_aptRole_weekly_ts_data = pd.DataFrame(appointment_severe.groupby(['apt_year_month_week', 'support_role_id']).count()).appointment_id
instanceSevere_aptRole_weekly_ts_data = instanceSevere_aptRole_weekly_ts_data.unstack().fillna(0)
instanceSevere_aptRole_weekly_ts_data.index = pd.MultiIndex.from_arrays([instanceSevere_aptRole_weekly_ts_data.index.year, 
                                                      instanceSevere_aptRole_weekly_ts_data.index.month, 
                                                      instanceSevere_aptRole_weekly_ts_data.index.day], 
                                                      names=['Year','Month','Week'])


# Plot incremental and cumulative appointments
def get_escalation_appointments(esc_data,appt_data,cumulative=False):
    # Plot parameters
    bar_width = 0.4
    title_prefix = ''
    save_suffix = ''

    # Set appt_data column order for color/legend coherence
    if cumulative:
        title_prefix = 'Cumulative '
        save_suffix = 'cumulative_'
        esc_data = [item.cumsum() for item in esc_data]
        appt_data = [item.cumsum() for item in appt_data]
    support_roles = sorted(provider_support_role.support_role_id.unique())
    support_role_colors = ['#003f5c','#374c80','#7a5195','#bc5090','#ef5675','#ff764a','#ffa600']
    titles = ['Mild Escalation','Moderate Escalation','Severe Escalation']
    for i in range(len(appt_data)):
        for role in support_roles:
            if role not in appt_data[i].columns:
                appt_data[i][role] = 0.0
        appt_data[i] = appt_data[i][support_roles]

    # Get x-axis labels and range
    xlabels = get_ts_xlabels(index=appt_data[np.argmax([len(df) for df in appt_data])].index, time='monthly')
    xmax = max([len(df) for df in appt_data])
    appt_ymax = max([df.sum(axis=1).max() for df in appt_data])
    esc_ymax = max([df.max()for df in esc_data])

    # Plot
    suptitle = title_prefix + 'Number of Booked Appointments per Month by Escalation and Provider Role'
    fig, axes = plt.subplots(3,1,figsize=(16,20), facecolor='whitesmoke')
    fig.suptitle(suptitle, fontsize=20, y=0.92)
    plt.grid(True, linestyle='--')

    for appt,esc,ax,title in zip(appt_data,esc_data,axes,titles):

        ax_temp = ax.twinx()
        ax_temp.set_ylabel('Escalation Count', fontsize=14)
        ax_temp.set_ylim(0,max(appt_ymax,esc_ymax)+5)
        esc.plot(ax=ax_temp, kind='bar', width=bar_width, color='grey', alpha=0.5, position=0)

        appt.plot(ax=ax, kind='bar', width=bar_width, stacked=True, color=support_role_colors, position=1)
        ax.set_title(title, fontsize=18)
        ax.set_xlabel('Date', fontsize=14)
        ax.set_xticks(np.arange(xmax))
        ax.set_xticklabels(xlabels, rotation=0)
        ax.set_xlim(-1,xmax)
        ax.set_ylabel('Appointment Count', fontsize=14)
        ax.set_ylim(0,appt_ymax+5)
        
    name = 'esc_apptRole_dist_monthly_' + save_suffix
    save_figure(fig,FIGURE_PATH,name)

# Build escalation and appointment datasets
esc_data = [instanceMild_aptRole_ts_data['ESCALATION'],instanceModerate_aptRole_ts_data['ESCALATION'],instanceSevere_aptRole_ts_data['ESCALATION']]
appt_data = [instanceMild_aptRole_ts_data,instanceModerate_aptRole_ts_data,instanceSevere_aptRole_ts_data]

# Plot incremental chnges per month
get_escalation_appointments(esc_data,appt_data,cumulative=False)

# Plot cumulative growth per month
get_escalation_appointments(esc_data,appt_data,cumulative=True)


#### TODO - 04/26/2022: ####
# availability (also email mark about historical data) 
# covid data second y-axis on escalation distribution figures
# covid data correlation/regression with escalation (total, by escalation severity), appt, user, etc.

# NEW - 05/19/2022:
# try graphing ratio instead
# try a heatmap visualization instead of stacked bars - just as above - months along x, roles along y, color for count


aptRole_ts_data.tail()

aptRole_avail_ts_data.tail()

# # TEMPTEMPTEMPTEMPTEMPTEMPTEMPTEMPTEMPTEMPTEMPTEMP 
# * (DONE - 02/02/2022) all unique account id's on Cobalt that have attempted an assessment, scheduled an appointment, or viewed content
# * (DONE - 02/02/2022) all unique account id's having completed the full assessment sequence and received an escalation - this makes the most sense given the next two metrics (moderate and severe counts/pcts)
# * (DONE - 02/02/2022) all unique appointment id's on Cobalt from account id's that have received an escalation

# In[218]:


class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   ITALICS = '\033[3m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'


attempt_cols = ['PHQ4_attempts','PHQ9_attempts','GAD7_attempts','PCPTSD_attempts']
instance_attempts = account_instance[account_instance[attempt_cols].sum(axis=1) > 0]

instance_attempts_account_ids = set(instance_attempts['account_id'].unique())
appointment_account_ids = set(appointment['account_id'].unique())
activity_tracking_account_ids = set(activity_tracking[(activity_tracking['activity_type_id']=='CONTENT') & 
                                                  (activity_tracking['activity_action_id']=='VIEW')]['account_id'].unique())
active_account_ids = instance_attempts_account_ids.union(appointment_account_ids,activity_tracking_account_ids)


instance_escalation = account_instance[account_instance['instance_complete']==1]
instance_escalation_moderate = instance_escalation[instance_escalation['escalation']=='moderate']
instance_escalation_severe = instance_escalation[instance_escalation['escalation']=='severe']

instance_escalation_account_ids = instance_escalation['account_id'].unique()
instance_escalation_moderate_account_ids = instance_escalation_moderate['account_id'].unique()
instance_escalation_severe_account_ids = instance_escalation_severe['account_id'].unique()

mod_pct_acct = (len(instance_escalation_moderate_account_ids)/len(instance_escalation_account_ids))*100
mod_pct_esc = (len(instance_escalation_moderate)/len(instance_escalation))*100
sev_pct_acct = (len(instance_escalation_severe_account_ids)/len(instance_escalation_account_ids))*100
sev_pct_esc = (len(instance_escalation_severe)/len(instance_escalation))*100

appointment_escalation = appointment[appointment['account_id'].isin(instance_escalation_account_ids)]
appt_acct_pct = (len(appointment_escalation.account_id.unique())/len(instance_escalation_account_ids))*100

#### Final Summary ####
# Active Accounts
print(color.BOLD + 'Active Accounts:' + color.END)
print(color.BOLD + '{:d} '.format(len(active_account_ids)) + color.END + 'unique, active accounts accessed mental health and well-being resources on the Cobalt platform')
print(color.BOLD + '\t{:d} '.format(len(instance_attempts_account_ids)) + color.END + 'distinct accounts attempted assessments')
print(color.BOLD + '\t{:d} '.format(len(appointment_account_ids)) + color.END + 'distinct accounts booked' + color.BOLD + ' {:d} '.format(len(appointment)) + color.END + 'appointments')
print(color.BOLD + '\t{:d} '.format(len(activity_tracking_account_ids)) + color.END + 'distinct accounts engaged with content')
print()

# Assessments and Escalations
print(color.BOLD + 'Assessments and Escalations:' + color.END)
print(color.BLUE+color.BOLD + '{:d} '.format(len(instance_escalation_account_ids)) + color.END+color.END + 
      'unique accounts completed mental health assessments, resulting in' + 
      color.BLUE+color.BOLD +' {:d} '.format(len(instance_escalation)) + color.END+color.END + 'distinct escalations')

print(color.BOLD + '\tModerate Escalation:' + color.END)
print('\t' + color.YELLOW+color.BOLD + '{:d}({:2.2f}%) '.format(len(instance_escalation_moderate_account_ids),mod_pct_acct) + color.END+color.END + 
      'unique accounts met moderate criteria, resulting in' + 
      color.YELLOW+color.BOLD +' {:d}({:2.2f}%) '.format(len(instance_escalation_moderate),mod_pct_esc) + color.END+color.END + 'moderate escalations')

print(color.BOLD + '\tSevere Escalation:' + color.END)
print('\t' + color.RED+color.BOLD + '{:d}({:2.2f}%) '.format(len(instance_escalation_severe_account_ids),sev_pct_acct) + color.END+color.END + 
      'unique accounts met severe criteria, resulting in' + 
      color.RED+color.BOLD +' {:d}({:2.2f}%) '.format(len(instance_escalation_severe),sev_pct_esc) + color.END+color.END + 'severe escalations')
print()

# Appointments
print(color.BOLD + 'Appointments:' + color.END)
print(color.BOLD + '{:d}/{:d}({:2.2f})% '.format(len(appointment_escalation.account_id.unique()),len(instance_escalation_account_ids),appt_acct_pct) + color.END + 
      'accounts with completed mental health assessments booked' + color.BOLD + ' {:d} '.format(len(appointment_escalation)) + color.END + 
      'appointments with a trained mental health professional')


#### Unused / Deprecated Code ####


"""
#### Sankey Diagram ####
# To get nodes from multi-index
list(itertools.chain.from_iterable(instance_flow.index.levels[0:]))
# To get flow sequence from multi-index
[' '.join(col).strip() for col in instance_flow.index.values]

#### Sankey Diagram - OLD method to adjust phq4 link data ####
# Get instance flow data
instance_subset,instance_flow = get_instance_flow(subset_cols, group_cols, agg_dict)
flow_data_p1 = instance_flow[instance_flow.index.isin(['no_symptoms'], level=0)].droplevel([1,2,3])
flow_data_p2 = instance_flow[~instance_flow.index.isin(['no_symptoms','inconclusive'], level=0)]

# Get sankey data
nodes_all,links_all = get_sankey_data(instance_flow)
nodes_p1,links_p1 = get_sankey_data(flow_data_p1)
nodes_p2,links_p2 = get_sankey_data(flow_data_p2)

# Adjust PHQ4 data with no_symptoms to point directly to escalation: mild
links_p1.loc[links_p1['target']!=1, 'target'] = nodes_all.loc[nodes_all['label']=='escalation: mild', 'idx'].values[0]

# Set final node and link data
nodes = nodes_all
links = pd.concat([links_p1,links_p2])

"""


"""
for bar,data in zip(ax.patches[::3],instance_esc_monthly_ts_data.values):
    total_height = data.sum()
    mild = data[0]
    moderate = data[1]
    severe = data[2]
    
    text_x = bar.get_x()+bar.get_width()/2
    text_y = total_height+text_pad
    text = str(int(total_height)) + ', (' + str("{:.1f}".format(float((mild/total_height)*100))) + '% mild)'
    ax.text(text_x, text_y, text, rotation=0, va='top', ha='center', color='black', fontsize=10, fontweight='bold')
"""


"""
data[0].plot(ax=ax[0], kind='bar', stacked=True)
ax[0].set_title('Mild Escalation', fontsize=18)
ax[0].set_xlabel('Date', fontsize=14)
ax[0].set_xticks(np.arange(xmax))
ax[0].set_xticklabels(xlabels, rotation=90)
ax[0].set_ylabel('Appointment Count', fontsize=14)
ax[0].set_xlim(-1,xmax)
#ax[0].legend(instanceMild_aptRole_ts_data.columns)

instanceModerate_aptRole_ts_data.plot(ax=ax[1], kind='bar', stacked=True)
ax[1].set_title('Moderate Escalation', fontsize=18)
ax[1].set_xlabel('Date', fontsize=14)
ax[1].set_xticks(np.arange(xmax))
ax[1].set_xticklabels(xlabels, rotation=90)
ax[1].set_ylabel('Appointment Count', fontsize=14)
ax[1].set_xlim(-1,xmax)
#ax[1].legend(instanceModerate_aptRole_ts_data.columns)

instanceSevere_aptRole_ts_data.plot(ax=ax[2], kind='bar', stacked=True)
ax[2].set_title('Severe Escalation', fontsize=18)
ax[2].set_xlabel('Date', fontsize=14)
ax[2].set_xticks(np.arange(xmax))
ax[2].set_xticklabels(xlabels, rotation=90)
ax[2].set_ylabel('Appointment Count', fontsize=14)
ax[2].set_xlim(-1,xmax)
#ax[2].legend((instanceSevere_aptRole_ts_data.columns)
"""


"""
# Exploring time between account creation and escalation
tdelta = acct_esc_time[['acct_esc_tdelta','year']]
tdelta.groupby(['year']).describe()
tdelta.groupby(['year']).apply(lambda x: x[np.abs(stats.zscore(x['acct_esc_tdelta'].dt.days)) < 1]).droplevel(level=0).groupby(['year']).describe()

"""


