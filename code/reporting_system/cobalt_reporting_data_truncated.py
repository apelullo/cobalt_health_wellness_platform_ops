#!/usr/bin/env python
# coding: utf-8

# # Cobalt Reporting Data Truncated
# * Truncated *cobalt_reporting_data.ipynb* code for faster generation of critical analytics spreadsheets

# In[1]:


# Modules
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
pd.set_option('display.precision', 4)


# # Master Data

# In[2]:


# Connect to database
read_cursor,reporting_cursor = database_connect()


# In[3]:


# List all Cobalt tables
query = """SELECT table_name FROM information_schema.tables WHERE table_schema='cobalt'"""
reporting_cursor.execute(query)
result = reporting_cursor.fetchall()

#print(len(result))
sorted(result)


# ## Institution

# ### Institution

# In[4]:


institution = get_table_data(reporting_cursor, 'institution')


# ### Institution Acount Source

# In[5]:


institution_account_source = get_table_data(reporting_cursor, 'institution_account_source')


# ### Institution Assessment

# In[6]:


institution_assessment = get_table_data(reporting_cursor, 'institution_assessment')


# ### Institution Content

# In[7]:


institution_content = get_table_data(reporting_cursor, 'institution_content')


# ## Accounts

# ### Account

# In[8]:


# Get account data
account = get_table_data(reporting_cursor, 'account')

# Adjust columns
account['year'] = account['created'].dt.year
account['month'] = account['created'].dt.month
account['week'] = account['created'].dt.isocalendar().week
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


# ## Providers

# ### Provider

# In[9]:


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


# ### Provider Support Role

# In[10]:


# Get provider role data
provider_support_role = get_table_data(reporting_cursor, 'provider_support_role')

# Filter for relevant data
provider_support_role = provider_support_role[provider_support_role['provider_id'].isin(provider['provider_id'])]


# ### Provider Appointment Type

# In[11]:


# Get provider appointment type data
provider_appointment_type = get_table_data(reporting_cursor, 'provider_appointment_type')

# Filter for relevant data
provider_appointment_type = provider_appointment_type[provider_appointment_type['provider_id'].isin(provider['provider_id'])]


# ## Appointments

# ### Appointment Type

# In[12]:


# Get appointment type data
appointment_type = get_table_data(reporting_cursor, 'appointment_type')

appointment_type_dict = dict(zip(appointment_type.appointment_type_id, appointment_type.name))


# ### Booked Appointments

# In[13]:


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


# ### Available Appointments

# In[14]:


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


# ## Group Sessions

# ### Group Event Type

# In[15]:


group_event_type = get_table_data(reporting_cursor, 'group_event_type')


# ### Group Session

# In[16]:


# Get group session data
group_session = get_table_data(reporting_cursor, 'group_session')


# ### Group Session Requests

# In[17]:


# Get group session request data
group_session_request = get_table_data(reporting_cursor, 'group_session_request')


# ### Group Session Request Status

# In[18]:


group_session_request_status = get_table_data(reporting_cursor, 'group_session_request_status')


# ### Group Session Reservations

# In[19]:


# Get group session reservation data
group_session_reservation = get_table_data(reporting_cursor, 'group_session_reservation')


# ### Group Session Response

# In[20]:


group_session_response = get_table_data(reporting_cursor, 'group_session_response')


# ### Group Session Scheduling System

# In[21]:


group_session_scheduling_system = get_table_data(reporting_cursor, 'group_session_scheduling_system')


# ### Group Session Status

# In[22]:


group_session_status = get_table_data(reporting_cursor, 'group_session_status')


# ### Group Session System

# In[23]:


group_session_system = get_table_data(reporting_cursor, 'group_session_system')


# ## Assessments

# ### Assessment

# In[24]:


# Get assessment data
assessment = get_table_data(reporting_cursor, 'assessment')
assessment_dict = dict(zip(assessment.assessment_id, assessment.assessment_type_id))

# Assessment IDs
PHQ4_id = assessment[assessment['assessment_type_id']=='PHQ4'].assessment_id.values[0]
PHQ9_id = assessment[assessment['assessment_type_id']=='PHQ9'].assessment_id.values[0]
GAD7_id = assessment[assessment['assessment_type_id']=='GAD7'].assessment_id.values[0]
PCPTSD_id = assessment[assessment['assessment_type_id']=='PCPTSD'].assessment_id.values[0]
RCT_ids = assessment[(assessment['assessment_type_id']=='PHQ9') | (assessment['assessment_type_id']=='GAD7')].assessment_id.values


# ### Assessment Type

# In[25]:


# Get assessment type data
assessment_type = get_table_data(reporting_cursor, 'assessment_type')


# ### Answer

# In[26]:


# Get answer data
answer = get_table_data(reporting_cursor, 'answer')


# ### Answer Category

# In[27]:


# Get answer category data
answer_category = get_table_data(reporting_cursor, 'answer_category')


# ### Category

# In[28]:


# Get category data
category = get_table_data(reporting_cursor, 'category')


# ### Question
# * overlapping phq4 questions excluded from phq9 and gad7

# In[29]:

"""
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


# ### Question Type

# In[30]:


# Get question type data
question_type = get_table_data(reporting_cursor, 'question_type')


# ## Activity and Content

# ### Content

# In[31]:


# Get content data
content = get_table_data(reporting_cursor, 'content')

# Filter for relevant data
content = content[content['owner_institution_id']=='PENN']


# ### Activity Tracking
# * To find the content users are consuming: activity_tracking.activity_key --> content.content_id (DEPRECATED - 01/11/2022)
# * content[content['content_id']=='0e997dda-15e1-446c-bcda-bea3a2271c60']

# In[32]:


# Get activity tracking data
activity_tracking = get_table_data(reporting_cursor, 'activity_tracking')

# Filter for relevant data
activity_tracking = activity_tracking[activity_tracking['account_id'].isin(account['account_id'])]

# Activity tracking subsets
activity_tracking_past = activity_tracking[activity_tracking['created'] <= current_date]
activity_tracking_past_30day = activity_tracking_past[activity_tracking_past['created'] >= past_30day]
activity_tracking_past_90day = activity_tracking_past[activity_tracking_past['created'] >= past_90day]


# ### Popular Content
# * Ordered by view_count for last 30 days

# In[33]:


popular_content_use_cols = ['content_id','created','content_type_id','title','description','author','duration_in_minutes','view_count']


# In[34]:


# Get content activity for last 30 days
content_activity_past_30day = activity_tracking_past_30day[activity_tracking_past_30day['activity_type_id']=='CONTENT'].copy()
content_activity_past_30day.loc[:,'context'] = content_activity_past_30day['context'].apply(lambda x: x['contentId']).copy()


# In[35]:


# Get content view counts
popular_content_past_30day = content_activity_past_30day.groupby(['context'])[['activity_tracking_id']].count()
popular_content_past_30day = popular_content_past_30day.rename(columns={'activity_tracking_id':'view_count'})
popular_content_past_30day = content.merge(popular_content_past_30day, how='inner', left_on='content_id', right_index=True)

# popular content
popular_content_past_30day = popular_content_past_30day.sort_values(['view_count'], ascending=False)[popular_content_use_cols].reset_index(drop=True)
popular_content_past_30day.to_csv(CHART_PATH + 'popular_content_past_30day_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')

# popular content grouped by type
popular_content_past_30day_grouped = popular_content_past_30day.sort_values(['content_type_id','view_count'], ascending=False)[popular_content_use_cols].reset_index(drop=True)
popular_content_past_30day_grouped.to_csv(CHART_PATH + 'popular_content_past_30day_grouped_' + str(datetime.datetime.now().date()).replace('-','') + '.csv')


# #### Content Summary

# In[36]:


# content summary
popular_content_past_30day_summary = pd.concat([popular_content_past_30day.groupby(['content_type_id'])['view_count'].count(),
                                     popular_content_past_30day.groupby(['content_type_id'])['view_count'].sum()], axis=1)
popular_content_past_30day_summary.columns = ['content_count', 'view_count']
popular_content_past_30day_summary['views_per_content'] = popular_content_past_30day_summary['view_count']/popular_content_past_30day_summary['content_count']
popular_content_past_30day_summary.to_csv(CHART_PATH + 'popular_content_past_30day_summary' + str(datetime.datetime.now().date()).replace('-','') + '.csv')


# ## Account Sessions

# ### Account Session

# In[37]:


# Get account session data
account_session = get_table_data(reporting_cursor, 'account_session')

# Filter for relevant data
account_session = account_session[account_session['account_id'].isin(account['account_id'])]

# Adjust columns
account_session['assessment_name'] = account_session['assessment_id'].map(assessment_dict)


# ### Account Session Answer

# In[38]:


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

# Merge account session answer and account account_id
use_cols = ['account_id','account_source_id','sso_id','first_name','last_name','email_address','phone_number','created']
account_merge = account[use_cols].copy()
account_merge = account_merge.rename(columns={'created':'created_account'})
account_session_answer = account_session_answer.merge(account_merge,how='inner',left_on='account_id',right_on='account_id')

# Merge account session answer and answer on answer_id
use_cols = ['answer_id','question_id','answer_text','display_order','answer_value','crisis','call']
answer_merge = answer[use_cols].copy()
account_session_answer = account_session_answer.merge(answer_merge,how='inner',left_on='answer_id', right_on='answer_id')


# # Derived Data

# ## Account Instance

# In[68]:


account_instance = pd.read_csv(DATA_PATH + 'account_instance_master_20230102.csv', index_col=0)
account_instance['complete_time'] = pd.to_datetime(account_instance['complete_time']).dt.tz_convert(tz=COBALT_TZ)


# ## Unused / Deprecated Code

# In[40]:


"""
"""
group_session.sort_values('start_date_time', ascending=False).groupby([group_session['start_date_time'].dt.year,group_session['start_date_time'].dt.month]).count()[['group_session_id']]

"""


# In[ ]:





# In[ ]:




