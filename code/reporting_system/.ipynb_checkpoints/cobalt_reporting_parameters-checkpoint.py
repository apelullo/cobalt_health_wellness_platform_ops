#!/usr/bin/env python
# coding: utf-8

#### Cobalt Reporting Parameters ####
#### Universal configuration and system-level (cobalt platform) configuration - only modify following changes to the cobalt platform that impact data structures and classifications ####

#### Modules ####
from cobalt_reporting_config import *
import pandas as pd
import datetime

#### Collections ####

month_dict = dict({1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 
                   7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec',})


appt_provider_role_dict = dict({'The Chaplain is In - for You':'CHAPLAIN',
                                '1:1 Session with Chaplain':'CHAPLAIN',
                                '1:1 with Peer':'PEER',
                                '1:1 Session with Resilience Coach':'COACH',
                                '1:1 with Care Manager':'CARE_MANAGER',
                                '1:1 Initial Appointment with Psychotherapist':'CLINICIAN','1:1 CTSA Intake Appointment':'CLINICIAN','1:1 Appointment with Psychotherapist':'CLINICIAN','CCT Intake Appointment':'CLINICIAN',
                                '1:1 Session with Psychiatrist':'PSYCHIATRIST','1:1 Psychiatrist Follow-ups':'PSYCHIATRIST','1:1 with Psychiatric Nurse Practitioner':'PSYCHIATRIST','1:1 Psych NP Follow-Up':'PSYCHIATRIST',
                                '1:1 with Dietitian':'OTHER','1:1 Session with Exercise Physiologist':'OTHER','1:1 Strength and Training Specialist':'OTHER','1:1 with Pain Specialist':'OTHER',})


#### Dates ####
# Dates and date ranges
cobalt_start_date = pd.to_datetime(COBALT_START_DATE).tz_localize(tz=COBALT_TZ)
current_date = pd.to_datetime(datetime.datetime.now()).tz_localize(tz=COBALT_TZ)
future_30day = pd.to_datetime(datetime.datetime.now() + pd.Timedelta(days=30)).tz_localize(tz=COBALT_TZ)
future_90day = pd.to_datetime(datetime.datetime.now() + pd.Timedelta(days=90)).tz_localize(tz=COBALT_TZ)
past_30day = pd.to_datetime(datetime.datetime.now() - pd.Timedelta(days=30)).tz_localize(tz=COBALT_TZ)
past_90day = pd.to_datetime(datetime.datetime.now() - pd.Timedelta(days=90)).tz_localize(tz=COBALT_TZ)

# Date indices
cobalt_date_range = pd.date_range(cobalt_start_date,current_date,freq='d')
cobalt_date_range = pd.DataFrame(cobalt_date_range, columns=['dates'])
cobalt_date_range['year'] = cobalt_date_range['dates'].dt.year
cobalt_date_range['month'] = cobalt_date_range['dates'].dt.month
cobalt_date_range['year_month_week'] = cobalt_date_range['dates'].values.astype('datetime64[W]')

month_index_df = pd.DataFrame(index=cobalt_date_range.groupby(['year','month']).count().index)
week_index_df = pd.DataFrame(index=cobalt_date_range.groupby(['year_month_week']).count().index)
week_index_df.index = pd.MultiIndex.from_arrays([week_index_df.index.year, 
                                                            week_index_df.index.month, 
                                                            week_index_df.index.day], 
                                                            names=['Year','Month','Week'])

#### Data Entity Meta ####
"""
Meta data for root (database) and derived (custom) entities.
Group, time, and indicator meta-record type: {'name':'','label':''}
Group cols: define entity-specific partitions of the reporting table columns
Time cols: temporal groups prepended to the index
Indicator cols: define global partitions in the reporting table index
Filter cols: custom filter rules - can be entity-specific or global (dict properties TBD)
""" 
group_template = {'name':'','label':''}
time_template = {'name':'','label':''}
indicator_template = {'name':'','label':''}
filter_template = {}

entity_meta_template = {'name':'',
                        'short_name':'',
                        'display_label':'',
                        'pkey':'',
                        'entity_type':'',
                        'getter_func':'',
                        'group_cols':[],
                        'time_cols':[],
                        'indicator_cols':[],
                        'filters':[],
                        'dependencies':[],
                        'references':[]}

#### Visualization ####
class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

SAVE_DPI = 500
APT_MONTHLY_YMAX = 300
APT_WEEKLY_YMAX = 100

#### Unused / Deprecated Code ####
"""

#### THIS IS AN OLD META STRUCTURE - KEEPING FOR REFERENCE ####
# Definitions
data_entity_names = ['account','acct_monthly_ts_data','acct_weekly_ts_data','acct_src_ts_data','acct_src_weekly_ts_data']
data_entity_types = ['root','master','derived']
data_entity_groups = ['all_data','root_data','master_data','derived_data', # entity types
                      'account_data','provider_data','appointment_data','outcomes_data' # entity content
                      'reporting_data','analysis_data','time_series_data','frequency_data','list_data'] # reporting
data_entity_properties = {'name':{'type':str, 'default':''},
                          'types':{'type':list, 'default':[]}, 
                          'subsets':{'type':list, 'default':[]},
                          'groups':{'type':list, 'default':[]},
                          'dependencies':{'type':list, 'default':[]},
                          'references':{'type':list, 'default':[]},
                          'custom_filters':{'type':list, 'default':[]},
                          'description':{'type':str, 'default':''}}

# Associations
data_entity_type_dict = {'account':['root','master'], 
                         'acct_monthly_ts_data':['derived'],
                         'acct_weekly_ts_data':['derived'],
                         'acct_src_ts_data':['derived'],
                         'acct_src_weekly_ts_data':['derived'],}

data_entity_subset_dict = {'account':[], 
                         'acct_monthly_ts_data':[],
                         'acct_weekly_ts_data':[],
                         'acct_src_ts_data':[],
                         'acct_src_weekly_ts_data':[],}

"""