#!/usr/bin/env python
# coding: utf-8

#### Cobalt Reporting Config ####
#### Institution-level configuration - modify according to specific institutional and reporting requirements ####

#### Platform Institution ####
INSTITUTION_ID = 'PENN'
COBALT_START_DATE = '2020/04/09'
COBALT_TZ = 'US/Eastern'


#### Database Connection ####
# Read-only
DB_NAME = 'cobalt'
DB_USER = 'cobalt_readonly'
DB_PASSWORD = 'REDACTED'
DB_HOST = 'REDACTED'

# Read and write 
DB_REPORTING_NAME = 'cobalt'
DB_REPORTING_USER = 'cobalt_reporting'
DB_REPORTING_PASSWORD = 'REDACTED'
DB_REPORTING_HOST = 'REDACTED'
DB_REPORTING_MONTHLY_NAME = 'reporting_monthly_rollup'
DB_REPORTING_WEEKLY_NAME = 'reporting_weekly_rollup'


#### Save Paths ####
# Main Cobalt project path
PROJECT_PATH = '/' # replace as needed

# Top level paths
CODE_PATH = PROJECT_PATH + 'code/'
DATA_PATH = PROJECT_PATH + 'data/'
LOGS_PATH = PROJECT_PATH + 'logs/'
MODELS_PATH = PROJECT_PATH + 'models/'
OUTPUT_PATH = PROJECT_PATH + 'output/'

# Code paths
REPORTING_SYSTEM_PATH = CODE_PATH + 'reporting_system/'

# Data paths
RAW_REPORTING_DATA_PATH = DATA_PATH + 'raw_reporting_data/'
MASTER_REPORTING_DATA_PATH = DATA_PATH + 'master_reporting_data/'
ACCOUNT_INSTANCE_DATA_PATH = DATA_PATH + 'account_instance_data/'
SANKEY_DATA_PATH = DATA_PATH + 'sankey_data/'
INSTITUTION_DATA_PATH = DATA_PATH + 'institution_data/'
COBALT_PLUS_DATA_PATH = DATA_PATH + 'cobalt_plus_data/'
GROUP_SESSION_FEEDBACK_DATA_PATH = DATA_PATH + 'group_session_feedback_data/'

GROUP_SESSION_FEEDBACK_MASTER_DATA_PATH = GROUP_SESSION_FEEDBACK_DATA_PATH + 'master_data/'
GROUP_SESSION_FEEDBACK_SESSIONS_DATA_PATH = GROUP_SESSION_FEEDBACK_DATA_PATH + 'sessions_data/'
GROUP_SESSION_FEEDBACK_SESSIONS_META_PATH = GROUP_SESSION_FEEDBACK_DATA_PATH + 'sessions_meta/'

# Logs paths
BACKEND_ISSUES_LOGS_PATH = LOGS_PATH + 'backend_issues_logs/'
FRONTEND_ISSUES_LOGS_PATH = LOGS_PATH + 'frontend_issues_logs/'
DATA_ANOMALIES_LOGS_PATH = LOGS_PATH + 'data_anomalies_logs/'
GROUP_SESSION_FEEDBACK_LOGS_PATH = LOGS_PATH + 'group_sesion_feedback_logs/'

# Models paths
MALLET_PATH = '/Users/arthurpelullo/mallet-2.0.8/bin/mallet'
GROUP_SESSION_FEEDBACK_MODELS_PATH = MODELS_PATH + 'group_session_feedback_models/'

# Output paths
REPORTING_SYSTEM_OUTPUT_PATH = OUTPUT_PATH + 'reporting_system_output/'
CRITICAL_ANALYTICS_OUTPUT_PATH = OUTPUT_PATH + 'critical_analytics_output/'
COBALT_PLUS_OUTPUT_PATH = OUTPUT_PATH + 'cobalt_plus_output/'
GROUP_SESSION_FEEDBACK_OUTPUT_PATH = OUTPUT_PATH + 'group_session_feedback_output/'

REPORTING_SYSTEM_CHART_PATH = REPORTING_SYSTEM_OUTPUT_PATH + 'charts/'
REPORTING_SYSTEM_FIGURE_PATH = REPORTING_SYSTEM_OUTPUT_PATH + 'figures/'

GROUP_SESSION_FEEDBACK_CHART_PATH = GROUP_SESSION_FEEDBACK_OUTPUT_PATH + 'charts/'
GROUP_SESSION_FEEDBACK_FIGURE_PATH = GROUP_SESSION_FEEDBACK_OUTPUT_PATH + 'figures/'
GROUP_SESSION_FEEDBACK_CLOUD_PATH = GROUP_SESSION_FEEDBACK_OUTPUT_PATH + 'clouds/'
GROUP_SESSION_FEEDBACK_PANEL_PATH = GROUP_SESSION_FEEDBACK_OUTPUT_PATH + 'panels/'
GROUP_SESSION_FEEDBACK_TOPIC_PATH = GROUP_SESSION_FEEDBACK_OUTPUT_PATH + 'topics/'


#### Custom Data Filtering ####
ACCOUNT_EXCLUDE_LIST = ['REDACTED','REDACTED']
ACTIVE_PROV_COUNT = 25

CUSTOM_FILTERS = {'account':{},
                  'provider':{},}
