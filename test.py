# filters out for role, using OR condition |.
import numpy as np
import datetime
# siv clean
import pandas as pd
from ps_module import ps_module as ps
import glob
import datetime

# looking for files based on today's date
date = datetime.datetime.now()
# format = DDabbreviationYEAR
date_string = str.lower(date.strftime('%d%b%Y'))

# contact clean
non_deduped_folder = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_Site' + '*.csv'
contact_file = glob.glob(non_deduped_folder)

# contact clean


contact_list = pd.read_csv(
    '/Users/megan/Library/CloudStorage/GoogleDrive-megan.edusada@reifyhealth.com/.shortcut-targets-by-id/1go88qsA3tuOJKUDVM2cFUQWfFG1K8i8y/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/02aug2023_Site_List_Contacts_t_1691014519080.csv')
filter_roles = ['Principal Investigator', 'Study Coordinator',
                'CRA', 'Primary CRA', 'Trial Co-ordinator',
                'Primary Study Coordinator', 'Primary Co-ordinator', 'PI', 'Primary Coordinator',
                'Site Monitor', 'Primary Investigator', 'Study Coordinator*', 'Study Coordinator (Primary)',
                'Study Coordinator Back-Up', 'Lead Research Coordinator', 'Research Coordinator',
                'Lead Study Coordinator',
                'Study Coordinator - Backup', 'Sub - Investigator', 'Sub-Investigator', 'Lead Site Monitor',
                'Study Coordinator(Back - up)',
                'Lead Coordinator', 'Monitor', 'Primary Contact', 'Site Staff'
                ]
contact_roles_filtered = contact_list.loc[contact_list['Role [DW]'].isin(filter_roles)]
