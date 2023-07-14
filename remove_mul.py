import pandas as pd
import numpy as np
from ps_module import ps_module as ps

remove_file_path = '/Users/megan/Library/CloudStorage/GoogleDrive-megan.edusada@reifyhealth.com/.shortcut-targets-by-id/1go88qsA3tuOJKUDVM2cFUQWfFG1K8i8y/RAW SITE LISTS FOR PROCESSING/In Process - Megan/MK-2140-004_cleaned_remove.csv'
sf_report_path = ''

remove_file = pd.read_csv(remove_file_path)
remove_file = remove_file[['PROTOCOL #', 'SITE', 'ROLE', 'NAME', 'EMAIL ADDRESS']]

# dedupe
remove_file = remove_file.drop_duplicates(subset=['PROTOCOL #', 'SITE', 'ROLE'])

cra = remove_file.loc[remove_file['ROLE'].str.contains('Monitor')]
primary_contact = remove_file.loc[remove_file['ROLE'].str.contains('Coordinator')]
pi = remove_file.loc[remove_file['ROLE'].str.contains('Investigator')]

# Put SF report in same file as remove file
