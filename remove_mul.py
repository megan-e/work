import pandas as pd
import numpy as np
from ps_module import ps_module as ps

# def remove_file_clean():
remove_file_path = '/Users/megan/Library/CloudStorage/GoogleDrive-megan.edusada@reifyhealth.com/.shortcut-targets-by-id/1go88qsA3tuOJKUDVM2cFUQWfFG1K8i8y/RAW SITE LISTS FOR PROCESSING/In Process - Megan/19jul2023_MK3475-676_clean_me_cleaned_remove.csv'

remove_file = pd.read_csv(remove_file_path)
remove_file = remove_file[['PROTOCOL #', 'SITE', 'ROLE', 'NAME', 'EMAIL ADDRESS']]
remove_file['SITE'] = remove_file['SITE'].astype(str)
remove_file['SITE'] = remove_file['SITE'].str.zfill(4)
# dedupe
remove_file = remove_file.drop_duplicates(subset=['PROTOCOL #', 'SITE', 'ROLE'])
remove_file.rename(columns={'SITE': 'Site Number'}, inplace=True)
remove_file = remove_file.loc[remove_file['EMAIL ADDRESS'].notna()]

cra = remove_file.loc[remove_file['ROLE'].str.contains('Monitor')]
# cra['rewritten_role'] = 'Monitor'
# cra = cra.drop_duplicates(subset=['Site Number', 'rewritten_role'], keep='first')

primary_contact = remove_file.loc[remove_file['ROLE'].str.contains('Coordinator')]
# primary_contact['rewritten_role'] = 'primary_contact'
# primary_contact = primary_contact.drop_duplicates(subset=['Site Number', 'rewritten_role'], keep='first')

pi = remove_file.loc[remove_file['ROLE'].str.contains('Investigator')]
# pi['rewritten_role'] = 'investigator'
# pi = pi.drop_duplicates(subset=['Site Number', 'rewritten_role'], keep='first')

# merging sf report
sf_report = pd.read_excel('/Users/megan/Downloads/Site List Update Workbook Report-2023-07-19-14-37-58.xlsx')
sf_report = sf_report[[
    'Trial: Trial Name', 'Site Number', 'Primary Contact: Full Name', 'Primary Contact: Email',
    'Primary Contact: Contact ID', 'Principal Investigator: Full Name',
    'Principal Investigator: Email', 'Principal Investigator: Contact ID',
    'CRA / CDC: Full Name', 'CRA / CDC: Email', 'CRA / CDC: Contact ID', 'Site Trial ID'
]]

sf_report['Site Number'] = sf_report['Site Number'].astype(str)
sf_report['Site Number'] = sf_report['Site Number'].str.zfill(4)

merged_on_site_cra = pd.merge(cra, sf_report, how='left', on='Site Number')
cra_match = merged_on_site_cra.loc[merged_on_site_cra['EMAIL ADDRESS'] == merged_on_site_cra['CRA / CDC: Email']]

merged_on_site_primary_contact = pd.merge(primary_contact, sf_report, how='left', on='Site Number')
primary_contact_match = merged_on_site_primary_contact.loc[
    merged_on_site_primary_contact['EMAIL ADDRESS'] == merged_on_site_primary_contact['Primary Contact: Email']]

merged_on_site_pi = pd.merge(pi, sf_report, how='left', on='Site Number')
pi_match = merged_on_site_pi.loc[
    merged_on_site_pi['EMAIL ADDRESS'] == merged_on_site_pi['Principal Investigator: Email']]

output_path = '/Users/megan/Library/CloudStorage/GoogleDrive-megan.edusada@reifyhealth.com/.shortcut-targets-by-id/1go88qsA3tuOJKUDVM2cFUQWfFG1K8i8y/RAW SITE LISTS FOR PROCESSING/In Process - Megan/'
output_file_name = 'remove_MK-3475-676.xlsx'
remove_file.to_excel(output_path + output_file_name)
with pd.ExcelWriter(output_path + output_file_name) as writer:
    remove_file.to_excel(writer, sheet_name="Full List")
    cra_match.to_excel(writer, sheet_name="cra_remove")
    primary_contact_match.to_excel(writer, sheet_name="primary_contact_remove")
    pi_match.to_excel(writer, sheet_name="pi_remove")

# Put SF report in same file as remove file
# final_output_file_path = pd.read_excel(output_path + output_file_name)
