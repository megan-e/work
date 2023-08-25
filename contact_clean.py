# filters out for role, using OR condition |.
import numpy as np
import datetime
# siv clean
import pandas as pd
from ps_module import ps_module as ps
import glob
import datetime
import re

# looking for files based on today's date
date = datetime.datetime.now()
# format = DDabbreviationYEAR
date_string = str.lower(date.strftime('%d%b%Y'))

# contact clean
non_deduped_folder = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_Site' + '*.csv'
contact_file = glob.glob(non_deduped_folder)

contact_list = pd.read_csv(contact_file[0])
filter_roles = ['Principal Investigator', 'Study Coordinator',
                'CRA', 'Primary CRA', 'Trial Co-ordinator',
                'Primary Study Coordinator', 'Primary Co-ordinator', 'PI', 'Primary Coordinator',
                'Site Monitor', 'Primary Investigator', 'Study Coordinator*', 'Study Coordinator (Primary)',
                'Study Coordinator Back-Up', 'Lead Research Coordinator', 'Research Coordinator',
                'Lead Study Coordinator',
                'Study Coordinator - Backup', 'Sub - Investigator', 'Sub-Investigator', 'Lead Site Monitor',
                'Study Coordinator(Back - up)',
                'Lead Coordinator', 'Monitor', 'Primary Contact', 'Site Staff', 'SC', 'Study Coordinator-back up'
                ]
contact_roles_filtered = contact_list[contact_list['Role [DW]'].isin(filter_roles)]

# Confirm which roles are getting excluded - add roles for filter_roles if needed
excluded_roles = contact_list[~contact_list['Role [DW]'].isin(filter_roles)]
print(excluded_roles['Role [DW]'].unique())

# Remove duplicate emails
deduped_list = contact_roles_filtered.drop_duplicates(subset=['Email [DW]'])
# Delete emails that do not contain @ symbol
not_email = deduped_list[~deduped_list['Email [DW]'].str.contains("@", na=False)]
print(not_email)
# All true emails
deduped_list = deduped_list[deduped_list['Email [DW]'].str.contains("@", na=False)]
# Remove missing emails
deduped_list = deduped_list[~deduped_list['Email [DW]'].isna()]
emails_before_clean = deduped_list
# Replace backslash with forward slash
deduped_list = deduped_list.replace(dict.fromkeys(['\xa0', r'\\n', r'\\t', '\u202f', r'\\'], '/'), regex=True)


# Remove any punctuation at the end
def strip_punc_at_end(email):
    if not email:
        return email  # nothing to strip
    start = 0
    for end, c in enumerate(email[::-1]):
        if c.isalpha():
            break
    return email[start:len(email) - end]


deduped_list['Email [DW]'] = deduped_list['Email [DW]'].map(strip_punc_at_end)
# Removing punctuation from end or beginning
deduped_list['Email [DW]'] = deduped_list['Email [DW]'].str.strip("/,. '>")

hetnar = deduped_list[deduped_list['Email [DW]'].str.contains("hetnar", na=False)]

## Begin separating out emails that need specific cleaning
# Isolate multiple emails in one cell
multiple_emails = deduped_list.loc[(deduped_list['Email [DW]'].str.count("@")) >= 2]
print(multiple_emails)
# Isolate angle brackets on email
angle_bracket = deduped_list.loc[(deduped_list['Email [DW]'].str.contains("<"))]
print(angle_bracket)
# Delete values with multiple emails and angle brackets
deduped_list = pd.concat([deduped_list, multiple_emails, angle_bracket]).drop_duplicates(keep=False)

## Clean
# Remove and split based on slashes separating multiple emails
multiple_emails['Email [DW]'] = multiple_emails['Email [DW]'].str.split(';| |/|,', n=1).str.get(0)

# Replace misspellings
deduped_list['Email [DW]'] = deduped_list['Email [DW]'].replace("novarrtis", "novartis", regex=True)
# Remove and split based on angle bracket
angle_bracket['Email [DW]'] = angle_bracket['Email [DW]'].str.split('<', n=1).str.get(1)

# Concat cleaned data to original deduped_list
data = [deduped_list, multiple_emails, angle_bracket]
deduped_list = pd.concat(data).drop_duplicates(keep=False)

##cleaning contact name
# Separating contacts that have first and last name switched (contains a ,)
char_remove = ['Dr.', 'Dr. ', ', MD', 'M.D', 'M.D.', ', M.D.']
for char in char_remove:
    deduped_list['Name [DW]'] = deduped_list['Name [DW]'].str.replace(char, '')

# 2 names sometimes separated with a "/", therefore stripping only trailing punctuation
deduped_list['Name [DW]'] = deduped_list['Name [DW]'].str.strip("/,., (")

# Multiple names may be separated by a comma, you will need to manually check and confirm the correct name
comma = deduped_list.loc[deduped_list['Name [DW]'].str.contains(',', na=False)]
print(comma['Name [DW]'])
deduped_list = pd.concat([deduped_list, comma]).drop_duplicates(keep=False)

comma['Name [DW]'] = comma['Name [DW]'].str.replace(', ', ',', regex=True)
comma['Last Name [DW] '] = comma['Name [DW]'].str.split(',', n=1).str.get(0)
comma['First Name [DW]'] = comma['Name [DW]'].str.split(',', n=1).str.get(1)
comma['First Name [DW]'] = comma['First Name [DW]'].str.strip("/,., ")

# Multiple names separated by "/"
deduped_list['Name [DW]'] = deduped_list['Name [DW]'].str.split('/', n=1).str.get(0)
deduped_list['First Name [DW]'] = deduped_list['Name [DW]'].str.split(' ', n=1).str.get(0)
deduped_list['Last Name [DW] '] = deduped_list['Name [DW]'].str.split(' ', n=1).str.get(-1)

data = ([comma, deduped_list])
final_list = pd.concat(data).drop_duplicates(keep=False)
# Removing any remaining characters trailing email. Purposely did not change characters WITHIN email
final_list['Email [DW]'] = final_list['Email [DW]'].str.strip(",. '>()")
final_list['Email [DW]'] = final_list['Email [DW]'].astype(str)
# replace any empty columns with nan
final_list = final_list.replace(r'^\s*$', np.nan, regex=True)

## Write new excel sheet for check_sheet
# Emails still containing errors
multiple_emails = final_list.loc[(final_list['Email [DW]'].str.count("@")) >= 2]
contains_space = final_list.loc[(final_list['Email [DW]'].str.contains(' '))]
contains_commas = final_list.loc[(final_list['Email [DW]'].str.contains(','))]
# character_errors = final_list[final_list['Email [DW]'].str.contains(',| |;|`|')]

# Where first and last name is empty
nan_last_name = final_list[final_list['Last Name [DW] '].isna()]
nan_first_name = final_list[final_list['First Name [DW]'].isna()]

check_sheet = [nan_last_name, nan_first_name, multiple_emails, contains_space, contains_commas]
check_sheet = pd.concat(check_sheet).drop_duplicates(keep=False)
print(check_sheet)

# Values are added to the bottom of the contact list
final_list = pd.concat([final_list, check_sheet]).drop_duplicates(keep=False)
# Remove nan values
final_list = final_list[~final_list['Email [DW]'].isna()]
final_list.reset_index(inplace=True)

# reassigning site and sponsor contacts
final_list['Role [DW]'] = final_list['Role [DW]'].str.lower()


# Begin mapping roles and account IDs
def site_contact(input_value):
    if 'sub' in input_value:
        return 'Sub-Investigator'
    if 'investigator' in input_value or 'pi' in input_value:
        return 'Principal Investigator'
    if 'coordinator' in input_value or 'co-ordinator' in input_value or 'primary contact' in input_value:
        return 'Research Coordinator'
    if 'monitor' in input_value or 'cra' or 'site staff' in input_value:
        return 'CRA'
    else:
        return None


# Set equal to each other in order to map
final_list['Role'] = final_list['Role [DW]']
final_list['Role'] = final_list['Role'].map(site_contact)


def record_type(input_value):
    if 'Principal Investigator' in input_value or 'Research Coordinator' in input_value or 'Sub-Investigator' in input_value:
        return '012f20000010G5HAAU'
    if 'CRA' in input_value:
        return '012f20000010G5MAAU'
    else:
        return None


final_list['Record Type ID'] = final_list['Role']
final_list['Record Type ID'] = final_list['Role'].map(record_type)

final_list_none = final_list.loc[final_list['Record Type ID'].isna()]


def lifestyle_stage(input_value):
    if 'CRA' in input_value:
        return 'Customer'
    else:
        return None


final_list['Lifecycle Stage'] = final_list['Role']
final_list['Lifecycle Stage'] = final_list['Role'].map(lifestyle_stage)

final_list.reset_index(inplace=True)

# Clean phone numbers
final_list['Phone'] = final_list['Phone'].str.strip("'+/-")
final_list['Phone'] = final_list['Phone'].str.split(';', n=1).str.get(0)
final_list['Phone'] = final_list['Phone'].replace(np.nan, '', regex=True)

final_list.to_excel(
    ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_non_deduped_contacts.xlsx',
    index=False
)

########################################################################################################################
# run this after generating secondary match Excel sheet
# start of secondary match
date = datetime.datetime.now()
# format = DDabbreviationYEAR
date_string = str.lower(date.strftime('%d%b%Y'))

# def secondary_match():
secondary_match_file = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_Contact' + '*.csv'
secondary_match_file = glob.glob(secondary_match_file)
secondary_match_list = pd.read_csv(secondary_match_file[0])

secondary_match_list = secondary_match_list[secondary_match_list['Match_Id'].isnull()]

site_contact = secondary_match_list.loc[secondary_match_list['Lifecycle Stage'].isnull()]
sponsor_contact = secondary_match_list.loc[secondary_match_list['Lifecycle Stage'].notnull()]


# Missing CRO IDs:
# ICON	001f200001i79C3AAI
# KCR Placement	0016Q00001aroRDQAY
# EPS Group (CRO)	0014P000033MnNXQA0
# Parexel 0014P00002crp3vQAA
# TFScro 001f200001pm3vSAAQ
def sponsor_id(input_email, input_sfid):
    print(input_email)
    if 'iqvia' in input_email:
        return '001f200001i6xPsAAI'
    if 'quintiles' in input_email:
        return '0016Q00001WdPboQAF'
    if 'docsglobal' in input_email:
        return '001f200001i79C3AAI'
    if 'excelya' in input_email:
        return '0014P000035BPVsQAO'
    if 'clinicaltrials.at' in input_email:
        return '001f200001uRF6lAAG'
    if 'inventivhealth.com' in input_email:
        return '0016Q00001bzkqhQAA'
    else:
        return input_sfid


sponsor_contact['Sponsor SFID'] = sponsor_contact.apply(lambda x: sponsor_id(x['Email [DW]'], x['Sponsor SFID']),
                                                        axis=1)

secondary_match_list.to_excel(
    ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/deduped_contacts/' + date_string + '_deduped_contacts.xlsx',
    index=False)
with pd.ExcelWriter(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/deduped_contacts/' + date_string + '_deduped_contacts.xlsx') as writer:
    emails_before_clean.to_excel(writer, sheet_name="original_list")
    secondary_match_list.to_excel(writer, sheet_name="new_contacts_list")
    site_contact.to_excel(writer, sheet_name="site")
    sponsor_contact.to_excel(writer, sheet_name="sponsor")
    check_sheet.to_excel(writer, sheet_name="check_these_contacts")

# secondary_match()
