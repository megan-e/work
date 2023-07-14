# filters out for role, using OR condition |.
import pandas as pd
import numpy as np
from ps_module import ps_module as ps
import datetime
from openpyxl import load_workbook
# siv clean
import pandas as pd
from ps_module import ps_module as ps
import glob
import datetime

# looking for files based on today's date
date = datetime.datetime.now()
date_string = str.lower(date.strftime('%d%b%Y'))
siv_path = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/siv/' + date_string + '*.csv'
file_names = glob.glob(siv_path)

siv = pd.read_csv(file_names[0])
# Drop sites without IDs
siv = siv.dropna(subset=['Site Trial SFID'])

# Drop duplicates based on site number and protocol
siv = siv.drop_duplicates(subset=['Site Number', 'Protocol']).reset_index(drop=True)

# Site numbers with total of 4 digits
siv['Site Number'] = siv['Site Number'].astype(str)
siv['Site Number'] = siv['Site Number'].str.zfill(4)
siv['SIV Date [SF]'] = siv['SIV Date [SF]'].astype('datetime64[ns]')
siv['SIV from DW (full, converted)'] = pd.to_datetime(siv['SIV from DW (full, converted)'],
                                                      format='%m/%d/%Y', errors='coerce')

# Find where there is a different SIV from the DW
upsert = siv.loc[siv['SIV Date [SF]'] != siv['SIV from DW (full, converted)']]

# Dropping invalid dates from DW
upsert = upsert.dropna(subset=['SIV from DW (full, converted)'])

# Creating and reformatting upsert column
upsert['upsert_date'] = upsert['SIV from DW (full, converted)']
upsert['upsert_date'] = upsert['upsert_date'].dt.strftime('%m/%d/%Y')

upsert.to_excel(
    ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/siv/' + date_string + '_siv_clean.xlsx',
    index=False)
# test paths
# path = '/Users/megan/Downloads/Site_List_Contacts_t_1688413811562.csv'
# path2 = '/Users/megan/Downloads/Site_List_Contacts_t_1689102490861.csv'
# path3 = '/Users/megan/Downloads/Site_List_Contacts_t_1689102530325.csv'

# contact clean
non_deduped_folder = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_Site' + '*.csv'
contact_file = glob.glob(non_deduped_folder)


# contact_file = pd.read_csv(contact_file_name[0])


def contact_clean():
    contact_list = pd.read_csv(contact_file[0])
    filter_roles = ['Principal Investigator', 'Study Coordinator',
                    'CRA', 'Primary CRA', 'Trial Co-ordinator',
                    'Primary Study Coordinator', 'Primary Co-ordinator', 'PI', 'Primary Coordinator',
                    'Site Monitor', 'Primary Investigator', 'Study Coordinator*', 'Study Coordinator (Primary)',
                    'Study Coordinator Back-Up', 'Lead Research Coordinator', 'Lead Study Coordinator',
                    'Study Coordinator - Backup', 'Sub - Investigator', 'Lead Site Monitor',
                    'Study Coordinator(Back - up)',
                    'Lead Coordinator', 'Monitor', 'Primary Contact'
                    ]
    contact_roles_filtered = contact_list.loc[contact_list['Role [DW]'].isin(filter_roles)]

    # Only one unique email is needed
    deduped_list = contact_roles_filtered.drop_duplicates(subset=['Email [DW]', 'Name [DW]'])
    deduped_list = deduped_list.replace(dict.fromkeys(['\xa0', r'\\n', r'\\t', '\u202f', r'\\'], '/'), regex=True)

    ##Begin separating out emails that need specific cleaning
    # Separating out multiple emails in one cell
    multiple_emails = deduped_list.loc[(deduped_list['Email [DW]'].str.count("@")) >= 2]

    # Concat and delete duplicate emails
    deduped_list = pd.concat([deduped_list, multiple_emails]).drop_duplicates(keep=False)

    # Clean
    # Split based on slashes at the end
    deduped_list['Email [DW]'] = deduped_list['Email [DW]'].str.split('/', n=1).str.get(0)
    # Remove and split based on slashes separating multiple emails
    multiple_emails['Email [DW]'] = multiple_emails['Email [DW]'].str.split(';| |/', n=1).str.get(0)

    # Replace misspellings
    deduped_list['Email [DW]'] = deduped_list['Email [DW]'].replace("novarrtis", "novartis", regex=True)

    # Concat cleaned data to original deduped_list
    data = [deduped_list, multiple_emails]
    deduped_list = pd.concat(data)

    # Remove trailing and leading punctuation and spaces
    # Did not remove spaces within email - there are sometimes spaces in place of dashes
    deduped_list['Email [DW]'] = deduped_list['Email [DW]'].str.strip(",. '>")

    # Delete emails that do not contain @ symbol
    not_email = deduped_list[~deduped_list['Email [DW]'].str.contains("@")]

    # Finding punctuation within email
    contains_punctuation = deduped_list[deduped_list['Email [DW]'].str.contains(r",|'", regex=True)]
    no_name = deduped_list.loc[deduped_list['Name [DW]'].isna()]
    check_sheet = [not_email, contains_punctuation, no_name]
    check_sheet = pd.concat(check_sheet)

    deduped_list = pd.concat([deduped_list, check_sheet]).drop_duplicates(keep=False)

    ##cleaning contact name
    # Separating contacts that have first and last name switched (contains a ,)
    deduped_list = deduped_list.replace([", MD", "M.D.", "Dr.", ", RN"], "")
    deduped_list['Name [DW]'] = deduped_list['Name [DW]'].str.strip("/,.")

    comma = deduped_list.loc[deduped_list['Name [DW]'].str.contains(',')]

    deduped_list = pd.concat([deduped_list, comma]).drop_duplicates(keep=False)

    comma['Name [DW]'] = comma['Name [DW]'].str.replace(', ', ',', regex=True)

    comma['Last Name [DW] '] = comma['Name [DW]'].str.split(',', n=1).str.get(0)
    comma['First Name [DW]'] = comma['Name [DW]'].str.split(',', n=1).str.get(1)
    deduped_list['Name [DW]'] = deduped_list['Name [DW]'].str.strip("/")
    deduped_list['Name [DW]'] = deduped_list['Name [DW]'].str.split('/', n=1).str.get(0)

    deduped_list['First Name [DW]'] = deduped_list['Name [DW]'].str.split(' ', n=1).str.get(0)
    deduped_list['Last Name [DW] '] = deduped_list['Name [DW]'].str.split(' ', n=1).str.get(-1)

    data = ([comma, deduped_list])
    final_list = pd.concat(data)
    final_list.reset_index(inplace=True)

    # reassigning site and sponsor contacts
    final_list['Role [DW]'] = final_list['Role [DW]'].str.lower()

    def site_contact(input_value):
        if 'investigator' in input_value or 'pi' in input_value:
            return 'Principal Investigator'
        if 'coordinator' in input_value or 'co-ordinator' in input_value or 'primary contact' in input_value:
            return 'Research Coordinator'
        if 'sub' in input_value:
            return 'Sub-Investigator'
        if 'monitor' in input_value or 'cra' in input_value:
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

    def lifestyle_stage(input_value):
        if 'CRA' in input_value:
            return 'Customer'
        else:
            return None

    final_list['Lifecycle Stage'] = final_list['Role']
    final_list['Lifecycle Stage'] = final_list['Role'].map(lifestyle_stage)
    final_list.reset_index(inplace=True)
    final_list['Email [DW]'] = final_list['Email [DW]'].str.strip(",. '>")

    # Clean phone numbers
    final_list['Phone'] = final_list['Phone'].str.strip("'+/-")
    final_list['Phone'] = final_list['Phone'].str.split(';', n=1).str.get(0)
    final_list['Phone'] = final_list['Phone'].replace(np.nan, '', regex=True)

    final_list.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_non_deduped_contacts.xlsx',
        index=False
    )
    return final_list


contact_clean()


########################################################################################################################
# run this after generating secondary match Excel sheet
# start of secondary match
def secondary_match():
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
        secondary_match_list.to_excel(writer, sheet_name="new contacts")
        site_contact.to_excel(writer, sheet_name="site")
        sponsor_contact.to_excel(writer, sheet_name="sponsor")


secondary_match()
