import pandas as pd
from ps_module import ps_module as ps
import glob
import datetime
import numpy as np

# looking for files based on today's date
date = datetime.datetime.now()
date_string = str.lower(date.strftime('%d%b%Y'))
# file_path = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/date_update_drop_cancel/' + date_string + '*.csv'
file_path = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/am_tasks/' + date_string + '*.csv'

file_names = glob.glob(file_path)
# update date
if any(map(lambda name: 'Protocol' in name, file_names)):
    protocol_path = next(x for x in file_names if 'Protocol_with_Trial' in x)
    update_date = pd.read_csv(protocol_path)
    update_date = update_date[update_date['Trial SFID [SF]'].notnull()]
    update_date['date updated'] = date_string
    # update_date.to_excel(
    #     ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/date_update_drop_cancel/' + date_string + '_update_date_clean.xlsx')
    update_date.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/am_tasks/' + date_string + '_update_date_clean.xlsx')

# dropped/cancelled sites
if any(map(lambda name: 'Sites' in name, file_names)):
    dropped_file_path = next(x for x in file_names if 'Sites_on_Site_Lists' in x)
    dropped_sites = pd.read_csv(dropped_file_path)
    # dropped_sites.to_excel(
    #     ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/date_update_drop_cancel/' + date_string + '_dropped_sites_clean.xlsx'
    # )
    dropped_sites.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/am_tasks/' + date_string + '_dropped_sites_clean.xlsx'
    )

# fill if empty
# file_names_fill = glob.glob(
#     ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/fill_if_empty/' + date_string + '*.csv')


# def fill_if_empty_roles():
if any(map(lambda name: 'Missing' in name, file_names)):
    fill_if_empty_path = next(x for x in file_names if 'Missing' in x)
    fill_if_empty = pd.read_csv(fill_if_empty_path)
    fill_if_empty = fill_if_empty.replace(r'^\s*$', np.nan, regex=True)
    fill_if_empty = fill_if_empty[fill_if_empty['Name [DW]'].notnull()]
    fill_if_empty = fill_if_empty[fill_if_empty['Role [DW]'].notnull()]


    def contact(input_value):
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
    fill_if_empty['Role [DW]'] = fill_if_empty['Role [DW]'].str.lower()
    fill_if_empty['Role'] = fill_if_empty['Role [DW]']
    fill_if_empty['Role'] = fill_if_empty['Role'].map(contact)

    dropped_dupes = fill_if_empty.drop_duplicates(subset=['SF Trial Name', 'SF Site Trial Number', 'Role'],
                                                  keep='last').reset_index(drop=True)
    # removing None from role column
    dropped_dupes = dropped_dupes[dropped_dupes['Role'].str.contains("None") == False]
    principal_investigator = dropped_dupes.loc[dropped_dupes['Role'].str.contains("Principal Investigator")]
    principal_investigator = principal_investigator.drop(columns=['SF Lead Coordinator ID', 'SF CRA ID'])
    principal_investigator = principal_investigator[principal_investigator['SF PI ID'].isnull()]

    primary_contact = dropped_dupes.loc[dropped_dupes['Role'].str.contains("Research Coordinator")]
    primary_contact = primary_contact.drop(columns=['SF PI ID', 'SF CRA ID'])
    primary_contact = primary_contact[primary_contact['SF Lead Coordinator ID'].isnull()]

    cra = dropped_dupes.loc[dropped_dupes['Role'].str.contains("CRA")]
    cra = cra.drop(columns=['SF Lead Coordinator ID', 'SF PI ID'])
    cra = cra[cra['SF CRA ID'].isnull()]

    # dropped_dupes.to_excel(
    #     ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/fill_if_empty/' + date_string + '_fill_if_empty_clean.xlsx')
    # with pd.ExcelWriter(
    #         ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/fill_if_empty/' + date_string + '_fill_if_empty_clean.xlsx') as writer:
    dropped_dupes.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/am_tasks/' + date_string + '_fill_if_empty_clean.xlsx')
    with pd.ExcelWriter(
            ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/am_tasks/' + date_string + '_fill_if_empty_clean.xlsx') as writer:
        dropped_dupes.to_excel(writer, sheet_name="Fill if Empty")
        principal_investigator.to_excel(writer, sheet_name="Principal Investigator")
        primary_contact.to_excel(writer, sheet_name="Primary Contact")
        cra.to_excel(writer, sheet_name="CRA")
