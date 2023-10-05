import pandas as pd
import numpy as np
from ps_module import ps_module as ps
import datetime
import glob
import datetime
import re

# looking for files based on today's date
date = datetime.datetime.now()
# format = DDabbreviationYEAR
date_string = str.lower(date.strftime('%d%b%Y'))

remove_contacts_folder = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/remove_contacts/' + date_string + '_REMOVE' + '*.csv'
contact_file = glob.glob(remove_contacts_folder)

remove_file = pd.read_csv(contact_file[0])
print(remove_file.columns.unique)

# remove 0's from sumover columns and dedupe
pi_contacts = remove_file.loc[(remove_file['PI Match Sumover'] > 0) &
                              (remove_file['Role [DW]'].str.contains('Investigator'))].drop_duplicates(
    subset=['SF Trial Name', 'SF Site Trial Number'])
pi_contacts['Role [DW]'] = pi_contacts['Role [DW]'].replace(["Principal Investigator", "Investigator", " "], "",
                                                            regex=True)

sc_contacts = remove_file.loc[(remove_file['Coordinator Match Sumover'] > 0) &
                              (remove_file['Role [DW]'].str.contains('Coordinator'))].drop_duplicates(
    subset=['SF Trial Name', 'SF Site Trial Number'])
sc_contacts['Role [DW]'] = sc_contacts['Role [DW]'].replace(["Study Coordinator", "Coordinator", "Primary", " "], "",
                                                            regex=True)

cra_contacts = remove_file.loc[(remove_file['CRA Match Sumover'] > 0) &
                               (remove_file['Role [DW]'].str.contains('Monitor'))].drop_duplicates(
    subset=['SF Trial Name', 'SF Site Trial Number'])
cra_contacts['Role [DW]'] = cra_contacts['Role [DW]'].replace(["Site Monitor", "Monitor", "Lead" " "], "",
                                                              regex=True)

remove_file.to_excel(
    ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/remove_contacts/' + date_string + '_remove_contacts_clean.xlsx',
    index=False)
with pd.ExcelWriter(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/remove_contacts/' + date_string + '_remove_contacts_clean.xlsx') as writer:
    remove_file.to_excel(writer, sheet_name="Original List")
    pi_contacts.to_excel(writer, sheet_name="Principal Investigator")
    sc_contacts.to_excel(writer, sheet_name="Study Coordinator")
    cra_contacts.to_excel(writer, sheet_name="CRA")
