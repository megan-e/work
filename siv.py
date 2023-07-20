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

# contact clean
non_deduped_folder = ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/non_deduped_contacts/' + date_string + '_Site' + '*.csv'
contact_file = glob.glob(non_deduped_folder)
