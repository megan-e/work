import pandas as pd
from ps_module import ps_module as ps
import glob
import datetime

# looking for files based on today's date
date = datetime.datetime.now()
date_string = str.lower(date.strftime('%d%b%Y'))
file_names = glob.glob(
    ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/date_update_drop_cancel/' + date_string + '*.csv')

# update date
if 'Protocol_with_Trial' in file_names:
    protocol_path = next(x for x in file_names if 'Protocol_with_Trial' in x)

    update_date = pd.read_csv(protocol_path)
    update_date = update_date[update_date['Trial SFID [SF]'].notnull()]
    update_date['date updated'] = date_string
    update_date.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/date_update_drop_cancel/' + date_string + '_update_date_clean.xlsx')

# dropped/cancelled sites
if 'Sites_on_Site_Lists' in file_names:
    dropped_file_path = next(x for x in file_names if 'Sites_on_Site_Lists' in x)
    dropped_sites = pd.read_csv(dropped_file_path)
    dropped_sites.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/date_update_drop_cancel/' + date_string + '_dropped_sites_clean.xlsx'
    )
