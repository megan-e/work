# filters out for role, using OR condition |.
import numpy as np
import datetime
# siv clean
import pandas as pd
from ps_module import ps_module as ps
import glob
import datetime
import country_converter as coco

# path = '/Users/megan/Library/CloudStorage/GoogleDrive-megan.edusada@reifyhealth.com/.shortcut-targets-by-id/1go88qsA3tuOJKUDVM2cFUQWfFG1K8i8y/RAW SITE LISTS FOR PROCESSING/scripts/new_sites/Sites_on_Site_List_N_1690296590485.csv'
new_sites_path = '/Users/megan/Library/CloudStorage/GoogleDrive-megan.edusada@reifyhealth.com/.shortcut-targets-by-id/1go88qsA3tuOJKUDVM2cFUQWfFG1K8i8y/RAW SITE LISTS FOR PROCESSING/scripts/new_sites/Sites_on_Site_List_N_1690296590485.csv'
new_sites = pd.read_csv(new_sites_path)
new_sites = new_sites[new_sites['site_name'].notnull()]
new_sites = new_sites[new_sites['site_number'].notnull()]
new_sites = new_sites[new_sites['site_street_address'].notnull()]

new_sites = new_sites[new_sites['site_country'].str.contains("not found", na=True) == False]

new_sites = new_sites.drop_duplicates(subset=['protocol_alias', 'site_number', 'site_street_address'])
new_sites = new_sites[['protocol_alias', 'site_number', 'site_name', 'site_street_address',
                       'site_city', 'site_state___province', 'site_zip___postal_code',
                       'site_country', 'SIV from DW (full, converted)']]
# new_sites.loc[new_sites['site_country'] == 'Turkiye', 'site_country_2'] = 'Turkey'
# new_sites.loc[new_sites['site_country'] == 'UK', 'site_country_2'] = 'United Kingdom'
new_sites['site_country'] = coco.convert(list(new_sites['site_country']), to='name')

new_sites['site_country'] = new_sites['site_country'].replace(dict.fromkeys(['Türkiye'], 'Turkey'), regex=True)
new_sites['site_country'] = new_sites['site_country'].replace(dict.fromkeys(['South Korea'], 'Korea, Republic of'),
                                                              regex=True)

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}
abbrev_to_australia = {"Victoria": "VIC",
                       "Queensland": "QLD",
                       "New South Wales": "NSW"
                       }
abbrev_to_canada = {"Ontario": "ON",
                    "Quebec": "QC"

                    }
abbrev_to_italy = {"Bologna": "BO",
                   "Bari": "BA"

                   }
abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))
# replace values with dictionary mapping
new_sites = new_sites.replace({'site_state___province': abbrev_to_us_state})
# check
united_states_changed = new_sites.loc[new_sites['site_country'] == 'United States']

new_sites = new_sites.replace({'site_state___province': abbrev_to_australia})
# check
australia_changed = new_sites.loc[new_sites['site_country'] == 'Australia']

# new_sites = new_sites.replace({'site_state___province': abbrev_to_canada})
# new_sites = new_sites.replace({'site_state___province': abbrev_to_italy})
#
# if new_sites['site_country'] in us_state_to_abbrev:
#     return new_sites.replace({'site_state___province': abbrev_to_us_state, })
