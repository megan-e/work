import pandas as pd
from ps_module import ps_module as ps
import numpy as np


# df = pd.read_csv('/Users/megan/Downloads/Missing_at_least_ONE_1689016080497.csv')


def fill_if_empty_roles(path=None, output_file_name='default_name'):
    fill_if_empty = pd.read_csv(path)
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

    dropped_dupes.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/fill_if_empty/' + output_file_name + '.xlsx')
    with pd.ExcelWriter(
            ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/fill_if_empty/' + output_file_name + '.xlsx') as writer:
        dropped_dupes.to_excel(writer, sheet_name="Fill if Empty")
        principal_investigator.to_excel(writer, sheet_name="Principal Investigator")
        primary_contact.to_excel(writer, sheet_name="Primary Contact")
        cra.to_excel(writer, sheet_name="CRA")

    return dropped_dupes
