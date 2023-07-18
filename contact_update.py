# Run this to mass update contacts
import pandas as pd
from ps_module import ps_module as ps


# contact_update_file = pd.read_csv('/Users/megan/Downloads/SF_Primary_Role_Does_1689019232908.csv')


def contact_update(path=None, output_file_name='default_name'):
    contact_update_file = pd.read_csv(path)
    # delete contacts without name/role
    contact_update_file = contact_update_file[contact_update_file['Name [DW]'].notnull()]
    contact_update_file = contact_update_file[contact_update_file['Role [DW]'].notnull()]
    contact_update_file['Role [DW]'] = contact_update_file['Role [DW]'].str.lower()
    contact_update_file = contact_update_file[contact_update_file['Role [DW]'].str.contains('sub') == False]

    def contact(input_value):
        if 'investigator' in input_value or 'pi' in input_value:
            return 'Principal Investigator'
        if 'coordinator' in input_value or 'co-ordinator' in input_value or 'primary contact' in input_value:
            return 'Research Coordinator'
        if 'monitor' in input_value or 'cra' in input_value:
            return 'CRA'
        else:
            return None

    # Set equal to each other in order to map
    contact_update_file['Role'] = contact_update_file['Role [DW]']
    contact_update_file['Role'] = contact_update_file['Role'].map(contact)
    dropped_dupes = contact_update_file.drop_duplicates(subset=['SF Trial Name', 'SF Site Trial Number', 'Role'],
                                                        keep='last').reset_index(drop=True)

    # isolating by role
    dropped_dupes = dropped_dupes.drop(columns=['PI Match Sumover', 'CRA Match Sumover', 'Coordinator Match Sumover'])
    dropped_dupes['Contact SFID'] = dropped_dupes['Contact SFID'].fillna('None')
    dropped_dupes = dropped_dupes.sort_values(by=['Contact SFID'], ascending=False)
    principal_investigator = dropped_dupes.loc[dropped_dupes['Role'].str.contains("Principal Investigator", na=False)]
    primary_contact = dropped_dupes.loc[dropped_dupes['Role'].str.contains("Research Coordinator", na=False)]
    cra = dropped_dupes.loc[dropped_dupes['Role'].str.contains("CRA", na=False)]

    dropped_dupes.to_excel(
        ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/contact_update/' + output_file_name + '.xlsx')
    with pd.ExcelWriter(
            ps.gdrive_root() + '/My Drive/RAW SITE LISTS FOR PROCESSING/scripts/contact_update/' + output_file_name + '.xlsx') as writer:
        dropped_dupes.to_excel(writer, sheet_name="Full List")
        principal_investigator.to_excel(writer, sheet_name="Principal Investigator")
        primary_contact.to_excel(writer, sheet_name="Primary Contact")
        cra.to_excel(writer, sheet_name="CRA")

    return dropped_dupes
