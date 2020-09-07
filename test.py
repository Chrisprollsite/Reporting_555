import pandas as pd
import pathlib

excel_file = "C:/Users/christian.baillard/ACTIMAGE CONSULTING SAS/BU Colmar Projects - Reporting 555/PROG/Actimage_team_managers.xlsx"
json_file_path = pathlib.Path(excel_file)
df = pd.read_excel(excel_file)

bu_dict = {}
for index, row in df.iterrows():
    key = row['Team name']
    values = row["Team aliases"].split(";")
    values=[x.replace(u'\xa0', u' ').strip() for x in values]
    bu_dict[key]=values
