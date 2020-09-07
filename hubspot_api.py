# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 14:16:14 2020

@author: christian.baillard
"""

import requests
import pandas as pd
import logging
import sys
import sqlalchemy as sql
import psycopg2


# logging.basicConfig(format="%(message)s",level=logging.INFO,stream=sys.stdout)
# logger = logging.getLogger(__name__)
# logger = logging.getLogger("")

def get_owner_by_id(owner_id, url_base="https://api.hubapi.com/crm/v3/", apikey="12c4aa91-04a8-45c5-87c5-b4ffa12dd9b2",
                    proxies=None):
    """
    Function made to return owner given the id
    
    Parameters
    ----------
    owner_id : str
        owner id i hubspoy
    url_base : str, optional
        hubspot api endpoint. The default is "https://api.hubapi.com/crm/v3/".
    apikey : str, optional
        hubspot apikey. The default is "12c4aa91-04a8-45c5-87c5-b4ffa12dd9b2".

    Returns
    -------
    result : dic
        owner

    """
    if owner_id is None:
        return None

    if owner_id == '':
        return None

    url_full = url_base + "owners/" + owner_id
    querystring = {"hapikey": apikey}
    headers = {'accept': 'application/json'}
    logging.info(url_full)
    response = requests.request("GET", url_full, headers=headers, params=querystring, proxies=proxies)
    try:
        result = response.json()

    except ValueError:
        print("id does not exist")
        result = None

    return result


def get_company_by_id(company_id, url_base="https://api.hubapi.com/crm/v3/",
                      apikey="12c4aa91-04a8-45c5-87c5-b4ffa12dd9b2", proxies=None):
    """
    Function made to return owner given the id
    
    Parameters
    ----------
    company_id : str
        owner id i hubspoy
    url_base : str, optional
        hubspot api endpoint. The default is "https://api.hubapi.com/crm/v3/".
    apikey : str, optional
        hubspot apikey. The default is "12c4aa91-04a8-45c5-87c5-b4ffa12dd9b2".

    Returns
    -------
    result : dic
        company

    """
    if company_id is None:
        return None

    if company_id == '':
        return None

    url_full = url_base + "objects/companies/" + company_id
    querystring = {"hapikey": apikey}
    headers = {'accept': 'application/json'}
    response = requests.request("GET", url_full, headers=headers, params=querystring, proxies=proxies)
    logging.info(url_full)
    try:
        result = response.json()
    except ValueError:
        print("id does not exist")
        result = None

    return result


def get_contacts(results_per_page=100, after_value=0, max_num_page=99999, url_base="https://api.hubapi.com/crm/v3/",
                 apikey="12c4aa91-04a8-45c5-87c5-b4ffa12dd9b2", proxies=None):
    """

    Parameters
    ----------
    proxies: dict
        define proxy parameters as dictinnary
    results_per_page : TYPE, optional
        DESCRIPTION. The default is 100.
    after_value : TYPE, optional
        DESCRIPTION. The default is 0.
    url_base : TYPE, optional
        DESCRIPTION. The default is "https://api.hubapi.com/crm/v3/".
    apikey : TYPE, optional
        DESCRIPTION. The default is "12c4aa91-04a8-45c5-87c5-b4ffa12dd9b2".

    Returns
    -------
    None.

    """

    ### Prepare
    after_value = str(after_value)
    100 if results_per_page > 100 else results_per_page

    ### Initialize 

    url_suffix = "objects/contacts"
    url_full = url_base + url_suffix
    headers = {'accept': 'application/json'}
    contacts = []

    ### Loop over all client pages

    k = 0
    while k < max_num_page:
        k += 1

        ### Prepare
        querystring = dict(archived="false", limit=results_per_page, after=after_value, hapikey=hubspot_apikey,
                           properties="hubspot_owner_id,firstname,lastname,associatedcompanyid,"
                                      "hs_lifecyclestage_lead_date,team_owner")

        ### Call
        response = requests.request("GET", url_full, headers=headers, params=querystring, proxies=proxies)

        ### Process
        json_response = response.json()
        contacts.extend(json_response["results"])

        try:
            print('Fetched %i contacts at index %s' % (len(contacts), after_value))
            after_value = json_response['paging']['next']['after']
        except:
            break

    print("Total of %i contacts fetched" % len(contacts))

    return contacts


def create_contact_table(table_name, db_engine):
    """
    Function made to create contact table
    :param table_name:
    :param db_engine:
    :return:
    """
    meta = sql.MetaData()

    table_obj = sql.Table(
        table_name, meta,
        sql.Column('contact_id', sql.Integer, primary_key=True),
        sql.Column('update_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('creation_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('KPI_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('owner_firstname', sql.String),
        sql.Column('owner_lastname', sql.String),
        sql.Column('team_owner', sql.String),
        sql.Column('contact_firstname', sql.String),
        sql.Column('contact_lastname', sql.String),
        sql.Column('company_name', sql.String)
    )
    meta.create_all(db_engine)

    return table_obj


####################
### Parameters
hubspot_apikey = "52002054-a768-41a8-ac8e-5d61d1bb41f6"
proxies = {
    "http": "http://dumbledore.actimage.int:80",
    "https": "http://dumbledore.actimage.int:80"
}
postgre_port = 5432
postgre_ip = "localhost"
postgre_db_name = "555"
postgre_user = "postgres"
postgre_pwd = "171286"
table_name = "contact"
cmd_connection = "postgresql://%s:%s@%s:%i/%s" % (postgre_user, postgre_pwd, postgre_ip, postgre_port, postgre_db_name)

###################################
### Retrieve all clients from API
contacts = get_contacts(apikey=hubspot_apikey, proxies=proxies, max_num_page=10000)

#################################
### Get pivot update date and list of contact_id  from existing table

db_engine = sql.create_engine(cmd_connection, echo=True)
connection = db_engine.connect()

update_date_pivot = pd.Timestamp("1900-01-01T", tz="UTC")  # Only update contacts changed after this pivot date
existing_contact_ids=[]
if db_engine.dialect.has_table(db_engine, table_name):
    existing_table = pd.read_sql(table_name, connection, columns=["contact_id", "update_date"])
    if not existing_table.empty:
        update_date_pivot = existing_table["update_date"].sort_values(ascending=False).iloc[0]
        existing_contact_ids = existing_table["contact_id"].to_list()



connection.close()

###################################
### Loop to retrieve associated company and owner name (long process, only update contacts based on update_date and
# if contact_id not in db)


data = []

i = 0
for contact in contacts:

    if (int(contact["id"]) in existing_contact_ids) and (pd.Timestamp(contact['updatedAt']) < update_date_pivot):
        continue
    else:
        i += 1
        print('Updating %i/%i' % (i, len(contacts)))
        row = {}
        row["contact_firstname"] = contact['properties']['firstname']
        row["contact_lastname"] = contact['properties']['lastname']
        row["team_owner"] = contact['properties']['team_owner']
        row["creation_date"] = pd.Timestamp(contact['createdAt'])
        row["update_date"] = pd.Timestamp(contact['updatedAt'])
        row["contact_id"] = int(contact["id"])

        if contact['properties']["hs_lifecyclestage_lead_date"] is None:
            row["KPI_date"] = None
        else:
            row["KPI_date"] = pd.Timestamp(contact['properties']["hs_lifecyclestage_lead_date"])

        row["company_name"] = None

        company = get_company_by_id(contact['properties']['associatedcompanyid'], apikey=hubspot_apikey,
                                    proxies=proxies)

        if company is not None:
            row["company_name"] = company["properties"]["name"]

        row['owner_firstname'] = None
        row['owner_lastname'] = None

        owner = get_owner_by_id(contact['properties']["hubspot_owner_id"], apikey=hubspot_apikey, proxies=proxies)

        if owner is not None:
            row['owner_firstname'] = owner["firstName"]
            row['owner_lastname'] = owner["lastName"]

        data.append(row)

df_full = pd.DataFrame(data)

#########################################
### Format columns properly before integration

df_full["company_name"]=df_full["company_name"].str.title()
df_full["contact_firstname"]=df_full["contact_firstname"].str.title()
df_full["contact_lastname"]=df_full["contact_lastname"].str.upper()
df_full["owner_firstname"]=df_full["owner_firstname"].str.title()
df_full["owner_lastname"]=df_full["owner_lastname"].str.upper()
df_full["team_owner"]=df_full["team_owner"].str.lower()


######################################
### Load old_df from existing database
connection = db_engine.connect()

if db_engine.dialect.has_table(db_engine, table_name):
    old_df = pd.read_sql(table_name, connection)
    new_df = pd.concat([old_df[~old_df.contact_id.isin(df_full.contact_id)], df_full])
    meta = sql.MetaData(bind=connection)
    table_obj = sql.Table(table_name, meta, autoload=True, autoload_with=db_engine)
    table_obj.drop(bind=connection)
else:
    new_df = df_full

### Recreate and load

create_contact_table(table_name, db_engine)
new_df.to_sql(table_name, connection, index=False, if_exists='append')
connection.close()
