# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 16:47:21 2020

@author: christian.baillard

Function made to retrieve data from actinet, parse it and move it to pandas dataframe

"""

import requests
import pandas as pd
import numpy as np
import logging
import sys
import sqlalchemy
import psycopg2
from requests.auth import HTTPBasicAuth
import html2text
import sqlalchemy as sql
import json
from pathlib import Path

## Parameters
url_base = "https://preprod-actinet.actimage.com/index.php"
username = "christian.baillard"
password = "20?Mericourt"
proxies = {
    "http": "http://dumbledore.actimage.int:80",
    "https": "http://dumbledore.actimage.int:80"
}


class Result():
    def __init__(self):
        self.result = []

    def get_actinet(self):
        return get_actinet()


def read_bu_json(
        json_path="C:/Users/christian.baillard/ACTIMAGE CONSULTING SAS/BU Colmar Projects - Reporting 555/PROG/bu_dictionary.json"):
    """
    Function made to read json contqining all terms associated with bu names
    :param json_path: path to the bu json
    :return: dictionary
    """

    json_file_path = Path(json_path)

    # Opening JSON file
    with open(json_file_path) as json_file:
        bu_dict = json.load(json_file)

    ### Switch key/values

    bu_dict = {z: x for x, y in bu_dict.items() for z in y}

    return bu_dict


def create_qualification_table(table_name, db_engine):
    """
    Function made to create contact table
    :param table_name:
    :param db_engine:
    :return:
    """
    meta = sql.MetaData()

    table_obj = sql.Table(
        table_name, meta,
        sql.Column('bid_id', sql.Integer),
        sql.Column('update_date', sql.TIMESTAMP(timezone=True)),
        # sql.Column('creation_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('qualified_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('owner_firstname', sql.String),
        sql.Column('owner_lastname', sql.String),
        sql.Column('team_owner', sql.String),
        sql.Column('bid_amount', sql.Float),
        sql.Column('bid_reference', sql.String)
    )
    meta.create_all(db_engine)

    return table_obj


def create_sending_table(table_name, db_engine):
    """
    Function made to create sending table
    :param table_name:
    :param db_engine:
    :return:
    """
    meta = sql.MetaData()

    table_obj = sql.Table(
        table_name, meta,
        sql.Column('bid_id', sql.Integer),
        sql.Column('update_date', sql.TIMESTAMP(timezone=True)),
        # sql.Column('creation_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('sent_date', sql.TIMESTAMP(timezone=True)),
        sql.Column('owner_firstname', sql.String),
        sql.Column('owner_lastname', sql.String),
        sql.Column('team_owner', sql.String),
        sql.Column('breakdown', sql.Float),
        sql.Column('bid_amount', sql.Float),
        sql.Column('bid_reference', sql.String)
    )
    meta.create_all(db_engine)

    return table_obj


def get_actinet(username, password, proxies=None, url_base="https://preprod.actinet.actimage.com/index.php"):
    """
    Function made to retrieve flux from actinet webservice
    
    Parameters
    ----------
    url_base : string
        base URL for actinet
    username : string
        username for basic auth
    password : string
        password for basic auth
        
    Returns
    -------
    resulting json as list

    """
    url_full = url_base
    querystring = {"module": "business", "action": "reporting555"}
    headers = {'accept': 'application/json'}
    response = requests.request("GET", url_full,
                                headers=headers, params=querystring, verify=False,
                                auth=HTTPBasicAuth(username, password),
                                proxies=proxies)

    try:
        result = response.json()

    except ValueError:
        print(html2text.html2text(response.text))
        result = None

    return result


def select_qualifier(qualifiers_list):
    """
    Function made to select the qualifier that first qualified the bid
    """
    if not qualifiers_list:
        return None
    else:
        oldest_index = np.argmin([pd.Timestamp(x["qualified_date"]) for x in qualifiers_list])
        qualifier = qualifiers_list[oldest_index]
        return qualifier


def clean_senders(bid):
    """
    Function made to put winners to true if all winners are false (i.e. box wasn't selected in actinet) so that all bid has at least one winner
    Transform in place
    :param bid: bid dictionary
    :return: updated bid dictionary
    """

    if not bid["sent_by"]:
        return bid

    senders = bid["sent_by"]
    senders = sorted(senders, reverse=True, key=lambda i: float(i['breakdown']))
    winners_booleans = [x["winner"] for x in senders]
    if not any(winners_booleans):
        for sender in senders:
            if float(sender["breakdown"]) >= 30:
                sender["winner"] = True

    ### Make sure that at least one winner is set even if all breakdowns < 30
    senders[0]["winner"] = True

    return bid


def select_senders(bid):
    """

    :param bid: bid object
    :param senders_list: list of senders in bid object
    :param bid_amount:
    :return: return a dictionnary containing winners senders and winner teams
    """

    clean_senders(bid)
    senders_list = bid["sent_by"]
    if not senders_list:
        return None
    else:
        ######## Handle owners
        ### Only take owners with winner=True
        winner_owners = [{"firstname": x["first_name"], "lastname": x["last_name"], "breakdown": float(x["breakdown"])}
                         for x in
                         senders_list if x["winner"] == True]

        ### Sort by breakdown from largest to smallest
        winner_owners = sorted(winner_owners, key=lambda i: i['breakdown'], reverse=True)

        ######## Handle Teams
        ### List uniques teams
        unique_teams = list(set([x["team_name"] for x in senders_list]))

        ### select all teams (not only when winner true)
        winner_teams = []
        for team_name in unique_teams:
            temp_dic = {}
            selection_senders = []
            for sender in senders_list:
                if sender["team_name"] == team_name:
                    selection_senders.append(sender)
            total_breakdown = sum([float(x["breakdown"]) for x in selection_senders])
            temp_dic["team"] = team_name
            temp_dic["breakdown"] = total_breakdown
            winner_teams.append(temp_dic)

        ### Sort by breakdown

        winner_teams = sorted(winner_teams, key=lambda i: i['breakdown'], reverse=True)

        ### Make selection on owners based on amount

        if bid["amount"] is None:
            bid["amount"] = "0"

        if float(bid["amount"]) < 10000:
            winner_owners = [winner_owners[0]]
            winner_teams = [winner_teams[0]]

        else:
            winner_owners = winner_owners[:3]
            temp_winner_teams = [x for x in winner_teams if x["breakdown"] >= 30]
            if not temp_winner_teams:
                winner_teams = [winner_teams[0]]
            else:
                winner_teams = temp_winner_teams

        return {"winner_owners": winner_owners, "winner_teams": winner_teams}


############################################################


### Load flux
bids = get_actinet(username, password, url_base=url_base, proxies=proxies)

# example = [bid for bid in bids if bid["id"] == "12213"][0]

########## Build Dataframe from json flux

data_qualif = []
data_send = []
for bid in bids:
    qualifier = select_qualifier(bid['qualified_by'])
    senders = select_senders(bid)
    if qualifier is not None:
        row_qualif = {}
        row_qualif["owner_lastname"] = qualifier['last_name'].upper()
        row_qualif["owner_firstname"] = qualifier['first_name'].title()
        row_qualif["team_owner"] = bid["qualified_for"]["team"]
        row_qualif["qualified_date"] = pd.Timestamp(qualifier['qualified_date'])
        row_qualif["update_date"] = pd.Timestamp(bid["last_date"])
        row_qualif["bid_reference"] = bid["bid_reference"]
        row_qualif["bid_amount"] = bid["amount"]
        row_qualif["bid_id"] = int(bid["id"])
        data_qualif.append(row_qualif)

    if senders is not None:
        for owner in senders["winner_owners"]:
            row_send = {}
            row_send["owner_lastname"] = owner["lastname"].upper()
            row_send["owner_firstname"] = owner['firstname'].title()
            row_send["breakdown"] = owner['breakdown']
            row_send["sent_date"] = pd.Timestamp(bid['sent_date'])
            row_send["update_date"] = pd.Timestamp(bid["last_date"])
            row_send["bid_reference"] = bid["bid_reference"]
            row_send["bid_amount"] = bid["amount"]
            row_send["bid_id"] = int(bid["id"])
            data_send.append(row_send)
        for team in senders["winner_teams"]:
            row_send = {}
            row_send["team_owner"] = team["team"].lower()
            row_send["breakdown"] = team["breakdown"]
            row_send["sent_date"] = pd.Timestamp(bid['sent_date'])
            row_send["update_date"] = pd.Timestamp(bid["last_date"])
            row_send["bid_reference"] = bid["bid_reference"]
            row_send["bid_amount"] = bid["amount"]
            row_send["bid_id"] = int(bid["id"])
            data_send.append(row_send)

## Convert to pandas dataframe and save to SQL

df_qualif = pd.DataFrame(data_qualif)
df_send = pd.DataFrame(data_send)

### Format columns properly before integration

df_qualif["team_owner"] = df_qualif["team_owner"].str.lower()
df_send["team_owner"] = df_send["team_owner"].str.lower()

####### Map teams names properly based on bu dictionary

try:
    dict_bu = read_bu_json()
    up_keys = df_qualif["team_owner"].append(df_send["team_owner"]).dropna().unique().tolist()
    new_dict = {x: x for x in up_keys if x not in dict_bu.keys()}
    dict_bu.update(new_dict)

    df_qualif["team_owner"] = df_qualif["team_owner"].map(dict_bu)
    df_send["team_owner"] = df_send["team_owner"].map(dict_bu)
except Exception as e:
    print(e)
    print("Team names left unchanged")
    pass

####################
### SQL Parameters
proxies = {
    "http": "http://dumbledore.actimage.int:80",
    "https": "http://dumbledore.actimage.int:80"
}
postgre_port = 5432
postgre_ip = "localhost"
postgre_db_name = "555"
postgre_user = "postgres"
postgre_pwd = "171286"
qualif_table_name = "qualification"
send_table_name = "sending"
cmd_connection = "postgresql://%s:%s@%s:%i/%s" % (postgre_user, postgre_pwd, postgre_ip, postgre_port, postgre_db_name)

######################################
### Load old_df from existing database
db_engine = sql.create_engine(cmd_connection, echo=False)
connection = db_engine.connect()

######## For Qualification
#### Load qualification tables

if db_engine.dialect.has_table(db_engine, qualif_table_name):
    meta = sql.MetaData(bind=connection)
    table_obj = sql.Table(qualif_table_name, meta, autoload=True, autoload_with=db_engine)
    table_obj.drop(bind=connection)

### Recreate and load

create_qualification_table(qualif_table_name, db_engine)
df_qualif.to_sql(qualif_table_name, connection, index=False, if_exists='append')

######## For Sending

if db_engine.dialect.has_table(db_engine, send_table_name):
    meta = sql.MetaData(bind=connection)
    table_obj = sql.Table(send_table_name, meta, autoload=True, autoload_with=db_engine)
    table_obj.drop(bind=connection)

### Recreate and load

create_sending_table(send_table_name, db_engine)
df_send.to_sql(send_table_name, connection, index=False, if_exists='append')

connection.close()
