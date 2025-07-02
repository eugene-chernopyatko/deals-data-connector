import datetime
from fast_bitrix24 import Bitrix
from pandas import DataFrame
import pandas
import gspread
import os
from datetime import date, timedelta

with open('Hook.txt') as HOOK:
    WEBHOOK = str(HOOK.read())


def get_lead_list(year):
    """Return list of all leads from Bitrix and 1 custom field UF_CRM_1623137378375
    filter from yesterday to day_now"""
    client = Bitrix(WEBHOOK)
    lead_list = client.get_all('crm.lead.list',
                               params={
                                   'filter': {'>DATE_CREATE': f'{year}-01-01T00:00:00', 'STATUS_ID': 'CONVERTED'},
                                   'select': ['*', 'UF_*'],
                               }
                               )
    # for i in lead_list:
    #     if i['STATUS_ID'] == 'CONVERTED':
    #         print()
    return lead_list


leads = get_lead_list(2024)

print(leads[1])
