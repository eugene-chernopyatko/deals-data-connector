import datetime
from fast_bitrix24 import Bitrix
from pandas import DataFrame
import pandas
import gspread
import os
from datetime import date, timedelta


DAY_NOW = date.today()
YESTERDAY = DAY_NOW - timedelta(days=1)

with open('Hook.txt') as HOOK:
    WEBHOOK = str(HOOK.read())

with open('Google_Sheet_Url.txt') as Url_file:
    GOOGLE_SHEET_URL = str(Url_file.readline())


def get_bitrix_client():
    """Return client from Bitrix (using fast_bitrix24 module)"""
    b_client = Bitrix(WEBHOOK)
    return b_client


class LeadConnector:
    def __init__(self):
        self.client = get_bitrix_client()
        self.ID = []
        self.Title = []
        self.Company_title = []
        self.Source_id = []
        self.Source_descr = []
        self.Status_id = []
        self.Currency_ID = []
        self.Opportunity = []
        self.Assigned_by_id = []
        self.Date_create = []
        self.Date_modify = []
        self.Date_closed = []
        self.Status_semantic_id = []
        self.Origin_id = []
        self.Utm_source = []
        self.Utm_medium = []
        self.Utm_campaign = []
        self.Utm_content = []
        self.Utm_term = []
        self.Country = []
        self.Day_of_the_week = []
        self.work_dict = {}
        self.pd = DataFrame()
        self.new_lead_date_create = []
        self.lead_service = []
        self.formated_service = []

    def get_lead_list(self, year):
        """Return list of all leads from Bitrix and 1 custom field UF_CRM_1623137378375
        filter from yesterday to day_now"""
        lead_list = self.client.get_all('crm.lead.list',
                                        params={
                                            'filter': {'>DATE_CREATE': f'{year}-01-01T00:00:00'},
                                            'select': ['*', 'UF_CRM_1623137378375', 'UF_CRM_1623158614433'],
                                        }
                                        )
        return lead_list

    def get_deals_list(self, year):
        """Return list of deals"""
        lead_list = self.client.get_all('crm.deal.list',
                                        params={
                                            'filter': {'>DATE_CREATE': f'{year}-01-01T00:00:00'},
                                            'select': ['*', 'UF_CRM_60BF763052479']
                                            # 'filter': {'>DATE_CREATE': f'{YESTERDAY}',
                                            #            '<DATE_CREATE': f'{DAY_NOW}'},
                                        }
                                        )
        return lead_list

    # noinspection PyTypeChecker
    def get_data_from_leads(self):
        """Sorting data by lists"""

        list_of_leads = self.get_lead_list()
        for elem in list_of_leads:
            self.ID.append(elem.get('ID', ''))
            self.Title.append(elem.get('TITLE', ''))
            self.Company_title.append(elem.get('COMPANY_TITLE', ''))
            self.Source_id.append(elem.get('SOURCE_ID', ''))
            self.Source_descr.append(elem.get('SOURCE_DESCRIPTION'))
            self.Status_id.append(elem.get('STATUS_ID', ''))
            self.Currency_ID.append(elem.get('CURRENCY_ID', ''))
            self.Opportunity.append(elem.get('OPPORTUNITY', ''))
            self.Assigned_by_id.append(elem.get('ASSIGNED_BY_ID', ''))
            self.Date_create.append(elem.get('DATE_CREATE', ''))
            self.Date_modify.append(elem.get('DATE_MODIFY', ''))
            self.Date_closed.append(elem.get('DATE_CLOSED', ''))
            self.Status_semantic_id.append(elem.get('STATUS_SEMANTIC_ID', ''))
            self.Origin_id.append(elem.get('ORIGIN_ID', ''))
            self.Utm_source.append(elem.get('UTM_SOURCE', ''))
            self.Utm_medium.append(elem.get('UTM_MEDIUM', ''))
            self.Utm_campaign.append(elem.get('UTM_CAMPAIGN', ''))
            self.Utm_content.append(elem.get('UTM_CONTENT', ''))
            self.Utm_term.append(elem.get('UTM_TERM', ''))
            self.Country.append(elem.get('UF_CRM_1623137378375', '')),
            self.lead_service.append(elem.get('UF_CRM_1623158614433', ''))

        # for i in self.Date_create:
        #     str_to_date = datetime.datetime.strptime(i, "%Y-%m-%d")
        #     self.new_lead_date_create.append(str_to_date)

        for i in self.Date_create:
            splited_date_create = pandas.Timestamp(i.split('T')[0])
            if splited_date_create.dayofweek == 0:
                self.Day_of_the_week.append('Monday')
            elif splited_date_create.dayofweek == 1:
                self.Day_of_the_week.append('Tuesday')
            elif splited_date_create.dayofweek == 2:
                self.Day_of_the_week.append('Wednesday')
            elif splited_date_create.dayofweek == 3:
                self.Day_of_the_week.append('Thursday')
            elif splited_date_create.dayofweek == 4:
                self.Day_of_the_week.append('Friday')
            elif splited_date_create.dayofweek == 5:
                self.Day_of_the_week.append('Saturday')
            elif splited_date_create.dayofweek == 6:
                self.Day_of_the_week.append('Sunday')

        for i in self.lead_service:
            if i == '44':
                self.formated_service.append('mobile')
            elif i == '46':
                self.formated_service.append('корпоративний сайт')
            elif i == '48':
                self.formated_service.append('cервiс')
            elif i == '1707':
                self.formated_service.append('b2b')
            elif i == '50':
                self.formated_service.append('лендинг')
            elif i == '269':
                self.formated_service.append('iнтернет-магазин')
            elif i == '52':
                self.formated_service.append('маркетинг')
            elif i == '54':
                self.formated_service.append('пiдтримка')
            elif i == '433':
                self.formated_service.append('чат-бот')
            elif i == '545':
                self.formated_service.append('UX')
            elif i == '547':
                self.formated_service.append('UI')
            elif i == '1431':
                self.formated_service.append('UX+UI')
            elif i == '1721':
                self.formated_service.append('маркетплейс')
            elif i == '775':
                self.formated_service.append('доопрацювання')
            elif i == '969':
                self.formated_service.append('frontend')
            elif i == '1261':
                self.formated_service.append('партнерка')
            elif i == '765':
                self.formated_service.append('вiдео-продакшн')
            elif i == '745':
                self.formated_service.append('продажа')
            elif i == '747':
                self.formated_service.append('впровадження')
            elif i == '747':
                self.formated_service.append('впровадження')
            elif i == '1273':
                self.formated_service.append('продажа та впровадження')
            elif i == '1351':
                self.formated_service.append('им+мобайл')
            elif i == '1881':
                self.formated_service.append('m2+app')
            elif i == '1371':
                self.formated_service.append('хостинг')
            elif i == '1409':
                self.formated_service.append('брендінг')
            elif i == '1675':
                self.formated_service.append('велике корпоративне впровадження')
            elif i == '1949':
                self.formated_service.append('таргетинг')
            elif i == '1951':
                self.formated_service.append('seo')
            elif i == '1953':
                self.formated_service.append('контекст')
            elif i == '1955':
                self.formated_service.append('linkedin')
            elif i == '1957':
                self.formated_service.append('smm')
            elif i == '581':
                self.formated_service.append('не визначена')
            else:
                self.formated_service.append('-')

    def get_data_to_dict(self, year):
        list_of_leads = self.get_lead_list(year)
        list_of_deals = self.get_deals_list(year)
        self.Day_of_the_week = []
        self.formated_service = []
        counter = 0

        for elem in list_of_leads:
            self.Date_create.append(elem.get('DATE_CREATE', ''))
            self.lead_service.append(elem.get('UF_CRM_1623158614433', ''))

        for i in self.Date_create:
            splited_date_create = pandas.Timestamp(i.split('T')[0])
            if splited_date_create.dayofweek == 0:
                self.Day_of_the_week.append('Monday')
            elif splited_date_create.dayofweek == 1:
                self.Day_of_the_week.append('Tuesday')
            elif splited_date_create.dayofweek == 2:
                self.Day_of_the_week.append('Wednesday')
            elif splited_date_create.dayofweek == 3:
                self.Day_of_the_week.append('Thursday')
            elif splited_date_create.dayofweek == 4:
                self.Day_of_the_week.append('Friday')
            elif splited_date_create.dayofweek == 5:
                self.Day_of_the_week.append('Saturday')
            elif splited_date_create.dayofweek == 6:
                self.Day_of_the_week.append('Sunday')

        for i in self.lead_service:
            if i == '44':
                self.formated_service.append('mobile')
            elif i == '46':
                self.formated_service.append('корпоративний сайт')
            elif i == '48':
                self.formated_service.append('cервiс')
            elif i == '1707':
                self.formated_service.append('b2b')
            elif i == '50':
                self.formated_service.append('лендинг')
            elif i == '269':
                self.formated_service.append('iнтернет-магазин')
            elif i == '52':
                self.formated_service.append('маркетинг')
            elif i == '54':
                self.formated_service.append('пiдтримка')
            elif i == '433':
                self.formated_service.append('чат-бот')
            elif i == '545':
                self.formated_service.append('UX')
            elif i == '547':
                self.formated_service.append('UI')
            elif i == '1431':
                self.formated_service.append('UX+UI')
            elif i == '1721':
                self.formated_service.append('маркетплейс')
            elif i == '775':
                self.formated_service.append('доопрацювання')
            elif i == '969':
                self.formated_service.append('frontend')
            elif i == '1261':
                self.formated_service.append('партнерка')
            elif i == '765':
                self.formated_service.append('вiдео-продакшн')
            elif i == '745':
                self.formated_service.append('продажа')
            elif i == '747':
                self.formated_service.append('впровадження')
            elif i == '747':
                self.formated_service.append('впровадження')
            elif i == '1273':
                self.formated_service.append('продажа та впровадження')
            elif i == '1351':
                self.formated_service.append('им+мобайл')
            elif i == '1881':
                self.formated_service.append('m2+app')
            elif i == '1371':
                self.formated_service.append('хостинг')
            elif i == '1409':
                self.formated_service.append('брендінг')
            elif i == '1675':
                self.formated_service.append('велике корпоративне впровадження')
            elif i == '1949':
                self.formated_service.append('таргетинг')
            elif i == '1951':
                self.formated_service.append('seo')
            elif i == '1953':
                self.formated_service.append('контекст')
            elif i == '1955':
                self.formated_service.append('linkedin')
            elif i == '1957':
                self.formated_service.append('smm')
            elif i == '581':
                self.formated_service.append('не визначена')
            else:
                self.formated_service.append('-')

        for elem in list_of_leads:
            self.work_dict[elem['ID']] = {
                'Title': elem.get('TITLE', ''),
                'COMPANY_TITLE': elem.get('COMPANY_TITLE', ''),
                'SOURCE_ID': elem.get('SOURCE_ID', ''),
                'SOURCE_DESCRIPTION': elem.get('SOURCE_DESCRIPTION', ''),
                'STATUS_ID': elem.get('STATUS_ID', ''),
                'CURRENCY_ID': elem.get('CURRENCY_ID', ''),
                'OPPORTUNITY': elem.get('OPPORTUNITY', ''),
                'ASSIGNED_BY_ID': elem.get('ASSIGNED_BY_ID', ''),
                'DATE_CREATE': elem.get('DATE_CREATE', ''),
                'DATE_MODIFY': elem.get('DATE_MODIFY', ''),
                'DATE_CLOSED': elem.get('DATE_CLOSED', ''),
                'STATUS_SEMANTIC_ID': elem.get('STATUS_SEMANTIC_ID', ''),
                'ORIGIN_ID': elem.get('ORIGIN_ID', ''),
                'UTM_SOURCE': elem.get('UTM_SOURCE', ''),
                'UTM_MEDIUM': elem.get('UTM_MEDIUM', ''),
                'UTM_CAMPAIGN': elem.get('UTM_CAMPAIGN', ''),
                'UTM_CONTENT': elem.get('UTM_CONTENT', ''),
                'UTM_TERM': elem.get('UTM_TERM', ''),
                'Country': elem.get('UF_CRM_1623137378375', ''),
                'Day_of_the_week': self.Day_of_the_week[counter],
                'Service': self.formated_service[counter]
            }
            counter += 1

        for deals in list_of_deals:
            if deals['LEAD_ID'] in self.work_dict.keys():
                self.work_dict[deals['LEAD_ID']]['Deals_Title'] = deals.get('TITLE', '')
                self.work_dict[deals['LEAD_ID']]['Deals_TYPE_ID'] = deals.get('TYPE_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_STAGE_ID'] = deals.get('STAGE_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_PROBABILITY'] = deals.get('PROBABILITY', '')
                self.work_dict[deals['LEAD_ID']]['Deals_CURRENCY_ID'] = deals.get('CURRENCY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_OPPORTUNITY'] = deals.get('OPPORTUNITY', '')
                self.work_dict[deals['LEAD_ID']]['Deals_IS_MANUAL_OPPORTUNITY'] = deals.get('IS_MANUAL_OPPORTUNITY', '')
                self.work_dict[deals['LEAD_ID']]['Deals_TAX_VALUE'] = deals.get('TAX_VALUE', '')
                self.work_dict[deals['LEAD_ID']]['Deals_COMPANY_ID'] = deals.get('COMPANY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_CONTACT_ID'] = deals.get('CONTACT_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_QUOTE_ID'] = deals.get('QUOTE_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_BEGINDATE'] = deals.get('BEGINDATE', '')
                self.work_dict[deals['LEAD_ID']]['Deals_CLOSEDATE'] = deals.get('CLOSEDATE', '')
                self.work_dict[deals['LEAD_ID']]['Deals_ASSIGNED_BY_ID'] = deals.get('ASSIGNED_BY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_CREATED_BY_ID'] = deals.get('CREATED_BY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_MODIFY_BY_ID'] = deals.get('MODIFY_BY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_DATE_CREATE'] = deals.get('DATE_CREATE', '')
                self.work_dict[deals['LEAD_ID']]['Deals_DATE_MODIFY'] = deals.get('DATE_MODIFY', '')
                self.work_dict[deals['LEAD_ID']]['Deals_OPENED'] = deals.get('OPENED', '')
                self.work_dict[deals['LEAD_ID']]['Deals_CLOSED'] = deals.get('CLOSED', '')
                self.work_dict[deals['LEAD_ID']]['Deals_CATEGORY_ID'] = deals.get('CATEGORY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_STAGE_SEMANTIC_ID'] = deals.get('STAGE_SEMANTIC_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_IS_NEW'] = deals.get('IS_NEW', '')
                self.work_dict[deals['LEAD_ID']]['Deals_IS_RECURRING'] = deals.get('IS_RECURRING', '')
                self.work_dict[deals['LEAD_ID']]['Deals_SOURCE_ID'] = deals.get('SOURCE_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_SOURCE_DESCRIPTION'] = deals.get('SOURCE_DESCRIPTION', '')
                self.work_dict[deals['LEAD_ID']]['Deals_ORIGINATOR_ID'] = deals.get('ORIGINATOR_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_ORIGIN_ID'] = deals.get('ORIGIN_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_MOVED_BY_ID'] = deals.get('MOVED_BY_ID', '')
                self.work_dict[deals['LEAD_ID']]['Deals_MOVED_TIME'] = deals.get('MOVED_TIME', '')
                self.work_dict[deals['LEAD_ID']]['Deals_UTM_SOURCE'] = deals.get('UTM_SOURCE', '')
                self.work_dict[deals['LEAD_ID']]['Deals_UTM_MEDIUM'] = deals.get('UTM_MEDIUM', '')
                self.work_dict[deals['LEAD_ID']]['Deals_UTM_CAMPAIGN'] = deals.get('UTM_CAMPAIGN', '')
                self.work_dict[deals['LEAD_ID']]['Deals_UTM_CONTENT'] = deals.get('UTM_CONTENT', '')
                self.work_dict[deals['LEAD_ID']]['Deals_UTM_TERM'] = deals.get('UTM_TERM', '')
                # self.work_dict[deals['LEAD_ID']]['Service'] = deals.get('UF_CRM_60BF763052479', '')

            # for keys in self.work_dict:
            #     try:
            #         if self.work_dict[keys]['Service'] in service_dict.keys():
            #             self.work_dict[keys]['Service'] = service_dict[self.work_dict[keys]['Service']]
            #     #     if self.work_dict[keys]['Service'] == '86':
            #     #         self.work_dict[keys]['Service'] = 'mobile'
            #     # elif keys['Service'] == '90':
            #     #     keys['Service'] = 'cервiс сайт'
            #     # elif keys['Service'] == '1713':
            #     #     keys['Service'] = 'b2b'
            #     # elif keys['Service'] == '92':
            #     #     keys['Service'] = 'лендинг'
            #
            #     except KeyError:
            #         pass
            #     else:
            #         continue

    def create_data_frame(self):
        """Creating Data Frames from Pandas"""
        self.pd['ID'] = self.ID
        self.pd['Title'] = self.Title
        self.pd['Company_title'] = self.Company_title
        self.pd['Source_id'] = self.Source_id
        self.pd['Source_descr'] = self.Source_descr
        self.pd['Status_id'] = self.Status_id
        self.pd['Currency_ID'] = self.Currency_ID
        self.pd['Opportunity'] = self.Opportunity
        self.pd['Assigned_by_id'] = self.Assigned_by_id
        self.pd['Date_create'] = self.Date_create
        self.pd['Date_modify'] = self.Date_modify
        self.pd['Date_closed'] = self.Date_closed
        self.pd['Status_semantic_id'] = self.Status_semantic_id
        self.pd['Origin_id'] = self.Origin_id
        self.pd['Utm_source'] = self.Utm_source
        self.pd['Utm_medium'] = self.Utm_medium
        self.pd['Utm_campaign'] = self.Utm_campaign
        self.pd['Utm_content'] = self.Utm_content
        self.pd['Utm_term'] = self.Utm_term
        self.pd['Country'] = self.Country
        self.pd['Day_of_the_week'] = self.Day_of_the_week
        self.pd['Lead_Service'] = self.formated_service

    def create_dataframe_to_dict(self):
        df = pandas.DataFrame.from_dict(self.work_dict,
                                        orient='index').fillna(' ')

        key_p = os.path.abspath('test-python-334320-3a0cd1d02f70.json')
        gc = gspread.service_account(filename=key_p)
        sheet = gc.open_by_url(GOOGLE_SHEET_URL)
        worksheet = sheet.get_worksheet(1)
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print('\nData Updated')

    # noinspection PyTypeChecker
    def upload_data_to_sheet(self):
        """Uploading data from pandas data frames to google sheet"""
        key_p = os.path.abspath('test-python-334320-3a0cd1d02f70.json')
        gc = gspread.service_account(filename=key_p)
        sheet = gc.open_by_url(GOOGLE_SHEET_URL)
        worksheet = sheet.get_worksheet(0)
        # worksheet.resize(rows=len(self.ID))
        worksheet.update([self.pd.columns.values.tolist()] + self.pd.values.tolist())
        print('\nData Updated')
