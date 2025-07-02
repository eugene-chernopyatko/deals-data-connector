from fast_bitrix24 import Bitrix

with open('Hook.txt') as HOOK:
    WEBHOOK = str(HOOK.read())


class LeadConnector:
    def __init__(self):
        self.client = Bitrix(WEBHOOK)
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
        # self.pd = DataFrame()
        self.new_lead_date_create = []
        self.lead_service = []
        self.formated_service = []

    def get_lead_list(self, year, month):
        """Return list of all leads from Bitrix and 1 custom field UF_CRM_1623137378375
        filter from yesterday to day_now"""

        params = {'filter': {'>DATE_CREATE': f'{year}-{month}-01T00:00:00',
                             'CATEGORY_ID': 19},
                  'select': ['ID', 'TITLE', 'DATE_CREATE'],
                  }
        deals = self.client.get_all('crm.deal.list', params)
        return deals


con = LeadConnector()

a = con.get_lead_list(2024, 7)
print(a)