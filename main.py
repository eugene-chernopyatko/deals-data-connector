from Lead_Connector import LeadConnector

con = LeadConnector()
con.get_data_to_dict(2022)
con.create_dataframe_to_dict()