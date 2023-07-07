from Lead_Connector import LeadConnector
import time


con = LeadConnector()
# con.get_data_from_leads()
# con.create_data_frame()
# con.upload_data_to_sheet()
#
# time.sleep(2)
con.get_data_to_dict(2022)
con.create_dataframe_to_dict()

