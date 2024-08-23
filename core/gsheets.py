import os
import pathlib
import polars as pl
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class Sheets:
    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 
                      'https://www.googleapis.com/auth/drive']

        credentials_dict = {
            "type": os.getenv("TYPE"),
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL")
        }

        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            credentials_dict, self.scope)
        self.client = gspread.authorize(self.credentials)


    def save_sheet_as_seed(self, workbook_name, sheet_name):
        output_folder = pathlib.Path("dbt/splitwise_duckdb/seeds/")
        os.makedirs(output_folder, exist_ok=True)

        sheet = self.client.open(workbook_name).worksheet(sheet_name)
        data = sheet.get_all_values()
        df = pl.DataFrame(data[1:], schema=data[0], orient="row")

        # this is used to replace commas with periods as thousands separators
        df = df.select([ 
            pl.col(column).str.replace(",", ".") for column in df.columns
        ])

        output_path = output_folder / f"seed_{sheet_name.lower().replace(' ', '_')}.csv"
        df.write_csv(output_path, separator=',')
        
        return 'Concluído'

    def save_all_my_sheets_as_seeds(self):
        status = {}
        for sheet in ['Ganhos', 'Limites', 'Presentes', 'Gastos futuros']:
            status[sheet] = self.save_sheet_as_seed('Suporte p orçamento', sheet)
        
        return status
