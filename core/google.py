import os
import pathlib
import polars as pl
import gspread
import smtplib
from oauth2client.service_account import ServiceAccountCredentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from core.schema import EmailSchema

class Google:
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
        self.sender_email = os.getenv('GOOGLE_EMAIL_SENDER')
        self.sender_password = os.getenv('GOOGLE_PASSWORD_SENDER')

    def save_sheet_as_seed(self, workbook_name, sheet_name):
        try:
            output_folder = pathlib.Path("dbt/splitwise_duckdb/seeds/")
            os.makedirs(output_folder, exist_ok=True)

            sheet = self.client.open(workbook_name).worksheet(sheet_name)
            data = sheet.get_all_values()
            df = pl.DataFrame(data[1:], schema=data[0], orient="row")

            # Replace commas with periods as thousands separators
            df = df.select([ 
                pl.col(column).str.replace(",", ".") for column in df.columns
            ])

            output_path = output_folder / f"seed_{sheet_name.lower().replace(' ', '_')}.csv"
            df.write_csv(output_path, separator=',')
            
            return {"message": f"Planilha {sheet_name} salva com sucesso.", "status_code": 200}

        except Exception as e:
            return {"message": f"Erro ao salvar a planilha {sheet_name}: {str(e)}", "status_code": 500}

    def send_email(self, email: EmailSchema):
        sender_email = self.sender_email
        sender_password = self.sender_password
        receiver_email = email.email

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = email.subject

        msg.attach(MIMEText(email.message, "plain"))

        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            server.close()
            return {"message": "E-mail enviado com sucesso.", "status_code": 200}
        except Exception as e:
            return {"message": f"Erro ao enviar o e-mail: {str(e)}", "status_code": 500}
