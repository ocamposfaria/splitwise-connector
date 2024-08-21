from dotenv import load_dotenv
import polars as pl
import os 
from core.schema import expenses_schema_polars, users_schema_polars

from core.splitwise import Splitwise

load_dotenv()

splitwise_client = Splitwise()

class Polars():
    def __init__(self):
        self.storage_options = {
                "aws_access_key_id": f"{os.getenv('AWS_ACCESS_KEY_ID')}",
                "aws_secret_access_key": f"{os.getenv('AWS_SECRET_ACCESS_KEY')}",
                "aws_region": "us-west-2"
            }
        return
    
    def _ingest_s3_table(self, df, schema_name, table_name, mode):
            try:
                df.write_delta(
                    target=f's3://general-purpose-data/{schema_name}/{table_name}/',
                    mode=mode,
                    storage_options=self.storage_options
                )
                return 'Concluído'
            except Exception as e:
                return str(e)

    def s3_expenses_ingestion(self, mode, limit=20):
        try:
            data = splitwise_client.get_expenses(limit=limit)
            df = pl.DataFrame(data['expenses'], schema=expenses_schema_polars)
            self._ingest_s3_table(df=df, schema_name='splitwise', table_name='expenses', mode=mode)
            return 'Concluído'
        
        except Exception as e:
            return str(e)

    def s3_groups_ingestion(self, mode):
        try:
            data = splitwise_client.get_groups()
            df = pl.DataFrame(data['groups'], schema=users_schema_polars)
            return self._ingest_s3_table(df=df, schema_name='splitwise', table_name='groups', mode=mode)

        except Exception as e:
            return str(e)
