from dotenv import load_dotenv
import polars as pl
import os 
from core.schema import expenses_schema_polars, users_schema_polars
from core.splitwise import Splitwise
from icecream import ic

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
            return {
                "status_code": 200,
                "message": f"Table {table_name} ingested successfully into {schema_name}.",
                "record_count": df.shape[0]
            }
        except Exception as e:
            return {
                "status_code": 500,
                "message": f"Failed to ingest table {table_name}: {str(e)}"
            }

    def s3_expenses_ingestion(self, mode='append', limit=20, updated_after=None, updated_before=None, dated_after=None, dated_before=None):
        try:
            data = splitwise_client.get_expenses(limit=limit, updated_after=updated_after, updated_before=updated_before, dated_after=dated_after, dated_before=dated_before)['data']
            df = pl.DataFrame(data['expenses'], schema=expenses_schema_polars)
            result = self._ingest_s3_table(df=df, schema_name='splitwise', table_name='expenses', mode=mode)
            
            if result["status_code"] == 200:
                result["record_count"] = df.height
            
            return result
        except Exception as e:
            return {
                "status_code": 500,
                "message": f"Failed to fetch expenses: {str(e)}"
            }

    def s3_groups_ingestion(self, mode='append'):
        try:
            data = splitwise_client.get_groups()['data']
            df = pl.DataFrame(data['groups'], schema=users_schema_polars)
            return self._ingest_s3_table(df=df, schema_name='splitwise', table_name='groups', mode=mode)
        except Exception as e:
            return {
                "status_code": 500,
                "message": f"Failed to fetch groups: {str(e)}"
            }
