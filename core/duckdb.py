import duckdb
import pandas as pd
import json
import os 

class DuckDB:
    def __init__(self):
        self.db_path = 'database/myduckdb'
        self.create_database_if_not_exists()

    def create_database_if_not_exists(self):
        try:
            connection = duckdb.connect(self.db_path)
            connection.close()
            return 'Quack 🦆'
        except Exception as e:
            return str(e)

    def create_s3_access(self):
        try:
            connection = duckdb.connect(self.db_path, read_only=True)

            response = connection.execute(f"""
                CREATE PERSISTENT SECRET secret (
                    TYPE S3,
                    KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID')}',
                    SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY')}',
                    REGION 'us-west-2'
                );
                INSTALL delta;
                LOAD delta;
                """).fetchall()

            connection.close()

            return {'Status': True, 'response': response}
        
        except Exception as e:
            return str(e)

    def query_duckdb(self, sql_query):
        try:
            connection = duckdb.connect(self.db_path, read_only=True)

            result_df = connection.execute(sql_query).fetchdf()

            connection.close()
            
            result = json.loads(result_df.to_json())

            return result
        except Exception as e:
            return str(e)

    def duckdb_ingestion(self, schema_name, table_name):
        try:
            connection = duckdb.connect(self.db_path)

            result_df = connection.execute(
                f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                (WITH ranked_records AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
                    FROM
                        delta_scan("s3://general-purpose-data/{schema_name}/{table_name}")
                )
                SELECT * EXCLUDE(rn) FROM ranked_records WHERE rn = 1 ORDER BY updated_at DESC);
                """
            ).fetchdf()

            connection.close()
            
            result = json.loads(result_df.to_json())

            return result
        
        except Exception as e:
            return str(e)
