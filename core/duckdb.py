import duckdb
import json
import os
import subprocess
from http import HTTPStatus
import pandas as pd
from core.splitwise import Splitwise

splitwise_client = Splitwise()

class DuckDB:
    def __init__(self):
        self.db_path = 'database/newduckdb.db'
        self.create_database_if_not_exists()

    def create_database_if_not_exists(self):
        try:
            connection = duckdb.connect(self.db_path)
            connection.close()
            return {'status_code': 200, 'message': 'Database created SUCCESSFULLY.'}
        except Exception as e:
            return {'status_code': HTTPStatus.INTERNAL_SERVER_ERROR, 'message': f'Error creating database: {str(e)}'}

    def create_s3_access(self):
        try:
            connection = duckdb.connect(self.db_path, read_only=True)

            connection.execute(f"""
                CREATE OR REPLACE PERSISTENT SECRET secret (
                    TYPE S3,
                    KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID')}',
                    SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY')}',
                    REGION 'us-west-2'
                );
                INSTALL delta;
                LOAD delta;
                """)
            
            connection.close()

            return {'status_code': 200, 'message': 'S3 access created SUCCESSFULLY.'}
        
        except Exception as e:
            return {'status_code': HTTPStatus.INTERNAL_SERVER_ERROR, 'message': f'Error creating S3 access: {str(e)}'}

    def duckdb_ingestion(self, schema_name, table_name):
        try:
            connection = duckdb.connect(self.db_path)

            result_df = connection.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS {schema_name};
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                (WITH ranked_records AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
                    FROM
                        delta_scan('s3://general-purpose-data/{schema_name}/{table_name}')
                )
                SELECT * EXCLUDE(rn) FROM ranked_records WHERE rn = 1 ORDER BY updated_at DESC);
                """
            ).fetchdf()

            connection.close()

            result = json.loads(result_df.to_json())
            return {'status_code': 200, 'message': 'Data ingestion successful.', 'data': result}
        
        except Exception as e:
            return {'status_code': HTTPStatus.INTERNAL_SERVER_ERROR, 'message': f'Error in data ingestion: {str(e)}'}

    def duckdb_direct_ingestion(self, table_name, limit, updated_after, updated_before, dated_after, dated_before):

            connection = duckdb.connect('database/newduckdb.db')

            if table_name == 'expenses':
                data = splitwise_client.get_expenses(limit=limit, updated_after=updated_after, updated_before=updated_before, dated_after=dated_after, dated_before=dated_before)['data']
                df = pd.DataFrame(data=data['expenses'])

            elif table_name == 'groups':
                data = splitwise_client.get_groups()['data']
                df = pd.DataFrame(data=data['groups'])

            connection.register('df_tmp', df)
            result_df = connection.execute(
            f"""
                    CREATE SCHEMA IF NOT EXISTS splitwise
                    ;
                    CREATE OR REPLACE TABLE splitwise.tmp_{table_name} AS SELECT * FROM df_tmp
                    ;
                    CREATE TABLE IF NOT EXISTS splitwise.{table_name} AS SELECT * FROM splitwise.tmp_{table_name} 
                    ;
                    INSERT INTO newduckdb.splitwise.{table_name}
                    SELECT * FROM newduckdb.splitwise.tmp_{table_name}
                    ;
                    CREATE OR REPLACE TABLE splitwise.{table_name} AS 
                    (WITH ranked_records AS (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
                        FROM
                            newduckdb.splitwise.{table_name}
                    )
                    SELECT * EXCLUDE(rn) FROM ranked_records WHERE rn = 1 ORDER BY updated_at DESC)
                    ;
            """
            ).fetchdf()
            
            connection.close()

            result = json.loads(result_df.to_json())
            return {'status_code': 200, 'message': 'Data ingestion successful.', 'data': result}
        


    def query_duckdb(self, sql_query: str):
        try:
            con = duckdb.connect(self.db_path)
            result_df = con.execute(sql_query).df()
            return {'status_code': 200, 'message': 'Query executed SUCCESSFULLY.', 'data': result_df}
        except Exception as e:
            return {'status_code': HTTPStatus.BAD_REQUEST, 'message': f'Error executing query: {str(e)}'}
    
    def export_table_to_csv(self, wichduckdb, schema_name: str, table_name: str):
        try:
            if wichduckdb == 'new':
                output_file = f'exports/currents/{table_name}.csv'
                connection = duckdb.connect('database/newduckdb.db')
            else:
                output_file = f'exports/{table_name}.csv'
                connection = duckdb.connect(self.db_path)
            connection.execute(f"COPY {schema_name}.{table_name} TO '{output_file}' WITH (FORMAT 'csv', HEADER TRUE)")
            connection.close()

            return {'status_code': 200, 'message': f'Table {schema_name}.{table_name} exported to {output_file} SUCCESSFULLY.'}
        
        except Exception as e:
            return {'status_code': HTTPStatus.INTERNAL_SERVER_ERROR, 'message': f'Error exporting table: {str(e)}'}
        
    def run_dbt_command(self, project, command='dbt build'):
        try:
            if project == 'old':
                os.chdir('dbt/splitwise_duckdb/')
            if project == 'new':
                os.chdir('dbt_currents/splitwise_currents/')

            result = subprocess.run(command.split(), capture_output=True, text=True)

            if result.returncode == 0:
                return {'status_code': 200, 'message': 'dbt command executed SUCCESSFULLY.', 'stdout': result.stdout.splitlines()}
            else:
                return {'status_code': HTTPStatus.INTERNAL_SERVER_ERROR, 'message': 'dbt command FAILED.', 'stderr': result.stderr.splitlines()}
        
        except Exception as e:
            return {'status_code': HTTPStatus.INTERNAL_SERVER_ERROR, 'message': f'Error running dbt command: {str(e)}'}
        
        finally:
            os.chdir('../../')