import duckdb
import json
import os
import subprocess

class DuckDB:
    def __init__(self):
        self.db_path = 'database/myduckdb.db'
        self.create_database_if_not_exists()

    def create_database_if_not_exists(self):
        try:
            connection = duckdb.connect(self.db_path)
            connection.close()
            return 'Quack'
        except Exception as e:
            return str(e)

    def create_s3_access(self):
        try:
            connection = duckdb.connect(self.db_path, read_only=True)

            response = connection.execute(f"""
                CREATE OR REPLACE PERSISTENT SECRET secret (
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

    def duckdb_ingestion(self, schema_name, table_name):
        try:
            connection = duckdb.connect(self.db_path)

            result_df = connection.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS splitwise;
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
        
    def query_duckdb(self, sql_query: str):

        con = duckdb.connect(self.db_path)

        return con.execute(sql_query).df()


    def export_table_to_csv(self, schema_name: str, table_name: str):
        """Exporta uma tabela do DuckDB para um arquivo CSV."""
        try:
            output_file = f'exports/{table_name}.csv'
            connection = duckdb.connect(self.db_path)
            connection.execute(f"COPY {schema_name}.{table_name} TO '{output_file}' WITH (FORMAT 'csv', HEADER TRUE)")
            connection.close()

            return f'Tabela {schema_name}.{table_name} exportada para {output_file} com sucesso!'
        
        except Exception as e:
            return str(e)
        
    def run_dbt_command(self, command='dbt build'):
        """Executa o comando dbt na pasta dbt/splitwise_duckdb/."""
        try:
            os.chdir('dbt/splitwise_duckdb/')  
            
            result = subprocess.run(command.split(), capture_output=True, text=True)

            return {
                'stdout': result.stdout.splitlines(),
                'stderr': result.stderr.splitlines(),
                'returncode': result.returncode
            }
        except Exception as e:
            return str(e)
        finally:
            os.chdir('../../') 
