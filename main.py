from fastapi import FastAPI, HTTPException, BackgroundTasks

from typing import Optional

from core.schema import OtherUsers, EmailSchema, UpdateExpensesRequest
from core.splitwise import Splitwise
from core.duckdb import DuckDB
from core.polars import Polars
from core.google import Google

app = FastAPI(title="Splitwise Connector REST API")

splitwise_client = Splitwise()
duckdb_client = DuckDB()
polars_client = Polars()
google_client = Google()

# Splitwise
@app.get("/get_expense", tags=["Splitwise"])
def get_expense(expense_id: int):
    try:
        response = splitwise_client.get_expense(expense_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/get_expenses", tags=["Splitwise"])
def get_expenses(
    group_id: Optional[int] = None,
    friend_id: Optional[int] = None,
    dated_after: Optional[str] = None,
    dated_before: Optional[str] = None,
    updated_after: Optional[str] = None,
    updated_before: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
):
    try:
        response = splitwise_client.get_expenses(
            group_id = group_id,
            friend_id = friend_id,
            dated_after = dated_after,
            dated_before = dated_before,
            updated_after = updated_after,
            updated_before = updated_before,
            limit = limit,
            offset = offset
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete_expense", tags=["Splitwise"])
def delete_expense(expense_id: int):
    try:
        response = splitwise_client.delete_expense(expense_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_groups", tags=["Splitwise"])
def get_groups():
    try:
        response = splitwise_client.get_groups()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_group", tags=["Splitwise"])
def get_group(group_id: int):
    try:
        response = splitwise_client.get_group(group_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_current_user", tags=["Splitwise"])
def get_current_user():
    try:
        response = splitwise_client.get_current_user()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/get_friends", tags=["Splitwise"])
def get_friends():
    try:
        response = splitwise_client.get_friends()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/create_expense", tags=["Splitwise"])
def create_expense(
    cost: float,
    description: str,
    group_id: int,
    details: str = None,
    date: str = None,
    repeat_interval: str = None,
    currency_code: str = None,
    category_id: int = None,
    main_user_id: int = 27512092,
    main_user_paid_share: float = None,
    main_user_owed_share: float = None,
    other_users: Optional[list[OtherUsers]]  = None
):
    
    if other_users == None:
       main_user_paid_share = cost
       main_user_owed_share = cost

    try:
        response = splitwise_client.create_expense(
                cost = cost,
                description = description,
                details = details,
                date = date,
                repeat_interval = repeat_interval,
                currency_code = currency_code,
                category_id = category_id,
                group_id = group_id,
                main_user_id = main_user_id,
                main_user_paid_share = main_user_paid_share,
                main_user_owed_share = main_user_owed_share,
                other_users = other_users 
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update_expense", tags=["Splitwise"])
def update_expense(
    expense_id: int,
    cost: float = None,
    description: str = None,
    group_id: int = None,
    details: str = None,
    date: str = None,
    repeat_interval: str = None,
    currency_code: str = None,
    category_id: int = None,
    main_user_id: int = None,
    main_user_paid_share: float = None,
    main_user_owed_share: float = None,
    other_users: Optional[list[OtherUsers]]  = None
):

    try:
        response = splitwise_client.update_expense(
                expense_id = expense_id,
                cost = cost,
                description = description,
                details = details,
                date = date,
                repeat_interval = repeat_interval,
                currency_code = currency_code,
                category_id = category_id,
                group_id = group_id,
                main_user_id = main_user_id,
                main_user_paid_share = main_user_paid_share,
                main_user_owed_share = main_user_owed_share,
                other_users = other_users 
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# DuckDB
@app.post("/create_database_if_not_exists", tags=["DuckDB"])
def create_database_if_not_exists():
    try:
        response = duckdb_client.create_database_if_not_exists()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query_duckdb", tags=["DuckDB"])
def query_duckdb(sql_query):
    try:
        response = duckdb_client.query_duckdb(sql_query)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/create_s3_access", tags=["DuckDB"])
def create_s3_access():
    try:
        response = duckdb_client.create_s3_access()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/duckdb_ingestion", tags=["DuckDB"])
def duckdb_ingestion(schema_name, table_name):
    try:
        response = duckdb_client.duckdb_ingestion(schema_name=schema_name, table_name=table_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/export_table_to_csv", tags=["DuckDB"])
def export_table_to_csv(schema_name, table_name):
    try:
        response = duckdb_client.export_table_to_csv(schema_name=schema_name, table_name=table_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/run_dbt_command", tags=["dbt"])
def run_dbt_endpoint(command='dbt build'):
    try:
        response = duckdb_client.run_dbt_command(command=command)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Polars S3
@app.post("/s3_expenses_ingestion", tags=["Polars S3"])
def s3_expenses_ingestion(mode='append', limit=20, updated_after=None, updated_before=None, dated_after=None, dated_before=None):
    try:
        response = polars_client.s3_expenses_ingestion(mode=mode, limit=limit, updated_after=updated_after, updated_before=updated_before, dated_after=dated_after, dated_before=dated_before)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/s3_groups_ingestion", tags=["Polars S3"])
def s3_groups_ingestion(mode='append'):
    try:
        response = polars_client.s3_groups_ingestion(mode=mode)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Google 
@app.post("/save_sheet_as_seed", tags=["Google"])
def save_sheet_as_seed(workbook_name, sheet_name):
    try:
        response = google_client.save_sheet_as_seed(workbook_name=workbook_name, sheet_name=sheet_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/save_listed_sheets_as_seeds", tags=["Google"])
def save_all_my_sheets_as_seeds():
    try:
        response = google_client.save_all_my_sheets_as_seeds()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/send_email/", tags=["Google"])
async def send_email_endpoint(email: EmailSchema, background_tasks: BackgroundTasks):
    background_tasks.add_task(google_client.send_email, email)
    return {"message": "E-mail enviado com sucesso!"}

# BATCH
@app.post("/update_expenses_month/", tags=["Batch"])
async def update_expenses_month(request: UpdateExpensesRequest):
    """
    Atualiza o campo details com os meses desejados.
    Espera um json na estrutura: 
    {
        "expenses": {
            "expense_id1": "month1",
            "expense_id2": "month2",
            ...
        }
    }
    """

    responses = []
    
    for expense_id, month in request.expenses.items():
        response = splitwise_client.update_expense(
            expense_id=expense_id,
            details=month
        )
        responses.append(response)
    
    return responses

@app.post("/refresh_splitwise/", tags=["Batch"])
async def refresh_splitwise(updated_after):
    """
    Atualiza Excel com dados do Splitwise.
    """

    errors = [] 

    response = polars_client.s3_expenses_ingestion(mode=None, limit=None, updated_after=updated_after, updated_before=None, dated_after=None, dated_before=None)
    
    if response.status != 200:
        errors.append("Falha na ingestão de despesas no S3.")

    if not errors:
        response = polars_client.s3_groups_ingestion(mode='replace')
        if response.status != 200:
            errors.append("Falha na ingestão de grupos no S3.")
    
    if not errors:
        response = duckdb_client.duckdb_ingestion(schema_name='splitwise', table_name='expenses')
        if response.status != 200:
            errors.append("Falha na ingestão de despesas no DuckDB.")
    
    if not errors:
        response = duckdb_client.duckdb_ingestion(schema_name='splitwise', table_name='groups')
        if response.status != 200:
            errors.append("Falha na ingestão de grupos no DuckDB.")
    
    if not errors:
        response = duckdb_client.export_table_to_csv(schema_name='main', table_name='master')
        if response.status != 200:
            errors.append("Falha ao exportar a tabela 'master' para CSV.")

    if not errors:
        response = duckdb_client.export_table_to_csv(schema_name='main', table_name='month')
        if response.status != 200:
            errors.append("Falha ao exportar a tabela 'month' para CSV.")
    
    if not errors:
        response = duckdb_client.export_table_to_csv(schema_name='main', table_name='master_limits_and_percentages')
        if response.status != 200:
            errors.append("Falha ao exportar a tabela 'master_limits_and_percentages' para CSV.")
    
    if not errors:
        response = duckdb_client.export_table_to_csv(schema_name='main', table_name='chart_limits_and_results')
        if response.status != 200:
            errors.append("Falha ao exportar a tabela 'chart_limits_and_results' para CSV.")
    
    if not errors:
        return {"status": "all true"}
    else:
        return {"status": "falhas", "detalhes": errors}
