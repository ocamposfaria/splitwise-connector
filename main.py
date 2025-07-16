from fastapi import FastAPI, HTTPException, BackgroundTasks
from datetime import datetime, timedelta
import re

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
        response = splitwise_client.get_expense(expense_id)['data']
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
        response = splitwise_client.get_groups()['data']
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
        response = splitwise_client.get_current_user()['data']
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/get_friends", tags=["Splitwise"])
def get_friends():
    try:
        response = splitwise_client.get_friends()['data']
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
    second_user_id: int = None,
    second_user_paid_share: float = None,
    second_user_owed_share: float = None,
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
                second_user_id = second_user_id,
                second_user_paid_share = second_user_paid_share,
                second_user_owed_share = second_user_owed_share,
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
    
@app.post("/duckdb_direct_ingestion", tags=["DuckDB"])
def duckdb_ingestion(table_name, updated_after = (datetime.utcnow() - timedelta(weeks=2)).strftime('%Y-%m-%dT%H:%M:%SZ')):
    try:
        response = duckdb_client.duckdb_direct_ingestion(table_name=table_name, limit=1000, updated_after=updated_after, updated_before=None, dated_after=None, dated_before=None)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/export_table_to_csv", tags=["DuckDB"])
def export_table_to_csv(wichduckdb, schema_name, table_name):
    try:
        response = duckdb_client.export_table_to_csv(wichduckdb=wichduckdb, schema_name=schema_name, table_name=table_name)
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


@app.post("/update_expenses_categories/", tags=["Batch"])
async def update_expenses_categories(request: UpdateExpensesRequest):
    """
    Recebe um dicionário com nova categoria a ser colocada entre os [] do campo description, e o expense_id.
    {
        "expenses": {
            "expense_id1": "category1",
            "expense_id2": "category2",
            ...
        }
    }

    Exemplo: se o description de um gasto for "[padaria] dona de casa", e eu quiser mudar a categoria para "mercado", 
    então o novo description vai ser "[mercado] dona de casa"
    """
    responses = []

    for expense_id, new_category in request.expenses.items():
        expense = splitwise_client.get_expense(expense_id)['data']
        description = expense['expense']['description']

        if re.search(r"\[.*?\]", description):
            new_description = re.sub(r"\[.*?\]", f"[{new_category}]", description)
        else:
            new_description = f"[{new_category}] {description}"

        updated_expense = splitwise_client.update_expense(
            expense_id=expense_id,
            description=new_description
        )

        responses.append({
            "expense_id": expense_id,
            "old_description": description,
            "new_description": new_description
        })

    return responses


@app.post("/save_all_my_sheets_as_seeds/", tags=["Batch"])
async def save_all_my_sheets_as_seeds(version='new'):

    if version == 'old':
        responses = []
        for sheet in ['Ganhos', 'Limites', 'Presentes', 'Gastos futuros']:
                response = google_client.save_sheet_as_seed('Suporte p orçamento', sheet, path="dbt/splitwise_duckdb/seeds/")
                responses.append(response)
    if version == 'new':
        responses = []
        for sheet in ['Ganhos', 'Limites', 'Gastos futuros']:
                response = google_client.save_sheet_as_seed('Extra Inputs', sheet, path="dbt_currents/splitwise_currents/seeds/")
                responses.append(response)
    
    return responses


@app.post("/refresh_splitwise/", tags=["Batch"])
async def refresh_splitwise(updated_after = (datetime.utcnow() - timedelta(weeks=2)).strftime('%Y-%m-%dT%H:%M:%SZ')):
    """
    Atualiza Excel com dados do Splitwise.
    """

    errors = [] 
    successes = []

    response = polars_client.s3_expenses_ingestion(mode='append', limit=1000, updated_after=updated_after, updated_before=None, dated_after=None, dated_before=None)
    if response['status_code'] != 200:
        error_message = response.get('message', 'Erro desconhecido.')
        errors.append(f"Falha na ingestão de despesas no S3: {error_message}")
    else:
        successes.append(response)

    if not errors:
        response = polars_client.s3_groups_ingestion(mode='overwrite')
        if response['status_code'] != 200:
            error_message = response.get('message', 'Erro desconhecido.')
            errors.append(f"Falha na ingestão de grupos no S3: {error_message}")
        else:
            successes.append(response)

    if not errors:
        response = duckdb_client.duckdb_ingestion(schema_name='splitwise', table_name='expenses')
        if response['status_code'] != 200:
            error_message = response.get('message', 'Erro desconhecido.')
            errors.append(f"Falha na ingestão de despesas no DuckDB: {error_message}")
        else:
            successes.append(response)
    
    if not errors:
        response = duckdb_client.duckdb_ingestion(schema_name='splitwise', table_name='groups')
        if response['status_code'] != 200:
            error_message = response.get('message', 'Erro desconhecido.')
            errors.append(f"Falha na ingestão de grupos no DuckDB: {error_message}")
        else:
            successes.append(response)

    if not errors:
        response = duckdb_client.run_dbt_command(project='old', command='dbt build')
        if response['status_code'] != 200:
            error_message = response.get('message', 'Erro desconhecido.')
            errors.append(f"Falha na execução do `dbt build`: {error_message}")
        else:
            successes.append(response)

    if not errors:
        export_response = await export_all_tables_to_csv()
        successes.append(export_response)
    
    if not errors:
        return {"status": 200, "message": "all true", "success_responses": successes}
    else:
        return {"status": 500, "message": errors}

@app.post("/export_all_tables_to_csv/", tags=["Batch"])
async def export_all_tables_to_csv(duckdb='old'):
    responses = []
    if duckdb=='old':
        for table_name in [
            'master', 
            'month', 
            'master_limits_and_percentages', 
            'chart_limits_and_results', 
            'overall_costs_future_estimated',
            'validate_percentages',
            'validate_bills']:
            response = duckdb_client.export_table_to_csv(wichduckdb=duckdb, schema_name='main', table_name=table_name)
            responses.append(response)
    else:
        for table_name in [
            'master',
            'master_limits',
            'months',
            'overall']:
            response = duckdb_client.export_table_to_csv(wichduckdb=duckdb, schema_name='main', table_name=table_name)
            responses.append(response)
    
    return responses

@app.post("/correct_expenses_percentages/", tags=["Batch"])
async def correct_expenses_percentages(except_those: str = None):
    result = duckdb_client.query_duckdb("""SELECT * FROM main.validate_percentages""")
    
    if result.get("data") is None:
        return {"status": 500, "message": "Erro ao consultar DuckDB"}
    
    df = result["data"]

    if df.empty:
        return {"status": 404, "message": "Nenhum dado encontrado"}

    if except_those:
        except_ids = except_those.split(";")
    else:
        except_ids = []

    update_results = []

    for _, row in df.iterrows():
        expense_id = str(row['expense_id'])

        if expense_id in except_ids:
            continue  

        result = splitwise_client.update_expense(
            expense_id=expense_id,
            main_user_id=row['main_user_id'],
            main_user_owed_share=row['main_user_owed_share'],
            main_user_paid_share=row['main_user_paid_share'],
            second_user_id=row['second_user_id'],
            second_user_paid_share=row['second_user_paid_share'],
            second_user_owed_share=row['second_user_owed_share']
        )

        update_results.append(result)

    return {"status": 200, "update_results": update_results}

@app.post("/refresh_splitwise_new_pipeline/", tags=["Batch"])
async def refresh_splitwise_new(updated_after = (datetime.utcnow() - timedelta(weeks=2)).strftime('%Y-%m-%dT%H:%M:%SZ')):
    """
    Atualiza Excel com dados do Splitwise.
    """

    errors = [] 
    successes = []

    if not errors:
        response = duckdb_client.duckdb_direct_ingestion(table_name='expenses', limit=1000, updated_after=updated_after, updated_before=None, dated_after=None, dated_before=None)
        if response['status_code'] != 200:
            error_message = response.get('message', 'Erro desconhecido.')
            errors.append(f"Falha na ingestão de despesas no DuckDB: {error_message}")
        else:
            successes.append(response)
    
    # if not errors:
    #     response = duckdb_client.duckdb_direct_ingestion(table_name='groups', limit=1000, updated_after=updated_after, updated_before=None, dated_after=None, dated_before=None)
    #     if response['status_code'] != 200:
    #         error_message = response.get('message', 'Erro desconhecido.')
    #         errors.append(f"Falha na ingestão de grupos no DuckDB: {error_message}")
    #     else:
    #         successes.append(response)

    if not errors:
        response = await save_all_my_sheets_as_seeds()
        successes.append(response)

    if not errors:
        response = duckdb_client.run_dbt_command(project='new', command='dbt build')
        if response['status_code'] != 200:
            error_message = response.get('message', 'Erro desconhecido.')
            errors.append(f"Falha na execução do `dbt build`: {error_message}")
        else:
            successes.append(response)

    if not errors:
        export_response = await export_all_tables_to_csv(duckdb='new')
        successes.append(export_response)
    
    if not errors:
        return {"status": 200, "message": "all true", "success_responses": successes}
    else:
        return {"status": 500, "message": errors}
