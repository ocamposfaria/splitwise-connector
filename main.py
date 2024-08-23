from fastapi import FastAPI, HTTPException

from typing import Optional

from core.schema import OtherUsers
from core.splitwise import Splitwise
from core.duckdb import DuckDB
from core.polars import Polars
from core.gsheets import Sheets


app = FastAPI(title="Splitwise Connector REST API")

splitwise_client = Splitwise()
duckdb_client = DuckDB()
polars_client = Polars()
gsheets_client = Sheets()

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

# Polars S3
@app.post("/s3_expenses_ingestion", tags=["Polars S3"])
def s3_expenses_ingestion(mode='append', limit=20):
    try:
        response = polars_client.s3_expenses_ingestion(mode=mode, limit=limit)
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

# Google Sheets
@app.post("/save_sheet_as_seed", tags=["Google Sheets"])
def save_sheet_as_seed(workbook_name, sheet_name):
    try:
        response = gsheets_client.save_sheet_as_seed(workbook_name=workbook_name, sheet_name=sheet_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/save_all_my_sheets_as_seeds", tags=["Google Sheets"])
def save_all_my_sheets_as_seeds():
    try:
        response = gsheets_client.save_all_my_sheets_as_seeds()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
