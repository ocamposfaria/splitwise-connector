import duckdb

db_path = 'database/newduckdb.db'

def get_groups():
    connection = duckdb.connect(db_path)
    result_df = connection.execute(
        """
        SELECT group_name as col FROM master GROUP BY 1
        """
    ).fetchdf()
    return result_df['col'].to_list()

def get_expenses():
    connection = duckdb.connect(db_path)
    result_df = connection.execute(
        """
        SELECT description as col FROM master GROUP BY 1
        """
    ).fetchdf()
    return result_df['col'].to_list()

def get_suggested_value(gasto):
    connection = duckdb.connect(db_path)
    result_df = connection.execute(
        """
        SELECT cost 
        FROM master
        WHERE description = ?
        LIMIT 1
        """,
        [gasto]
    ).fetchdf()

    if not result_df.empty and result_df['cost'][0] is not None:
        return result_df['cost'][0]
    else:
        return None

def get_suggested_category(gasto):
    connection = duckdb.connect(db_path)
    result_df = connection.execute(
        """
        SELECT category 
        FROM master
        WHERE description = ?
        LIMIT 1
        """,
        [gasto]
    ).fetchdf()

    if not result_df.empty and result_df['category'][0] is not None:
        return result_df['category'][0]
    else:
        return None
