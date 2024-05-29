import os
import sqlite3
import pandas as pd

DATABASE_FOLDER = os.path.join(os.getcwd(), "db")
DATABASE_PATH = os.path.join(DATABASE_FOLDER, "example.db")

if not os.path.exists(DATABASE_FOLDER):
    os.makedirs(DATABASE_FOLDER)


def create_table(cursor:sqlite3.Cursor, table_name:str, columns:str|list[str]):
    if isinstance(columns, str):
        columns.strip(",")
        columns = columns.split(",")
    elif isinstance(columns, list):
        columns = [str(column) for column in columns]
    else:
        raise ValueError(f"Parameter \"columns\" must be str|list[str] not {type(columns)}.")
    columns = [column.strip() for column in columns if column.strip()]
    columns = ",".join(columns)

    operation = f"CREATE TABLE IF NOT EXISTS {table_name}({columns})"
    cursor.execute(operation)


def get_tables(cursor:sqlite3.Cursor) -> list:
    operation = f"SELECT name FROM sqlite_master WHERE type=\"table\""
    cursor.execute(operation)
    
    info = cursor.fetchall()
    tables = [table[0] for table in info]
    
    return tables


def get_table_column_info(cursor:sqlite3.Cursor, table_name:str) -> list[tuple]:
    if table_name not in get_tables(cursor=cursor):
        raise ValueError(f"Table name \"{table_name}\" not in table.")

    operation = f"PRAGMA table_info({table_name})"
    cursor.execute(operation)

    column_info = cursor.fetchall()
    return column_info # [(cid, name, type, notnull, dflt_value, pk)]


def get_table_column_names(cursor:sqlite3.Cursor, table_name:str) -> list:
    column_info = get_table_column_info(cursor=cursor, table_name=table_name)
    columns = [name for (cid, name, type, notnull, dflt_value, pk) in column_info]
    
    return columns


def get_table_column_nullabilities(cursor:sqlite3.Cursor, table_name:str) -> list:
    column_info = get_table_column_info(cursor=cursor, table_name=table_name)
    columns = [notnull == 0 for (cid, name, type, notnull, dflt_value, pk) in column_info]
    
    return columns


def add_row(cursor:sqlite3.Cursor, table_name:str, data:dict, error_checking="STRICT") -> bool:
    nullabilities = get_table_column_nullabilities(cursor=cursor, table_name=table_name)
    columns = get_table_column_names(cursor=cursor, table_name=table_name)
    
    for column in data.keys():
        if column not in columns:
            raise ValueError(f"\"{column}\" not in table columns: {columns}.")

    error_checking = error_checking.upper()
    if error_checking == "STRICT":
        if sorted(data.keys()) != sorted(columns):
            raise ValueError(f"Columns {[key for key in data.keys()]} does not match table colums {columns}.")
        values = [data[column] for column in columns]
    elif error_checking == "RELAXED":
        values = []
        for column, nullable in zip(columns, nullabilities):
            value = data.get(column, None)
            if not nullable and value is None:
                raise ValueError(f"Column \"{column}\" does not accept NULL values")
            values.append(value)
    
    columns = ','.join(columns)
    placeholders = ",".join("?" for value in values)
    
    operation = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(operation, values)

    
def get_table_data(cursor:sqlite3.Cursor, table_name:str):
    operation = f"SELECT * FROM {table_name}"
    cursor.execute(operation)
    
    data = cursor.fetchall()
    columns = get_table_column_names(cursor=cursor, table_name=table_name)
    
    df = pd.DataFrame(data, columns=columns)
    
    return df
            

def main():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    
    create_table(cursor=cursor, table_name="tools", columns=["toolname", "arguments", "date", "user", "execution_time"])
    
    data = {
        "toolname": "mytool",
        "arguments": "1 2 3",
        "date": "05/28/2024",
        "user": "me",
        "execution_time": "10:52:11",
    }
    for x in range(1):
        add_row(cursor=cursor, table_name="tools", data=data, error_checking="RELAXED")
    
    df = get_table_data(cursor=cursor, table_name="tools")
    print(df)
    
    connection.commit()
    connection.close()
    

if __name__ == "__main__":
    main()