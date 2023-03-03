import pyodbc
from subprocess import Popen, PIPE
from asyncio import create_task, gather, run

from queries import dbStructureQueries, dataQueries
from connectionData import drivers, connStrs

async def get_connection(dbType: str, dbPath: str = None, additionalData: dict = None):
    # print(pyodbc.drivers())
    
    if dbType not in drivers:
        # raise Exception -- "No drivers found for 'dbType' type databases."
        return
    else:
        connectionStr = connStrs[dbType]
        if dbType == "sqlite3":
            connectionStr = connectionStr.format(dbPath)
        elif dbType in ("mysql", "postgresql"):
            connectionStr = connectionStr.format(**additionalData)
    
    connectionStr = f"DRIVER={{{drivers[dbType]}}};" + connectionStr
    print(connectionStr)
    return pyodbc.connect(connectionStr)
    
async def get_cursor(connObject):
    return connObject.cursor()

async def read_tables(dbType: str, dbPath: str, additionalData: dict = None, tablesLimitOffset: 'dict[str, tuple[int]]' = None) -> dict:
        with await get_connection(dbType, dbPath, additionalData) as connection:
            cursor = await get_cursor(connection)
            tasks = [
                create_task(build_table_data(dbType, tableName, cursor, additionalData, LimitOffset))
                for tableName, LimitOffset in tablesLimitOffset.items()
                ]
            data = await gather(*tasks)
            print(data)
            cursor.close()

async def run_query_and_get_result(query: str, cursor, *params: int) -> list:
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    
    return cursor.fetchall()

async def build_table_data(dbType: str, tableName: str, cursor, additionalData: dict = None, LimitOffset: 'tuple[int]' = None) -> 'dict[str, tuple[list]]':
    '''
    Controlar con regex el tipo de base de datos y el nombre de la tabla
    para evitar inyección SQL
    '''
    
    if dbType != "postgresql":
        structQuery = dbStructureQueries[dbType].format(tableName)
        tableSQL = await run_query_and_get_result(structQuery, cursor)
    
    else:
        process = Popen(f"pg_dump -U {additionalData['user']} -t 'public.{tableName}' --schema-only {additionalData['dbName']}", stdout= PIPE, shell= True) # Escribir guia de uso para POSTGRESQL (set en modo TRUST)
        tableSQL = str(process.communicate()[0])
        
    dataQuery = dataQueries[dbType].format(tableName)
        
    if LimitOffset:
        dataQuery += " LIMIT ? OFFSET ?"
        if LimitOffset[1] is None:
            LimitOffset = (LimitOffset[0], 0)
        print(dataQuery, LimitOffset)
        tableData = await run_query_and_get_result(dataQuery, cursor, LimitOffset[0], LimitOffset[1])
    else:
        tableData = await run_query_and_get_result(dataQuery, cursor)
    
    cols = tuple([description[0] for description in cursor.description])
    tableData.insert(0, cols)
    
    return {
        tableName: (tableData, tableSQL)
    }

if __name__ == "__main__":
    run(read_tables("postgresql", "/home/brunengo/Escritorio/northwind.db", additionalData={"user": "dbdummy", "password": "sql", "host": "localhost", "dbName": "dvdrental", "port": 5433}, tablesLimitOffset= {"actor": (20, None)}))