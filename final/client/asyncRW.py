import sqlite3, mysql.connector, psycopg2
from subprocess import Popen, PIPE
from asyncio import create_task, gather, run
from queries import dbStructureQueries, dataQueries, limitOffsetAppend

async def get_connection(dbType: str, dbPath: str = None, additionalData: dict = None):
    if dbType == "sqlite":
        return sqlite3.connect(dbPath, 2)
    elif dbType == "mysql":
        return mysql.connector.connect(
            user= additionalData["user"],
            password= additionalData["password"],
            host= additionalData["host"],
            database= additionalData["dbName"],
            port= additionalData["port"]
        )
    elif dbType == "postgresql":
        return psycopg2.connect(
            user= additionalData["user"],
            password= additionalData["password"],
            host= additionalData["host"],
            database= additionalData["dbName"],
            port= additionalData["port"]
        )
    else: #raise Exception
        pass

async def get_cursor(dbType: str, connObject):
    if dbType != "mysql":
        return connObject.cursor()
    else:
        return connObject.cursor(buffered= True)

async def read_tables(dbType: str, dbPath: str, additionalData: dict = None, tablesLimitOffset: 'dict[str, tuple[int]]' = None) -> dict:
        with await get_connection(dbType, dbPath, additionalData) as connection:
            cursor = await get_cursor(dbType, connection)
            tasks = [
                create_task(build_table_data(dbType, tableName, cursor, additionalData, LimitOffset))
                for tableName, LimitOffset in tablesLimitOffset.items()
                ]
            data = await gather(*tasks)
            print(data)
            cursor.close()

async def get_cursor_data(cursor) -> list:
    return cursor.fetchall()

async def run_query(query: str, cursor, params: tuple = None) -> None:
    print(query)
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)

async def build_table_data(dbType: str, tableName: str, cursor, additionalData: dict = None, LimitOffset: 'tuple[int]' = None) -> 'dict[str, tuple[list]]':
    '''
    Controlar con regex el tipo de base de datos y el nombre de la tabla
    para evitar inyección SQL
    '''
    
    if dbType != "postgresql":
        structQuery = dbStructureQueries[dbType].format(tableName)
        await run_query(structQuery, cursor)
        tableSQL = await get_cursor_data(cursor)
    
    else:
        process = Popen(f"pg_dump -U {additionalData['user']} -t 'public.{tableName}' --schema-only {additionalData['dbName']}", stdout= PIPE, shell= True) # Escribir guia de uso para POSTGRESQL (set en modo TRUST)
        tableSQL = str(process.communicate()[0])
        
    dataQuery = dataQueries[dbType].format(tableName)
        
    if LimitOffset:
        dataQuery += limitOffsetAppend[dbType]
        if LimitOffset[1] is None:
            LimitOffset = (LimitOffset[0], 0)
        print(dataQuery, LimitOffset)
        await run_query(dataQuery, cursor, LimitOffset)
    else:
        await run_query(dataQuery, cursor)
    
    cols = tuple([description[0] for description in cursor.description])
    tableData = await get_cursor_data(cursor) 
    
    tableData.insert(0, cols)
    
    return {
        tableName: (tableData, tableSQL)
    }

if __name__ == "__main__":
    run(read_tables("mysql", "/home/brunengo/Escritorio/northwind.db", additionalData={"user": "DBDummy", "password": "sql", "host": "localhost", "dbName": "classicmodels", "port": 3306}, tablesLimitOffset= {"customers": (20, None)}))