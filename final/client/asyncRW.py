import sqlite3, mysql.connector
from asyncio import create_task, gather, run

async def get_connection(dbType: str, dbPath: str = None, aditionalData: dict = None):    
    if dbType == 'sqlite':
        return sqlite3.connect(dbPath, 2)
    elif dbType == 'mysql':
        return mysql.connector.connect(
            user= aditionalData["user"],
            password= aditionalData["password"],
            host= aditionalData["host"],
            database= aditionalData["dbName"]
        )
    else: #raise Exception
        pass

async def read_tables(dbPath: str, dbType: str, additionalData: dict = None, tablesLimitOffset: 'dict[str, tuple[int]]' = None) -> dict:
    '''
    Crear un switch para instanciar la conexión adecuada según el tipo
    de base de datos.
    '''
    with await get_connection(dbType, dbPath, additionalData) as connection: 
        cursor = connection.cursor(buffered= True)
        tasks = [
            create_task(build_table_data(name, dbType, cursor, rowLimitOffset))
            for name, rowLimitOffset in tablesLimitOffset.items()
            ]
        data = await gather(*tasks)
        print(data)
        cursor.close()

'''async def execute_query(dbType: str, query: str, rowLimitOffset: 'tuple[int]', cursor) -> list:
    sqlDict = {
        "sqlite": cursor.execute,
        "":""
    }
    Diccionario con el comando de ejecución adecuado, indexado por el
    tipo de base de datos (SQLite, MySQL, ...)
    pass'''

async def get_cursor_data(cursor) -> dict:
    return cursor.fetchall()

async def run_query(query: str, cursor, params: tuple = None) -> None:
    print(query)
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)

async def build_table_data(tableName: str, dbType: str, cursor, rowLimitOffset: 'tuple[int]' = None) -> 'dict[str, list[tuple]]':
    '''
    Controlar con regex el tipo de base de datos y el nombre de la tabla
    para evitar inyección SQL
    '''
    
    SQLQueries = {
        "sqlite": f"SELECT sql FROM {dbType}_master WHERE type='table' and name={tableName}",
        "mysql": f"SHOW CREATE TABLE {tableName}"
    } 

    await run_query(SQLQueries[dbType], cursor)
    tableSQL = await get_cursor_data(cursor)

    print(tableSQL)
        
    dataQuery = f"SELECT * FROM [{tableName}]"
        
    if rowLimitOffset:
        dataQuery += {"sqlite": " LIMIT ? OFFSET ?", "mysql": " LIMIT %s OFFSET %s"
        if rowLimitOffset[1] is None:
            rowLimitOffset = (rowLimitOffset[0], 0)
        print(dataQuery, rowLimitOffset)
        await run_query(dataQuery, cursor, rowLimitOffset)
    else:
        await run_query(dataQuery, cursor)
    
    cols = tuple([description[0] for description in cursor.description])
    tableData = await get_cursor_data(dbType, cursor)
    
    tableData.insert(0, cols)
    
    return {
        tableName: (tableData, tableSQL)
    }

if __name__ == "__main__":
    run(read_tables("/home/brunengo/Escritorio/northwind.db", "mysql", additionalData={"user": "DBDummy", "password": "sql", "host": "localhost", "dbName": "classicmodels"}, tablesLimitOffset= {"customers": (20, None)}))