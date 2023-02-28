import sqlite3, mysql.connector
from asyncio import create_task, gather, run

async def get_connection(dbType: str, dbPath: str = None, user: str = None, password: str = None, host: str = "localhost", dbName: str = None):    
    if dbType == 'sqlite':
        return sqlite3.connect(dbPath, 2)
    elif dbType == 'mysql':
        return mysql.connector.connect(
            user= user,
            password= password,
            host= host,
            database= dbName
        )

async def read_tables(dbPath: str, dbType: str, additionalData: dict, tablesLimitOffset: 'dict[str, tuple[int]]') -> dict:
    '''
    Crear un switch para instanciar la conexión adecuada según el tipo
    de base de datos.
    '''
    with await get_connection(
        dbType, dbPath, additionalData["user"], additionalData["password"], additionalData["host"], additionalData["dbName"]
        ) as connection: 
        cursor = connection.cursor()
        tasks = [
            create_task(build_table_data(name, dbType, rowLimitOffset, cursor))
            for name, rowLimitOffset in tablesLimitOffset.items()
            ]
        data = await gather(*tasks)
        cursor.close()

'''async def execute_query(dbType: str, query: str, rowLimitOffset: 'tuple[int]', cursor) -> list:
    sqlDict = {
        "sqlite": cursor.execute,
        "":""
    }
    '''
    Diccionario con el comando de ejecución adecuado, indexado por el
    tipo de base de datos (SQLite, MySQL, ...)
    '''
    pass'''

async def build_table_data(tableName: str, dbType: str, rowLimitOffset: 'tuple[int]', cursor) -> 'dict[str, list[tuple]]':
    '''
    Controlar con regex el tipo de base de datos y el nombre de la tabla
    para evitar inyección SQL
    '''
    
    SQLQuery = f"SELECT sql FROM {dbType}_master WHERE type='table' and name=?"

    cursor.execute(SQLQuery, (tableName,))
    tableSQL = cursor.fetchall()
        
    dataQuery = f"SELECT * FROM [{tableName}]"
        
    if rowLimitOffset != (None, None):
        dataQuery += " LIMIT ? OFFSET ?"      
        if rowLimitOffset[1] is None:
            rowLimitOffset = (rowLimitOffset[0], 0)
        cursor.execute(dataQuery, (rowLimitOffset))
    else:
        cursor.execute(dataQuery)
    
    cols = tuple([description[0] for description in cursor.description])
    tableData = cursor.fetchall()
    
    tableData.insert(0, cols)
    
    return {
        tableName: (tableData, tableSQL)
    }

if __name__ == "__main__":
    run(read_tables("/home/brunengo/Escritorio/northwind.db", "sqlite", {"Order Details": (20, None)}))