import sqlite3
from asyncio import create_task, gather, run

async def read_tables(dbPath: str, dbType: str, tablesAndRowsLimit: 'dict[str, int]') -> dict:
    '''
    Crear un switch para instanciar la conexión adecuada según el tipo
    de base de datos.
    '''
    with sqlite3.connect(dbPath, 2) as connection: 
        cursor = connection.cursor()
        tasks = [
            create_task(build_table_data(name, rowLimit, dbType, cursor))
            for name, rowLimit in tablesAndRowsLimit.items()
        ]
        data = await gather(*tasks)
        print(data)
        cursor.close()

async def build_table_data(tableName: str, rowLimit: int, dbType: str, cursor: sqlite3.Cursor) -> dict:
    '''
    Try/except o verificación de entrada para el formato de bd especificado.
    '''
    cursor.execute(
        f"SELECT sql FROM {dbType + '_master'} WHERE type='table' AND name=?", 
        (tableName,)
    )
    tableSchema = cursor.fetchall()
    print(tableSchema)
    
    dataQuery = f"SELECT * FROM {tableName} {'LIMIT ?' if rowLimit is not None else ''}"
    if rowLimit is not None:
        cursor.execute(dataQuery, (rowLimit,))
    else:
        cursor.execute(dataQuery)
    tableData = cursor.fetchall()
    print(tableData)
    
    tableDict = {
        tableName: ["prueba"]
    }

if __name__ == "__main__":
    run(read_tables("/home/brunengo/Escritorio/northwind.db", "sqlite", {"Employees": 10, "Customers": None}))