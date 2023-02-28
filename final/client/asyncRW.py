import sqlite3, re
from asyncio import create_task, gather, run

async def read_tables(dbPath: str, dbType: str, tablesLimitOffset: 'dict[str, tuple[int]]') -> dict:
    '''
    Crear un switch para instanciar la conexión adecuada según el tipo
    de base de datos.
    '''
    with sqlite3.connect(dbPath, 2) as connection: 
        cursor = connection.cursor()
        tasks = [
            create_task(build_table_data(name, dbType, rowLimitOffset, cursor))
            for name, rowLimitOffset in tablesLimitOffset.items()
            ]
        data = await gather(*tasks)
        cursor.close()

async def build_table_data(tableName: str, dbType: str, rowLimitOffset: 'tuple[int]', cursor: sqlite3.Cursor) -> 'dict[str, list[tuple]]':
    '''
    Controlar con regex el tipo de base de datos y el nombre de la tabla
    para evitar inyección SQL
    '''
    
    SQLQuery = f"SELECT sql FROM {dbType}_master WHERE type='table' and name=?"

    cursor.execute(SQLQuery, (tableName,))
    tableSQL = cursor.fetchall()
    constraints = re.findall(r'(PRIMARY KEY \(\`*\`\)$)||(FOREIGN KEY \(\`*\`\)$)', str(tableSQL))
    
    dataQuery = f"SELECT * FROM {tableName}"
        
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
        tableName: tableData
    }

if __name__ == "__main__":
    run(read_tables("/home/brunengo/Escritorio/northwind.db", "sqlite", {"Employees": (20, None)}))