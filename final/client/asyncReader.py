import pyodbc, pymongo
from subprocess import Popen, PIPE
from asyncio import create_task, gather, run

from queries import SQLDbStructureQueries, SQLdataQueries, mongodbAvailableQueryElems
from connectionData import drivers, connStrs

# SQL

async def get_connection(dbType: str, dbPath: str = None, additionalData: dict = None):
    if dbType not in connStrs:
        # raise Exception -- "Database type 'dbType' is not currently supported."
        return
    else:
        connectionStr = connStrs[dbType]
        if dbType in ("mysql", "postgresql"):
            connectionStr = connectionStr.format(**additionalData)
        elif dbType == "sqlite3":
            connectionStr = connectionStr.format(dbPath)
    
    connectionStr = f"DRIVER={{{drivers[dbType]}}};" + connectionStr
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
            cursor.close()
            return data

async def get_postgresql_structure(additionalData: dict, tableName: str) -> str:
    # Escribir guia de uso para POSTGRESQL (set en modo TRUST)
    process = Popen(f"pg_dump -U {additionalData['user']} -t 'public.{tableName}' --schema-only {additionalData['dbName']}", stdout= PIPE, shell= True)
    return str(process.communicate()[0])

async def run_SQLquery_and_get_result(query: str, cursor, *params) -> list:
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    
    return cursor.fetchall()

async def build_table_data(dbType: str, tableName: str, cursor, additionalData: dict = None, limitOffset: 'tuple[int]' = None) -> 'dict[str, tuple[list]]':
   
    if dbType != "postgresql":
        structQuery = SQLDbStructureQueries[dbType].format(tableName)
        tableSQL = await run_SQLquery_and_get_result(structQuery, cursor)
    else:
        tableSQL = await get_postgresql_structure(additionalData, tableName)
        
    dataQuery = SQLdataQueries[dbType].format(tableName)
        
    if limitOffset:
        dataQuery += " LIMIT ? OFFSET ?"
        if limitOffset[1] is None:
            limitOffset = (limitOffset[0], 0)
        tableData = await run_SQLquery_and_get_result(dataQuery, cursor, limitOffset[0], limitOffset[1])
    else:
        tableData = await run_SQLquery_and_get_result(dataQuery, cursor)
    
    cols = tuple([description[0] for description in cursor.description])
    tableData.insert(0, cols)
    
    return {
        tableName: (tableData, tableSQL)
    }

# NoSQL

async def get_mongo_client(additionalData: dict = None) -> pymongo.MongoClient:
    return pymongo.MongoClient(connStrs["mongodb"].format(**additionalData))

async def read_collections(additionalData: dict = None, collectionLimitSkip: 'dict[str, tuple[int]]' = None) -> dict:
        with await get_mongo_client(additionalData) as client:
            tasks = [
                create_task(build_collection_data(collectionName, client, additionalData, LimitSkip))
                for collectionName, LimitSkip in collectionLimitSkip.items()
                ]
            data = await gather(*tasks)
            return data

async def run_mongoquery_and_get_result(queryElems: list, collectionObject, *params):
    queryObj = f"collectionObject" + "".join(mongodbAvailableQueryElems[elem] for elem in queryElems).format(*params)
    return eval(queryObj)

async def build_collection_data(collectionName: str, client: pymongo.MongoClient, additionalData: dict = None, limitSkip: 'tuple[int]' = None) -> 'dict[str, list]':
    collection = client[additionalData['dbName']][collectionName]

    if limitSkip:
        if limitSkip[1] is None:
            limitSkip = (limitSkip[0], 0)
        collectionData = await run_mongoquery_and_get_result(["find", "limit", "skip"], collection, limitSkip[0], limitSkip[1])
    else:
        collectionData = await run_mongoquery_and_get_result(["find"], collection)
    
    return {
        collectionName: list(collectionData)
    }

if __name__ == "__main__":
    run(read_tables("mysql", "/home/brunengo/Escritorio/Proshecto/northwind.db", additionalData= {"user": "DBDummy", "password": "sql", "host": "localhost", "dbName": "classicmodels", "port": 3306}, tablesLimitOffset= {"customers": (20, None)}))