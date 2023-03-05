import pyodbc, pymongo
from subprocess import Popen, PIPE
from asyncio import create_task, gather, run
from re import match

from queries import SQLDbStructureQueries, SQLDataQueries, SQLViewQueries, SQLIndexQueries, mongodbAvailableQueryElems
from connectionData import drivers, connStrs
from exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError

# SQL

async def get_connection(dbType: str, dbPath: str = None, additionalParams: dict = None):
    if dbType not in connStrs:
        raise UnsupportedDBTypeError(f"Unsupported or unexisting database type '{dbType}'.")
    else:
        connectionStr = connStrs[dbType]
        if dbType in ("mysql", "postgresql"):
            try:
                connectionStr = connectionStr.format(**additionalParams)
            except KeyError:
                raise ArgumentError(f"Missing required arguments for establishing '{dbType}' connection.")
        elif dbType == "sqlite3":
                connectionStr = connectionStr.format(dbPath)
                process = Popen(["cat", dbPath], stdout= PIPE, stderr= PIPE)
                if process.communicate()[1]:
                    raise ArgumentError(f"Database path '{dbPath}' not found; check for any misspellings.")
    
    connectionStr = f"DRIVER={{{drivers[dbType]}}};" + connectionStr
    
    try:
        return pyodbc.connect(connectionStr, readonly= True, autocommit= False)
    except (pyodbc.OperationalError, pyodbc.Error):
        raise ConnectionError(
            "Failed to establish connection with {host}:{port} (database `{dbName}`).\nEnsure: \n - server is running and accepting TCP/IP connections at that address. \n - the correct database type has been specified. \n - other requiered arguments (username, password, ...) are not incorrect.".format(**additionalParams)
            )
    
async def get_cursor(connObject):
    try:
        return connObject.cursor()
    except pyodbc.Error:
        raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")

async def get_cursor_description(cursor):
    try:
        return [description[0] for description in cursor.description]
    except pyodbc.Error:
        raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")

async def read_tables(dbType: str, dbPath: str = None, additionalParams: dict = None, tablesLimitOffset: 'dict[str, tuple[int]]' = None) -> dict:
    try:
        with await get_connection(dbType, dbPath, additionalParams) as connection:
            cursor = await get_cursor(connection)
            tasks = [
                create_task(build_table_data(dbType, tableName, cursor, additionalParams, LimitOffset))
                for tableName, LimitOffset in tablesLimitOffset.items()
                ]
            data = await gather(*tasks)
            cursor.close()
            return data
    
    except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
        raise

async def read_views(dbType: str, dbPath: str = None, additionalParams: dict = None, viewsName: list = None):
    # Investigar creación de views en Mongo
    try:
        with await get_connection(dbType, dbPath, additionalParams) as connection:
            cursor = await get_cursor(connection)
            if dbType != "mongodb":
                tasks = [
                    create_task(build_SQL_view_data(dbType, viewName, cursor, additionalParams))
                    for viewName in viewsName
                    ]
            else:
                pass
                '''tasks = [
                    create_task(build_mongo_view_data(dbType, viewName, cursor, additionalParams))
                    for viewName in viewsName
                    ]'''
            data = await gather(*tasks)
            cursor.close()
            return data
    
    except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
        raise

async def read_index(dbType: str, dbPath: str = None, additionalParams: dict = None, indexesTables: 'dict[str, str]' = None):
    try:
        with await get_connection(dbType, dbPath, additionalParams) as connection:
            cursor = await get_cursor(connection)
            if dbType != "mongodb":
                tasks = [
                    create_task(build_SQL_index_data(dbType, indexName, tableName, cursor, additionalParams))
                    for indexName, tableName in indexesTables.items()
                    ]
            else:
                pass
                '''tasks = [
                    create_task(build_mongo_index_data(dbType, indexName, cursor, additionalParams))
                    for viewName in viewsName
                    ]'''
            data = await gather(*tasks)
            cursor.close()
            return data
    
    except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
        raise

async def get_postgresql_structure(additionalParams: dict, tableName: str) -> str:
    # Escribir guia de uso para POSTGRESQL (set en modo TRUST)
    process = Popen(f"pg_dump -U {additionalParams['user']} -t 'public.{tableName}' --schema-only {additionalParams['dbName']}", stdout= PIPE, stderr= PIPE, shell= True)
    if process.communicate()[1]:
        raise ConnectionError(
            "Failed to retrieve PostgreSQL database's schema, check for any incorrect arguments or enable TRUST authentication in 'pg_hba.conf'."
            )
    return str(process.communicate()[0])

async def run_SQL_query_and_get_result(query: str, cursor, *params) -> list:
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
    
    except (pyodbc.ProgrammingError, pyodbc.Error) as e:
        if isinstance(e, pyodbc.ProgrammingError):
            raise ExecutionError("Failed to execute query; check for any missing/incorrect arguments (table name, parameters, ...).")
        else:
            raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")
    
    return cursor.fetchall()

async def build_table_data(dbType: str, tableName: str, cursor, additionalParams: dict = None, limitOffset: 'tuple[int]' = None) -> 'dict[str, tuple[list]]':
    try:
        if dbType != "postgresql":
            structQuery = SQLDbStructureQueries[dbType].format(tableName)
            tableSQL = await run_SQL_query_and_get_result(structQuery, cursor)
        else:
            tableSQL = await get_postgresql_structure(additionalParams, tableName)
        
        dataQuery = SQLDataQueries[dbType].format(tableName)
            
        if limitOffset:
            dataQuery += " LIMIT ? OFFSET ?"
            if limitOffset[1] is None:
                limitOffset = (limitOffset[0], 0)
                tableData = await run_SQL_query_and_get_result(dataQuery, cursor, limitOffset[0], limitOffset[1])
            else:
                tableData = await run_SQL_query_and_get_result(dataQuery, cursor)
    
        cols = tuple(await get_cursor_description(cursor))
        tableData.insert(0, cols)
    
    except (ExecutionError, ConnectionError) as e:
        if isinstance(e, ConnectionError):
            e.args = (e.args[0].format(**additionalParams),)
        raise

    return {
        tableName: (tableData, tableSQL)
    }

async def build_SQL_view_data(dbType: str, viewName: str, cursor, additionalParams: dict = None) -> 'dict[str, list]':
    try:
        viewQuery = SQLViewQueries[dbType].format(viewName)
        viewData = await run_SQL_query_and_get_result(viewQuery, cursor)
    
    except (ExecutionError, ConnectionError) as e:
        if isinstance(e, ConnectionError):
            e.args = (e.args[0].format(**additionalParams),)
        raise

    return {
        viewName: viewData    
        }

async def build_SQL_index_data(dbType: str, indexName: str, tableName: str, cursor, additionalParams: dict = None) -> 'dict[str, list]':
    try:
        if dbType != "mysql":
            indexData = SQLIndexQueries[dbType].format(indexName)
        else:
            indexQuery = SQLIndexQueries[dbType].format(tableName)
            indexData = await run_SQL_query_and_get_result(indexQuery, cursor)
            indexData = match(rf"KEY: `{indexName}` (`.+`)", indexData)
    
    except (ExecutionError, ConnectionError) as e:
        if isinstance(e, ConnectionError):
            e.args = (e.args[0].format(**additionalParams),)
        raise

    return {
        {"name": indexName, "fromTable": tableName}: indexData    
        }

# NoSQL

async def get_mongo_client(additionalParams: dict = None) -> pymongo.MongoClient:
    return pymongo.MongoClient(connStrs["mongodb"].format(**additionalParams))

async def read_collections(additionalParams: dict = None, collectionLimitSkip: 'dict[str, tuple[int]]' = None) -> dict:
        with await get_mongo_client(additionalParams) as client:
            tasks = [
                create_task(build_collection_data(collectionName, client, additionalParams, LimitSkip))
                for collectionName, LimitSkip in collectionLimitSkip.items()
                ]
            data = await gather(*tasks)
            return data

async def run_mongo_query_and_get_result(queryElems: list, collectionObject, *params):
    queryObj = f"collectionObject" + "".join(mongodbAvailableQueryElems[elem] for elem in queryElems).format(*params)
    return eval(queryObj)

async def build_collection_data(collectionName: str, client: pymongo.MongoClient, additionalParams: dict = None, limitSkip: 'tuple[int]' = None) -> 'dict[str, list]':
    collection = client[additionalParams['dbName']][collectionName]

    if limitSkip:
        if limitSkip[1] is None:
            limitSkip = (limitSkip[0], 0)
        collectionData = await run_mongo_query_and_get_result(["find", "limit", "skip"], collection, limitSkip[0], limitSkip[1])
    else:
        collectionData = await run_mongo_query_and_get_result(["find"], collection)
    
    return {
        collectionName: list(collectionData)
    }

if __name__ == "__main__":
    run(read_views("postgresql", "/home/brunengo/Escritorio/Proshecto/northwind.db", additionalParams= {"user": "dbdummy", "password": "sql", "host": "localhost", "dbName": "dvdrental", "port": 5433}, viewsName= ["test"]))