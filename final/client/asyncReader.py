import pyodbc, pymongo
from subprocess import Popen, PIPE
from asyncio import Future, create_task, gather, run
from re import findall

from queries import SQLDbStructureQueries, SQLObjectsNameQueries, SQLDataQueries, SQLViewQueries, SQLIndexQueries, mongodbAvailableQueryElems
from connectionData import drivers, connStrs
from exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError

# SQL

class SQLDatabaseReader():

    async def get_sql_connection(self, dbType: str, dbPath: str = None, connectionParams: dict = None) -> pyodbc.Connection:        
        if dbType not in connStrs:
            raise UnsupportedDBTypeError(f"Unsupported or unexisting database type '{dbType}'.")
        
        else:
            connectionStr = connStrs[dbType]
            
            if dbType in ("mysql", "postgresql"):
                try:
                    connectionStr = connectionStr.format(**connectionParams)
                except KeyError:
                    raise ArgumentError(f"Missing required arguments for establishing '{dbType}' connection.")
            
            elif dbType == "sqlite3":
                connectionStr = connectionStr.format(dbPath)
                process = Popen(["cat", dbPath], stdout= PIPE, stderr= PIPE)
                if process.communicate()[1]:
                    raise ConnectionError(
                        f"Database path '{dbPath}' not found or read/write access not granted for current user; check for any misspellings and ensure you have read/write permissions."
                        )
        
        connectionStr = f"DRIVER={{{drivers[dbType]}}};" + connectionStr
        
        try:
            return pyodbc.connect(connectionStr, readonly= True, autocommit= False)
        
        except (pyodbc.OperationalError, pyodbc.Error):
            raise ConnectionError(
                "Failed to establish connection with {host}:{port} (database `{dbName}`).\nEnsure: \n - server is running and accepting TCP/IP connections at that address. \n - the correct database type has been specified. \n - other requiered arguments (username, password, ...) are not incorrect.".format(**connectionParams)
                )
        
    async def get_sql_cursor(self, connObject) -> pyodbc.Cursor:
        try:
            return connObject.cursor()
        except pyodbc.Error:
            raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")

    async def get_cursor_description(self, cursor) -> list:
        try:
            return [description[0] for description in cursor.description]
        except pyodbc.Error:
            raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")

    async def sql_connect_and_get_objects_name(self, dbType: str, dbPath: str = None, connectionParams: dict = None) -> 'dict[str, list]':
        try:
            if dbType == "sqlite3":
                connectionParams = {"dbName": dbPath.split("/")[-1][:-3:]}
                with await self.get_sql_connection(dbType, dbPath, connectionParams) as connection:
                    query = SQLObjectsNameQueries[dbType].format(connectionParams['dbName'])
                    cursor = await self.get_sql_cursor(connection)
                    data = await self.run_sql_query_and_get_result(query, cursor)
            
            else:
                originalDbName = connectionParams["dbName"]
                data = {}
                
                if dbType == 'mysql':
                    connectionParams.update({"dbName": "INFORMATION_SCHEMA"})

                with await self.get_sql_connection(dbType, dbPath, connectionParams) as connection:
                    query = SQLObjectsNameQueries[dbType]
                    cursor = await self.get_sql_cursor(connection)
                    for objectType, specificQuery in query.items():
                        specificQuery = specificQuery.format(originalDbName)
                        data.update({objectType: await self.run_sql_query_and_get_result(specificQuery, cursor)})
                    
        except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
            raise e
    
        return data            
    
    async def sql_connect_and_read(self, dbType: str, dbPath: str = None, connectionParams: dict = None, readParams: 'tuple[str, dict | list]' = None) -> 'Future[list]':
        if dbType == "sqlite3":
            connectionParams = {"dbName": dbPath.split("/")[-1][:-3:]}
        
        taskFunctions = {
            "table": self.create_sql_table_tasks,
            "view": self.create_sql_view_tasks,
            "index": self.create_sql_index_tasks
        }
        
        try:
            with await self.get_sql_connection(dbType, dbPath, connectionParams) as connection:
                cursor = await self.get_sql_cursor(connection)
                tasks = await taskFunctions[readParams[0]](dbType, cursor, connectionParams, readParams[1])
                data = await gather(*tasks)
                cursor.close()
            
            return data
        
        except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
            raise e
            
    async def create_sql_table_tasks(self, dbType: str, cursor, connectionParams: dict = None, tablesLimitOffset: 'dict[str, tuple[int]]' = None) -> list:
        tasks = [
            create_task(self.build_sql_table_data(dbType, tableName, cursor, connectionParams, LimitOffset))
            for tableName, LimitOffset in tablesLimitOffset.items()
            ]
        return tasks
        
    async def create_sql_view_tasks(self, dbType: str, cursor, connectionParams: dict = None, viewNames: list = None) -> list:
        tasks = [
            create_task(self.build_sql_view_data(dbType, viewName, cursor, connectionParams))
            for viewName in viewNames
            ]
        return tasks

    async def create_sql_index_tasks(self, dbType: str, cursor, connectionParams: dict = None, indexesTables: 'dict[str, str]' = None) -> list:
        tasks = [
            create_task(self.build_sql_index_data(dbType, indexName, tableName, cursor, connectionParams))
            for indexName, tableName in indexesTables.items()
            ]
        return tasks

    async def get_postgresql_structure(self, connectionParams: dict, tableName: str) -> str:
        # Escribir guia de uso para POSTGRESQL (set en modo TRUST)
        process = Popen(f"pg_dump -U {connectionParams['user']} -t 'public.{tableName}' --schema-only {connectionParams['dbName']}", stdout= PIPE, stderr= PIPE, shell= True)
        if process.communicate()[1]:
            raise ConnectionError(
                "Failed to retrieve PostgreSQL database's schema, check for any incorrect arguments or enable TRUST authentication in 'pg_hba.conf'."
                )
        return str(process.communicate()[0])

    async def run_sql_query_and_get_result(self, query: str, cursor, *params) -> list:    
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
        
        except (pyodbc.ProgrammingError, pyodbc.Error) as e:
            if isinstance(e, pyodbc.ProgrammingError) or e.args[0] == 'HY000':
                raise ExecutionError("Failed to execute query; check for any missing/incorrect arguments (table/view/index name, parameters, ...).")
            else:
                raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")
        
        return cursor.fetchall()

    async def build_sql_table_data(self, dbType: str, tableName: str, cursor, connectionParams: dict = None, limitOffset: 'tuple[int, int]' = None) -> 'dict[str, tuple[list]]':
        self.check_for_sanitized_sql_input(dbType, tableName)
        
        try:
            if dbType != "postgresql":
                structQuery = SQLDbStructureQueries[dbType].format(tableName)
                tableSQL = await self.run_sql_query_and_get_result(structQuery, cursor)
            else:
                tableSQL = await self.get_postgresql_structure(connectionParams, tableName)
            
            dataQuery = SQLDataQueries[dbType].format(tableName)
            
            if limitOffset:
                dataQuery += " LIMIT ? OFFSET ?"
                if limitOffset[1] is None:
                    limitOffset = (limitOffset[0], 0)  
                tableData = await self.run_sql_query_and_get_result(dataQuery, cursor, limitOffset[0], limitOffset[1])  
            else:
                tableData = await self.run_sql_query_and_get_result(dataQuery, cursor)
        
            cols = tuple(await self.get_cursor_description(cursor))
            tableData.insert(0, cols)
        
        except (ExecutionError, ConnectionError) as e:
            if isinstance(e, ConnectionError):
                e.args = (e.args[0].format(**connectionParams),)
            raise e

        return {
            tableName: (tableData, tableSQL)
        }

    async def build_sql_view_data(self, dbType: str, viewName: str, cursor, connectionParams: dict = None) -> 'dict[str, list]':
        self.check_for_sanitized_sql_input(dbType, viewName)
        
        try:
            viewQuery = SQLViewQueries[dbType].format(viewName)
            viewData = await self.run_sql_query_and_get_result(viewQuery, cursor)
        
        except (ExecutionError, ConnectionError) as e:
            if isinstance(e, ConnectionError):
                e.args = (e.args[0].format(**connectionParams),)
            raise e
    
        try:
            if viewData[0][0] is None:
                raise ExecutionError(f"Unexisting view `{viewName}` in '{connectionParams['dbName']}' database. Check for any misspellings.")
        
        except IndexError:
            raise ExecutionError(f"Unexisting view `{viewName}` in '{connectionParams['dbName']}' database. Check for any misspellings.")
        
        return {
            viewName: viewData[0]  
            }

    async def build_sql_index_data(self, dbType: str, indexName: str, tableName: str, cursor, connectionParams: dict = None) -> 'dict[str, list]':
        for name in (indexName, tableName):
            self.check_for_sanitized_sql_input(dbType, name) 
        
        try:
            if dbType != "mysql":
                indexQuery = SQLIndexQueries[dbType].format(indexName)
                indexData = await self.run_sql_query_and_get_result(indexQuery, cursor)
            else:
                indexQuery = SQLIndexQueries[dbType].format(tableName)
                indexData = await self.run_sql_query_and_get_result(indexQuery, cursor)
                indexData = findall(r"KEY `{0}` \(`.*`\)".format(indexName), indexData[0][1])
        
        except (ExecutionError, ConnectionError) as e:
            if isinstance(e, ConnectionError):
                e.args = (e.args[0].format(**connectionParams),)
            raise
        
        return {
            f"{indexName}-{tableName}": indexData[0]
            }

    def check_for_sanitized_sql_input(self, dbType: str, queryInput: str) -> None:
        forbbidenChars = {
            "sqlite3": ["[", "]", "'"],
            "mysql": ["`"],
            "postgresql": ["\"", "'"]
        }
        
        for char in forbbidenChars[dbType]:
            splitInput = queryInput.split(char)

            if len(splitInput) > 1:
                raise ArgumentError(
                    "Potencially malicious query arguments. Query input containing quotes, backticks or squared brackets is always rejected depending on database type, as to avoid SQL injections."
                )

# NoSQL
# Escribir permisos necesarios para MongoDB

class MongoDatabaseReader():

    async def get_mongo_client(self, connectionParams: dict) -> pymongo.MongoClient:
        try:
            client = pymongo.MongoClient(connStrs["mongodb"].format(**connectionParams))
        
        except (KeyError, pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.OperationFailure) as e:
            if isinstance(e, KeyError):
                raise ArgumentError(f"Missing required arguments for establishing 'mongodb' connection.")
            elif isinstance(e, pymongo.errors.ServerSelectionTimeoutError):
                raise ConnectionError(
                    "Failed to establish connection with {host}:{port} (database `{dbName}`).\nEnsure: \n - server is running and accepting TCP/IP connections at that address. \n - the correct database type has been specified. \n - other requiered arguments are not incorrect.".format(**connectionParams)
                )
            else:
                raise ConnectionError("Could not authenticate: wrong credentials. Check for any misspellings in your username or password.")
        
        return client

    async def mongo_connect_and_read(self, connectionParams: dict, readParams: 'tuple[str, dict | list]') -> 'Future[list]':
        taskFunctions = {
            "collection": self.create_mongo_collection_tasks,
            "view": self.create_mongo_view_tasks,
            "index": self.create_mongo_index_tasks
        }
        
        with await self.get_mongo_client(connectionParams) as client:
            tasks = await taskFunctions[readParams[0]](connectionParams, client, readParams[1])
            data = await gather(*tasks)
            
        return data

    async def create_mongo_collection_tasks(self, connectionParams: dict, client: pymongo.MongoClient, collectionLimitSkip: 'dict[str, tuple[int]]' = None) -> list:
        tasks = [
            create_task(self.build_mongo_collection_data(collectionName, client, connectionParams, limitSkip))
            for collectionName, limitSkip in collectionLimitSkip.items()
            ]
        return tasks

    async def create_mongo_view_tasks(self, connectionParams: dict, client: pymongo.MongoClient, viewNames: list = None) -> list:
        tasks = [
            create_task(self.build_mongo_view_data(viewName, client, connectionParams))
            for viewName in viewNames
            ]
        return tasks

    async def create_mongo_index_tasks(self, connectionParams: dict, client: pymongo.MongoClient, indexesCollection: 'dict[str, str]' = None) -> list:
        tasks = [
            create_task(self.build_mongo_index_data(indexName, collectionName, client, connectionParams))
            for indexName, collectionName in indexesCollection.items()
            ]
        return tasks

    async def run_mongo_query_and_get_result(self, queryElems: list, accesibleObject, *params):
        try:
            queryObj = "accesibleObject" + "".join(mongodbAvailableQueryElems[elem] for elem in queryElems).format(*params)
            return eval(queryObj)
        
        except (pymongo.errors.ServerSelectionTimeoutError,) as e:
            raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) could not be established or has been lost.\nEnsure: \n - server is running and accepting TCP/IP connections at that address. \n - the correct database type has been specified. \n - other requiered arguments (username, password, ...) are not incorrect.")

    async def build_mongo_collection_data(self, collectionName: str, client: pymongo.MongoClient, connectionParams: dict = None, limitSkip: 'tuple[int]' = None) -> 'dict[str, list]':
        list_of_collections = client[connectionParams['dbName']].list_collection_names()

        if not (collectionName in list_of_collections):
            raise ExecutionError(f"Unexisting collection `{collectionName}` in '{connectionParams['dbName']}' database. Check for any misspellings.")
        
        try:
            collection = client[connectionParams['dbName']][collectionName]
            
            if limitSkip:
                if limitSkip[1] is None:
                    limitSkip = (limitSkip[0], 0)
                collectionData = await self.run_mongo_query_and_get_result(["find", "limit", "skip"], collection, "", limitSkip[0], limitSkip[1])
            
            else:
                collectionData = await self.run_mongo_query_and_get_result(["find"], collection, "")
        
        except ConnectionError as e:
            e.args = (e.args[0].format(**connectionParams),)
            raise e
        
        return {
            collectionName: list(collectionData)
        }

    async def build_mongo_view_data(self, viewName: str, client: pymongo.MongoClient, connectionParams: dict = None) -> 'dict[str, dict]':
        try:
            views = client[connectionParams['dbName']]['system.views']
            viewData = await self.run_mongo_query_and_get_result(["find"], views, {"_id": f"{connectionParams['dbName']}.{viewName}"})
        
        except ConnectionError as e:
            e.args = (e.args[0].format(**connectionParams),)
            raise e

        if not list(viewData):
            raise ExecutionError(f"Unexisting view `{viewName}` in '{connectionParams['dbName']}' database. Check for any misspellings.")
        
        return {
            viewName: list(viewData)[0]
        }

    async def build_mongo_index_data(self, indexName: str, collectionName: str, client: pymongo.MongoClient, connectionParams: dict = None) -> 'dict[str, dict]':
        try:
            collection = client[connectionParams['dbName']][collectionName]
            indexData = await self.run_mongo_query_and_get_result(["getIndexes"], collection)
        
        except ConnectionError as e:
            e.args = (e.args[0].format(**connectionParams),)
            raise e
        
        if not list(indexData):
            raise ExecutionError(f"Unexisting index `{indexName}` in collection `{collectionName}` from '{connectionParams['dbName']}' database. Check for any misspellings.")
        
        return {
            indexName: indexData[indexName]
        }

if __name__ == "__main__":
    mongoReader = MongoDatabaseReader()
    sqlReader = SQLDatabaseReader()
    print(run(sqlReader.sql_connect_and_get_objects_name("sqlite3", "/home/brunengo/Escritorio/Proshecto/northwind.db", {})))
    
    '''print(run(sqlReader.sql_connect_and_read(
        dbType= "sqlite3", 
        dbPath= "/home/brunengo/Escritorio/Proshecto/northwind.db", 
        connectionParams= {"user": "dbdummy", "password": "sql", "host": "localhost", "dbName": "dvdrental", "port": 5433}, 
        readParams= ("table", {"Customers": None})
        )))'''
    
    '''print(run(mongo_connect_and_read(
        connectionParams= {"user": "dbdummy", "password": "mongo", "host": "localhost", "dbName": "admin", "port": 27018}, 
        readParams= ("index", {"nonexistant": "books"})
        )))'''