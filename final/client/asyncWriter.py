import pyodbc
from subprocess import Popen, PIPE
from asyncio import Future, create_task, gather, run
from sys import path

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from baseAcceser import SQLDatabaseAcceser

from common.queries import SQLDbStructureQueries, SQLObjectsNameQueries, SQLDataQueries, SQLViewQueries, SQLIndexQueries, mongodbAvailableQueryElems
from common.connectionData import mainDbName
from common.exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError

# SQL

class SQLDatabaseWriter(SQLDatabaseAcceser):
    
    async def connect_and_create_database(self, dbType: str, dbPath: str = None, connectionParams: dict = None) -> None:
        if dbType == "sqlite3": # Evitar sobreescribir el archivo si ya existe
            process = Popen(f"cd {dbPath}; touch {connectionParams['dbName']}.db", stdout= PIPE, stderr= PIPE, shell= True)  # Variables de entorno
            if process.communicate()[1]:
                raise ExecutionError(
                    f"Failed to create database file '{connectionParams['dbName']}.db', check for any incorrect arguments or missing permissions."
                )
        
        else:
            connectionParams['dbName'] = mainDbName[dbType]
            
            try:
                with await self.get_connection(dbType, dbPath, connectionParams, True) as connection:
                    self.check_for_sanitized_input(dbType, connectionParams['dbName'])
                    cursor = await self.get_cursor(connection)
                    await self.run_query_and_get_result(f"CREATE DATABASE {connectionParams['dbName']}", cursor)
            
            except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
                raise e
    
    def create_table_tasks(self, cursor, tableStatements: 'list[tuple]') -> 'tuple[list]':
        creationTasks, insertionTasks = [], []
        
        for statementTuple in tableStatements:
            creationTasks.append(
                create_task(self.run_query_and_get_result(statementTuple[0], cursor))
            )
            insertionTasks.append(
                create_task(self.run_query_and_get_result(statementTuple[1], cursor))
            )
        
        return creationTasks, insertionTasks
        
    def create_view_or_index_tasks(self, cursor, viewOrIndexStatements: 'list[str]') -> list:
        tasks = [
            create_task(self.run_query_and_get_result(statement, cursor))
            for statement in viewOrIndexStatements
            ]
        return tasks
    
    async def connect_and_load_data(self, dbType: str, objectType: str, statementList: 'list[tuple | str]', dbPath: str = None, connectionParams: dict = None) -> None:
        taskFunctions = {
            "table": self.create_table_tasks,
            "view": self.create_view_or_index_tasks,
            "index": self.create_view_or_index_tasks
        }
        
        try:
            with await self.get_connection(dbType, dbPath, connectionParams, False) as connection:
                cursor = await self.get_cursor(connection)
                
                if objectType == "table":
                    creationTasks, insertionTasks = taskFunctions[objectType](cursor, statementList)
                    await gather(*creationTasks)
                    await gather(*insertionTasks)
                
                else:
                    tasks = taskFunctions[objectType](cursor, statementList)
                    await gather(*tasks)
                
                cursor.commit()
                cursor.close()
        
        except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError) as e:
            cursor.rollback()
            raise e

    # Verificar orden de escritura para views anidadas

class MongoDatabaseWriter():
    pass

if __name__ == "__main__":
    dbwriter = SQLDatabaseWriter()
    run(dbwriter.connect_and_create_database(
        "sqlite3", "/home/brunengo/Escritorio", 
        connectionParams= {"dbName": "trying"}
        ))