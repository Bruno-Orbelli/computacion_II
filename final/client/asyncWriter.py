import pyodbc
from dotenv import load_dotenv
from os import getcwd, getenv, remove
from os.path import dirname
from shutil import move
from os.path import dirname
from subprocess import Popen, PIPE
from asyncio import create_task, gather, run
from sys import path

baseDir = dirname(getcwd())
try:
    path.index(baseDir)
except ValueError:
    path.append(baseDir)

from baseAcceser import SQLDatabaseAcceser

from common.connectionData import mainDbName
from common.exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError, InitializationError

# SQL

class SQLDatabaseWriter(SQLDatabaseAcceser):
    
    def __init__(self) -> None:
        load_dotenv()
        
        self.migratedPermissions = ("MIGRATED_DB_PERMISSIONS", getenv("MIGRATED_DB_PERMISSIONS"))
        self.alreadyExistentBehaviour = ("ALREADY_EXISTENT_DB_BEHAVIOUR", getenv("ALREADY_EXISTENT_DB_BEHAVIOUR"))

        if None in (self.migratedPermissions[1], self.alreadyExistentBehaviour[1]):
            envVars = (self.migratedPermissions, self.alreadyExistentBehaviour)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
    
    async def connect_and_create_database(self, dbType: str, dbPath: str = None, connectionParams: dict = None) -> None:
        if dbType == "sqlite3": # Evitar sobreescribir el archivo si ya existe
            if self.alreadyExistentBehaviour == "overwrite":
                try:
                    move(connectionParams['dbPath'], "./del")
                except FileNotFoundError:
                    pass
            
            boundVolPath = f"/volumebind{dbPath}"
            process = Popen(f"cd {dirname(boundVolPath)}; touch {connectionParams['dbName']};", stdout= PIPE, stderr= PIPE, shell= True)  # Variables de entorno
            
            if process.communicate()[1]:
                raise ExecutionError(
                    f"Failed to create database file '{connectionParams['dbName']}', check for any incorrect arguments or missing permissions."
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
        
    def create_view_or_index_tasks(self, cursor, viewOrIndexStatements: 'list[tuple]') -> list:
        tasks = [
            create_task(self.run_query_and_get_result(statementTuple[0], cursor))
            for statementTuple in viewOrIndexStatements
            ]
        return tasks
    
    async def connect_and_write_data(self, dbType: str, objectType: str, statementList: 'list[tuple | str]', dbPath: str = None, connectionParams: dict = None) -> None:
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
            
            if self.alreadyExistentBehaviour == "overwrite":
                if dbType == "sqlite3":
                    try:
                        remove(f"../del/{connectionParams['dbName']}")
                    except FileNotFoundError:
                        pass
        
        except (ArgumentError, ConnectionError, ExecutionError, UnsupportedDBTypeError, pyodbc.Error) as e:
            if isinstance(e, pyodbc.Error):
                if self.alreadyExistentBehaviour == "append":
                    raise ExecutionError(f"There is at least one object matching in name with an already existent entity in '{connectionParams['dbname']}'.")
                elif self.alreadyExistentBehaviour == "append-and-replace":
                    pass
            
            else:
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