import pyodbc
from subprocess import Popen, PIPE
from os.path import dirname
from sys import path

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common.connectionData import drivers, connStrs
from common.exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError

class SQLDatabaseAcceser():
    
    async def get_connection(self, dbType: str, dbPath: str = None, connectionParams: dict = None, autocommit = bool) -> pyodbc.Connection:        
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
            return pyodbc.connect(connectionStr, readonly= True, autocommit= autocommit)
        
        except (pyodbc.OperationalError, pyodbc.Error):
            raise ConnectionError(
                "Failed to establish connection with {host}:{port} (database `{dbName}`).\nEnsure: \n - server is running and accepting TCP/IP connections at that address. \n - the correct database type has been specified. \n - other requiered arguments (username, password, ...) are not incorrect.".format(**connectionParams)
                )
        
    async def get_cursor(self, connObject) -> pyodbc.Cursor:
        try:
            return connObject.cursor()
        except pyodbc.Error:
            raise ConnectionError("Connection with database at {host}:{port} (`{dbName}`) has been lost.")
    
    def check_if_directory_exists(self, dbPath: str):
        process = Popen(f"cd {dirname(dbPath)}", stdout= PIPE, stderr= PIPE, shell= True)
        if process.communicate()[1]:
            raise ConnectionError(
                f"Database dir '{dirname(dbPath)}' not found or read/write access not granted for current user; check for any misspellings and ensure you have read/write permissions."
            )
    
    async def run_query_and_get_result(self, query: str, cursor: pyodbc.Cursor, *params) -> 'list | None':    
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
        
        try:
            return cursor.fetchall()
        
        except pyodbc.ProgrammingError:
            pass
    
    def check_for_sanitized_input(self, dbType: str, queryInput: str) -> None:
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