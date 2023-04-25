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
from common.connectionData import drivers, connStrs
from common.exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError

# SQL

class SQLDatabaseWriter(SQLDatabaseAcceser):
    async def connect_and_create_database(self, dbType: str, dbName: str = None, dbPath: str = None, connectionParams: dict = None) -> None:
        if dbType == "sqlite3":
            Popen(["cd", f"{dbPath};", "touch", f"{dbName}.db"])


class MongoDatabaseWriter():
    pass