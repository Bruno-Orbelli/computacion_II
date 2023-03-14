from asyncio import run
from re import fullmatch
from argparse import ArgumentParser, Namespace
from getpass import getpass
import tqdm
from colorama import Fore, Back, Style, init
from pyfiglet import Figlet
from time import sleep
from typing import Literal

from asyncReader import SQLDatabaseReader, MongoDatabaseReader
from exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError, InitializationError

class CommandLineInterface():

    def __init__(self) -> None:
        self.flags = self.parse_flags()
        init(autoreset= True)

    def parse_flags(self) -> Namespace:
        argParser = ArgumentParser(
            description= """
            Command line interface for migrating SQL and NoSQL database files to another SQL or NoSQL format, or converting specific tables/collections/views/
            indexes to a certain SQL/NoSQL database type.
            """,
            epilog= "By default, if no tables, collections, views or indexes are specified as params, ConverseSQL will perform a full migration of the original database."
            )
        
        argParser.add_argument("--disableLog", action= "store_true", default= False, help= "disables server logging of interactions (enabled by default)")
        argParser.add_argument("-v", "--verbose", action= "store_true", default= False, help= "enables verbose mode")
        
        return argParser.parse_args()
    
    async def main(self):
        title = Figlet("larry3d")
        print(title.renderText("ConverSQL"))
        sleep(2)
        
        originArgs = self.get_args(0)
        
        if originArgs["dbType"] == "mongodb":
            reader = MongoDatabaseReader()
        else:
            reader = SQLDatabaseReader()
        
        try:
            await self.select_objects_to_migrate(originArgs, reader)
        except (ArgumentError, ExecutionError, UnsupportedDBTypeError, ConnectionError) as e:
            print(Fore.RED + f"> {e}")
            exit(1)

        destinationArgs = self.get_args(1)
        print(originArgs, destinationArgs)
     
    def get_database_format(self) -> str:
        dbFormat = None
        
        while not dbFormat:
            inp = input(">> ")

            if inp not in (str(num) for num in range(1, 5)):
                print(Fore.RED + "\n> Invalid option, try again.\n")
                continue
                    
            dbFormat = ["sqlite3", "mysql", "postgresql", "mongodb"][int(inp) - 1]
        
        return dbFormat
    
    def get_database_name(self) -> str:
        while True:
            dbName = input(">> ")

            if not fullmatch(r"([a-zA-Z0-9]|_)+", dbName):
                print(Fore.RED + "\n> Invalid database name, try again.\n")
                continue
            
            return dbName
    
    def get_sqlite3_database_path(self) -> str:
        while True:
            dbPath = input(">> ")

            if not fullmatch(r"(\/.+)*(\/.+\.db)+", dbPath):
                print(Fore.RED + "\n> Invalid path, try again.\n")
                continue
      
            return dbPath
    
    def get_database_ip(self) -> str:
        while True:
            dbIP = input(">> ")

            ipv4re = r"((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|localhost"
            ipv6re = r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"
            
            if not (fullmatch(ipv4re, dbIP) or fullmatch(ipv6re, dbIP)):
                print(Fore.RED + "\n> Invalid IP address, try again.\n")
                continue
      
            return dbIP
    
    def get_database_port(self) -> int:
        while True:
            dbPort = input(">> ")

            if dbPort not in (str(i) for i in range(1024, 65535)):
                print(Fore.RED + "\n> Invalid port number, try again (valid port numbers range from 1024 to 65535).\n")
                continue
      
            return int(dbPort)
    
    def get_database_username(self) -> str:
        while True:
            username = input(">> ")

            if username.isspace():
                print(Fore.RED + "\n> Invalid username, try again.\n")
                continue
      
            return username
    
    def get_database_password(self) -> str:
        while True:
            password = getpass(">> ")

            if password.isspace():
                print(Fore.RED + "\n> Invalid password, try again.\n")
                continue
      
            return password
    
    def get_args(self, originOrDestination: Literal[0, 1]) -> 'dict[str, str]':
        print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is the {['original', 'destination'][originOrDestination]} format of your database (database type {['from', 'to'][originOrDestination]} which you want to convert)?")
        print(Fore.CYAN + "> " + Fore.WHITE + "SQLite3 (1)  MySQL (2)   PostgreSQL (3)  MongoDB (4)\n")
        
        args = {"dbType": self.get_database_format()}

        if args["dbType"] == "sqlite3":
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is the absolute file path of the {['original', 'destination'][originOrDestination]} database? (remember that your Linux user requieres read permissions on this file).")
            print()
            args.update({"dbPath": self.get_sqlite3_database_path()})
        
        else:
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is the name of the {['original', 'destination'][originOrDestination]} database?")
            print()
            args.update({"dbName": self.get_database_name()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is the {['original', 'destination'][originOrDestination]} database server IP address? (both IPv4 and IPv6 supported).")
            print()
            args.update({"host": self.get_database_ip()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"On which port is the {['original', 'destination'][originOrDestination]} database server running?")
            print()
            args.update({"port": self.get_database_port()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is your username for '{args['dbName']}' database? (remember that your user requires read permissions over this database's schema and tables/views/indexes).")
            print(Fore.CYAN + "> " + Fore.WHITE + "If authentication is not required, press ENTER and ignore this field.\n")
            args.update({"user": self.get_database_username()})

            args.setdefault("password", "")
            if args["user"]:
                print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is your password for '{args['user']}'?")
                print()
                args.update({"password": self.get_database_password()})
        
        return args
       
    async def attempt_db_connection(self, connArgs: 'dict[str, str]', reader: 'SQLDatabaseReader | MongoDatabaseReader') -> None:       
        if connArgs["dbType"] == "sqlite3":
            connData = connArgs["dbPath"]
        else:
            connData = f"{connArgs['host']}:{connArgs['port']}"
        
        connArgs.setdefault("dbPath", None)
        if connArgs["dbPath"] is not None:
            connArgs.setdefault("dbName", connArgs["dbPath"].split("/")[-1][:-3:])
        
        print(f"\nAttempting connection to {connData}...")
        
        if isinstance(reader, SQLDatabaseReader):
            await reader.get_sql_connection(connArgs["dbType"], connArgs["dbPath"], connArgs)
        else:
            await reader.get_mongo_client(connArgs)
        
        print(Fore.GREEN + "> Connection established.")
        return connArgs
    
    async def select_objects_to_migrate(self, connArgs: 'dict[str, str]', reader: 'SQLDatabaseReader | MongoDatabaseReader'):
        newArgs = await self.attempt_db_connection(connArgs, reader)
        
        print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"Would you like to perform a full migration of '{connArgs['dbName']}' (1) or just convert certain objects (2)?")
        print()

        while True:
            option = input(">> ")

            if option not in (str(num) for num in range(1, 3)):
                print(Fore.RED + "\n> Invalid option, try again.\n")
                continue
      
            break

        if option == "2":
            if isinstance(reader, SQLDatabaseReader):
                availableObjects = await reader.sql_connect_and_get_objects_name(newArgs["dbType"], newArgs["dbPath"], newArgs)
                tableOrCollectionNames = self.display_available_objects_and_get_input(availableObjects, "table", newArgs["dbName"])
            
            else:
                availableObjects = await reader.mongo_connect_and_get_objects_name(newArgs)
                tableOrCollectionNames = self.display_available_objects_and_get_input(availableObjects, "collection", newArgs["dbName"])              
            
            viewNames = self.display_available_objects_and_get_input(availableObjects, "view", newArgs["dbName"], tableOrCollectionNames)
            indexNames = self.display_available_objects_and_get_input(availableObjects, "index", newArgs["dbName"], tableOrCollectionNames)
        
        print(tableOrCollectionNames, viewNames, indexNames)
            
    def display_available_objects_and_get_input(self, availableObjects: 'list[tuple[str]]', objectType: str, dbName: str, selectedTablesOrCollections: 'list[str]' = None):
        sleep(0.5)
        print( "\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"Which of the following {objectType}s in '{dbName}' would you like to convert?")
        print(Fore.CYAN + "> " + Fore.WHITE + "Write the appropiate names one by one, then press ENTER. To end, press ENTER without any input.")
        
        print(availableObjects, objectType)
        availableObjectNames = [obj[1] for obj in availableObjects if obj[0] == objectType]
        print(availableObjectNames)
        
        if availableObjectNames:   
            if objectType in ('table', 'collection'):
                print("\n" + f"{objectType.upper()}S".center(40, "-") + "\no " + "\no ".join(availableObjectNames) + "\n")
            
            else:
                printedTitle = 0
                nonTableOrCollection = [obj for obj in availableObjects if obj[0] not in ('table', 'collection')]
                for availableObj in nonTableOrCollection:
                    if availableObj[1] in availableObjectNames and all(obj in selectedTablesOrCollections for obj in availableObj[2]):
                        if not printedTitle:
                            print("\n" + f"{objectType.upper()}{'ES' if objectType == 'index' else 'S'}".center(40, "-") + "\n")
                            printedTitle = 1
                        
                        print(f"o {availableObj[1]} DEPENDS ON {', '.join(availableObj[2])}")
                print()
                
            return self.get_object_names(availableObjectNames, objectType)
        
        return None
    
    def get_object_names(self, availableObjectNames: 'list[str]', objectType: str) -> 'list[str]':
        objects = []
        objectName = None
        
        while objectName != "":
            if objects == availableObjectNames:
                break
            
            objectName = input(">> ")
            
            if objectName not in availableObjectNames + [""]:
                print(Fore.RED + f"\n> {objectType.capitalize()} '{objectName}' does not exist, check for any misspellings.\n")
                continue
            
            elif objectName in objects:
                print(Fore.RED + f"\n> {objectType.capitalize()} '{objectName}' has already been selected, try again.\n")
                continue

            elif objectName != "":
                objects.append(objectName)
        
        return objects
            
if __name__ == "__main__":
    cli = CommandLineInterface()
    run(cli.main())
        