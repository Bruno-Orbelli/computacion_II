from asyncio import run
from dotenv import load_dotenv
from os import getenv
from re import fullmatch, split as resplit
from argparse import ArgumentParser, Namespace
from getpass import getpass
from copy import deepcopy
from sys import path

from tqdm import tqdm
from colorama import Fore, Back, init
from pyfiglet import Figlet
from time import sleep
from typing import Literal

from asyncReader import SQLDatabaseReader, MongoDatabaseReader
from asyncWriter import SQLDatabaseWriter, MongoDatabaseWriter
from dataSenderAndReceiver import ClientDataSenderAndReceiver

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common.exceptions import ExecutionError, ConnectionError, UnsupportedDBTypeError, ArgumentError
from common.connectionData import mainDbName

class CommandLineInterface():

    def __init__(self) -> None:
        self.flags = self.parse_flags()
        init(autoreset= True)
        
        load_dotenv()
        self.nestedViewItLimit = ("NESTED_VIEW_ITERATION_LIMIT", getenv("NESTED_VIEW_ITERATION_LIMIT"))
        self.alreadyExistDbBehaviour = ("ALREADY_EXISTENT_DB_BEHAVIOUR", getenv("ALREADY_EXISTENT_DB_BEHAVIOUR"))
        
        if None in (self.nestedViewItLimit[1], self.alreadyExistDbBehaviour[1]):
            envVars = (self.nestedViewItLimit, self.alreadyExistDbBehaviour)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)      
            print(
                "\n" + Fore.RED + f"> Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'.\n"
            )
            exit(1)
        
        self.nestedViewItLimit = int(self.nestedViewItLimit[1])
        self.alreadyExistDbBehaviour = self.alreadyExistDbBehaviour[1]
    
    def parse_flags(self) -> Namespace:
        argParser = ArgumentParser(
            description= """
            Command line application for migrating SQL and NoSQL database files to another SQL or NoSQL format, or converting specific tables/collections/views/
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
        
        originArgs = self.get_database_server_args(0)
        
        if originArgs["dbType"] == "mongodb":
            reader = MongoDatabaseReader()
        else:
            reader = SQLDatabaseReader()
        
        try:
            objectsToMigrate = await self.select_objects_to_migrate(originArgs, reader)
        
        except (ArgumentError, ExecutionError, UnsupportedDBTypeError, ConnectionError) as e:
            print("\n" + Fore.RED + f"> {e}\n")
            exit(1)

        sleep(0.5)
        destinationArgs = self.get_database_server_args(1)

        if destinationArgs["dbType"] == "mongodb":
            writer = MongoDatabaseWriter()
            pass
        else:
            writer = SQLDatabaseWriter()
            pass

        try:
            connArgs = await self.attempt_db_connection(destinationArgs, writer)
            if self.alreadyExistDbBehaviour == "default":
                print("\n" + Fore.RED + f"> Database already exists. If you want to overwrite or modify it, you must change .env configurations.\n")
                exit(1)
        
        except (ArgumentError, ExecutionError, UnsupportedDBTypeError, ConnectionError) as e:
            if isinstance(e, ConnectionError):
                if destinationArgs["dbType"] != "sqlite3":
                    destinationArgs["dbName"] = mainDbName[destinationArgs["dbType"]]
                
                    try:
                        connArgs = await self.attempt_db_connection(destinationArgs, writer)                            
                        
                    except (ArgumentError, ExecutionError, UnsupportedDBTypeError, ConnectionError) as e:
                        print("\n" + Fore.RED + f"> {e}\n")
                        exit(1)
                
                else:
                    try:
                        writer.check_if_directory_exists(destinationArgs["dbPath"])
                        print(Fore.GREEN + "> Unexisting file: OK.")
                    
                    except ConnectionError as e:
                        print("\n" + Fore.RED + f"> {e}\n")
                        exit(1)
            
            else:
                print("\n" + Fore.RED + f"> {e}\n")
                exit(1)
                
        print("\nReading database...")
        readData = await self.read_objects(reader, objectsToMigrate, originArgs)
        
        senderReceiver = ClientDataSenderAndReceiver()
        
        for objTuple in readData:
            objType, objList = objTuple
            for objDict in objList:
                await senderReceiver.add_conversion_request(originArgs["dbType"], destinationArgs["dbType"], objType, objDict)
        
        responses = await senderReceiver.connect_and_run()
     
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
    
    def get_database_server_args(self, originOrDestination: Literal[0, 1]) -> 'dict[str, str]':
        print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is the {['original', 'destination'][originOrDestination]} format of your database (database type {['from', 'to'][originOrDestination]} which you want to convert)?")
        print(Fore.CYAN + "> " + Fore.WHITE + "SQLite3 (1)  MySQL (2)   PostgreSQL (3)  MongoDB (4)\n")
        
        args = {"dbType": self.get_database_format()}
        args.setdefault("dbPath", None)

        if args["dbType"] == "sqlite3":
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is the absolute file path of the {['original', 'destination'][originOrDestination]} database? (remember that your Linux user requieres {'read' if not originOrDestination else 'write'} permissions on this file).")
            if originOrDestination and self.alreadyExistDbBehaviour != "default":
                configDescription = {
                    "overwrite": "it will be overwritten entirely.",
                    "append": "objects which do not already exist will be migrated to it (an error will be raised if existing objects with matching names are found).",
                    "append-and-replace": "both already-existent and non-existent objects will be migrated, overwriting those that match in name."
                 }
                print(Fore.YELLOW + f"> WARNING: Env. variable 'ALREADY_EXISTENT_DB_BEHAVIOUR' has been set to {self.alreadyExistDbBehaviour}, meaning that if the specified path points to an already-existent file, {configDescription[self.alreadyExistDbBehaviour]}")
                print(Fore.YELLOW + f"> If this is not what you intend, exit the application and set 'ALREADY_EXISTENT_DB_BEHAVIOUR' to 'default' in .env.")
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
    
    async def attempt_db_connection(self, connArgs: 'dict[str, str]', readerOrWriter: 'SQLDatabaseReader | MongoDatabaseReader | SQLDatabaseWriter | MongoDatabaseWriter') -> None:       
        if connArgs["dbType"] == "sqlite3":
            connData = connArgs["dbPath"]
        else:
            connData = f"{connArgs['host']}:{connArgs['port']}"
        
        connArgs.setdefault("dbPath", None)
        if connArgs["dbPath"] is not None:
            connArgs.setdefault("dbName", connArgs["dbPath"].split("/")[-1][:-3:])
        
        print(f"\nAttempting connection to {connData}...")
        
        if isinstance(readerOrWriter, SQLDatabaseReader) or isinstance(readerOrWriter, SQLDatabaseWriter):
            await readerOrWriter.get_connection(connArgs["dbType"], connArgs["dbPath"], connArgs)
        else:
            await readerOrWriter.get_client(connArgs)
        
        print(Fore.GREEN + "> Connection established.")
        return deepcopy(connArgs)
    
    async def select_objects_to_migrate(self, connArgs: 'dict[str, str]', reader: 'SQLDatabaseReader | MongoDatabaseReader'):
        newArgs = await self.attempt_db_connection(connArgs, reader)
        objectsToMigrate = {}
        
        print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"Would you like to perform a full migration of '{connArgs['dbName']}' (1) or just convert certain objects (2)?")
        print()

        while True:
            option = input(">> ")

            if option not in (str(num) for num in range(1, 3)):
                print(Fore.RED + "\n> Invalid option, try again.\n")
                continue
      
            break

        if option == "1":
            if isinstance(reader, SQLDatabaseReader):
                availableObjects = await reader.connect_and_get_objects_description(newArgs["dbType"], newArgs["dbPath"], newArgs)
                objectsToMigrate.update({"tables": [(obj[1], None, None) for obj in availableObjects if obj[0] == "table"]})
                objectsToMigrate.update({"views": [obj[1] for obj in availableObjects if obj[0] == "view"]})
                objectsToMigrate.update({"indexes": [obj[1::] for obj in availableObjects if obj[0] == "index"]})
        
        else:
            if isinstance(reader, SQLDatabaseReader):
                availableObjects = await reader.connect_and_get_objects_description(newArgs["dbType"], newArgs["dbPath"], newArgs)
                tableOrCollectionNames = self.display_available_objects_and_get_input(availableObjects, "table", connArgs["dbName"])
                objectsToMigrate.update({"tables": tableOrCollectionNames})
            
            else:
                availableObjects = await reader.connect_and_get_objects_description(newArgs)
                tableOrCollectionNames = self.display_available_objects_and_get_input(availableObjects, "collection", connArgs["dbName"])
                objectsToMigrate.update({"collections": tableOrCollectionNames})        
            
            viewNames = self.display_available_objects_and_get_input(availableObjects, "view", connArgs["dbName"], [obj[0] for obj in tableOrCollectionNames])
            indexNames = self.display_available_objects_and_get_input(availableObjects, "index", connArgs["dbName"], [obj[0] for obj in tableOrCollectionNames])
            objectsToMigrate.update({"views": viewNames, "indexes": indexNames})
    
        return objectsToMigrate
               
    def build_available_objects(self, availableObjects: 'list[tuple[str]]', objectType: str, selectedTablesOrCollections: 'list[str]'):
        typeSpecificAvailableObjects = [obj for obj in availableObjects if obj[0] == objectType]
        nonTableOrCollection = [obj for obj in availableObjects if obj[0] not in ('table', 'collection')]
        
        if selectedTablesOrCollections:
            availableViewsOrIndexes = []
            
            for availableObj in typeSpecificAvailableObjects:
                if all(obj in selectedTablesOrCollections for obj in availableObj[2]):
                    availableViewsOrIndexes.append(availableObj)
            
            for _ in range(self.nestedViewItLimit):
                previousIterationList = deepcopy(availableViewsOrIndexes)
                for apparentlyNotAvailable in [obj for obj in nonTableOrCollection if (obj in typeSpecificAvailableObjects and obj not in availableViewsOrIndexes)]:
                    if all(obj in selectedTablesOrCollections + [obj[1] for obj in availableViewsOrIndexes] for obj in apparentlyNotAvailable[2]):
                        availableViewsOrIndexes.append(apparentlyNotAvailable)
                
                if availableViewsOrIndexes == previousIterationList:
                    break
            
            return availableViewsOrIndexes
        
        return typeSpecificAvailableObjects
    
    def display_available_objects_and_get_input(self, rawAvailableObjects: 'list[tuple[str]]', objectType: str, dbName: str, selectedTablesOrCollections: 'list[str]' = None):
        availableObjects = self.build_available_objects(rawAvailableObjects, objectType, selectedTablesOrCollections)
        
        if not availableObjects:
            return None
        
        sleep(0.5)
        print( "\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"Which of the following {objectType}{'es' if objectType == 'index' else 's'} in '{dbName}' would you like to convert?")
        print(Fore.CYAN + "> " + Fore.WHITE + f"Write the appropiate names one by one, {'in single quotes, ' if objectType in ('table', 'collection') else ''}then press ENTER. To end, press ENTER without any input.")
        
        if objectType in ('table', 'collection'):
            print(Fore.CYAN + "> " + Fore.WHITE + "You can specify row limit and offset/skip by using the optional flags -l (integer) and -osk (integer), respectively, after each table/collection's name.")
            print(Fore.CYAN + "> " + Fore.WHITE + "E.g: \"'foo' -l 70 -osk 5\" will select the first 70 rows of table/collection 'foo' for conversion, starting from the 6th row.")
            print("\n" + f"{objectType.upper()}S".center(40, "-") + "\n\no " + "\no ".join([obj[1] for obj in availableObjects]) + "\n")
            
        else:
            print("\n" + f"{objectType.upper()}{'ES' if objectType == 'index' else 'S'}".center(40, "-") + "\n")
            for availableViewOrIndex in availableObjects:
                print(f"o {availableViewOrIndex[1]} DEPENDS ON {', '.join(availableViewOrIndex[2])}")
            print()
        
        return self.get_object_names_with_limit_offset_skip(availableObjects, objectType, selectedTablesOrCollections)        
             
    def get_object_names_with_limit_offset_skip(self, availableObjects: 'list[str]', objectType: Literal["table", "collection", "view", "index"], selectedTablesOrCollections: 'list[str]' = None):
        selectedObjectNames, selectedObjects = [], []
        receivedInput, limit, offsetOrSkip = None, None, None

        while receivedInput != "":            
            if selectedObjectNames == [obj[1] for obj in availableObjects]:
                break
            
            receivedInput = input(">> ")

            if objectType in ("table", "collection") and receivedInput:
                receivedInputList = resplit(r'( -l | -osk )', receivedInput)
                objectName = fullmatch(r'\'(.+)\'', receivedInputList[0])
                    
                if not objectName:
                    print(Fore.RED + "\n> Invalid table/collection input; remember to quote the object's name and specify any limit or offset/skip with flags -l and -osk.\n")
                    continue

                if " -l " in receivedInputList:
                    limit = receivedInputList[receivedInputList.index(" -l ") + 1]
                if " -osk " in receivedInputList:
                    offsetOrSkip = receivedInputList[receivedInputList.index(" -osk ") + 1]
                
                if limit and not limit.isnumeric():
                    print(Fore.RED + "\n> Invalid limit or offset/skip input: valid flags are -l and -osk, with only positive integers allowed as arguments.\n")
                    continue
                
                elif offsetOrSkip and not offsetOrSkip.isnumeric():
                    print(Fore.RED + "\n> Invalid limit or offset/skip input: valid flags are -l and -osk, with only positive integers allowed as arguments.\n")
                    continue

                objectName = objectName[0][1:-1:]

            else:
                objectName = receivedInput
            
            if objectName not in [obj[1] for obj in availableObjects] + [""]:
                print(Fore.RED + f"\n> {objectType.capitalize()} '{objectName}' does not exist, check for any misspellings.\n")
                continue
            
            if objectName in selectedObjectNames:
                print(Fore.RED + f"\n> {objectType.capitalize()} '{objectName}' has already been selected, try again.\n")
                continue

            if objectType == "view":
                viewTuple = next(filter(lambda obj: obj[1] == objectName, availableObjects), None)
                if not all(originalTableOrView in selectedTablesOrCollections + selectedObjectNames for originalTableOrView in viewTuple[2]):
                    print(Fore.RED + f"\n> {objectType.capitalize()} '{objectName}' cannot be selected for conversion, since at least one of the tables/collections/views it depends on has not yet been selected.\n")
                    continue
            
            if objectName:
                if objectType in ("table", "collection"):
                    selectedObjects.append((objectName, limit, offsetOrSkip))
                elif objectType in ("view"):
                    selectedObjects.append(objectName)
                else:
                    indexTuple = next(filter(lambda obj: obj[1] == objectName, availableObjects), None)
                    selectedObjects.append(indexTuple[1::])
                
                selectedObjectNames.append(objectName)
        
        return selectedObjects
    
    async def read_objects(self, reader: 'SQLDatabaseReader | MongoDatabaseReader', objectsToMigrate: 'dict[str, list]', originArgs: 'dict[str, str]') -> list:
        readData = [] # solo lee las tablas intencionalmente
        tablesOrCollections, views, indexes = {}, [], {}
        
        for objType, objList in objectsToMigrate.items():
            if objList:
                if objType in ("tables", "collections"):
                    
                    for obj in objList:
                        tablesOrCollections.update({obj[0]: obj[1::]})
                
                '''elif objType == "views":
                    for obj in objList:
                        views.append(obj)

                else:
                    for obj in objList:
                        indexes.update({obj[0]: obj[1][0]})''' # conversión de vistas e índices aún no operativas
                    
        dbType, dbPath = originArgs["dbType"], originArgs["dbPath"]
        auxArgs = deepcopy(originArgs)
        auxArgs.pop("dbType")
        auxArgs.pop("dbPath")
        
        progressBar = tqdm(desc= "Reading objects data...", total= len(tablesOrCollections) + len(views) + len(indexes), colour= "green")
        for objTuple in (("table", tablesOrCollections), ("view", views), ("index", indexes)):
            if objTuple[1]:
                readData.append((objTuple[0], await reader.connect_and_read_data(dbType, dbPath, auxArgs, objTuple)))
            progressBar.update(len(objTuple[1]))

        return readData
            
if __name__ == "__main__":
    cli = CommandLineInterface()
    run(cli.main())
        