from re import fullmatch
from argparse import ArgumentParser, Namespace
from getpass import getpass
import tqdm
from colorama import Fore, Back, Style, init
from pyfiglet import Figlet
from time import sleep

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
    
    def main(self):
        title = Figlet("larry3d")
        print(title.renderText("ConverSQL"))
        sleep(2)
        
        originArgs = self.get_origin_args()
        destinationArgs = self.get_destination_args()
        print(originArgs, destinationArgs)
     
    def get_database_format(self) -> str:
        dbFormat = None
        
        while not dbFormat:
            inp = input(">> ")

            if inp not in (str(num) for num in range(1, 5)):
                print(Fore.RED + "> Invalid option, try again.\n")
                continue
                    
            dbFormat = ["sqlite3", "mysql", "postgresql", "mongodb"][int(inp) - 1]
        
        return dbFormat
    
    def get_database_name(self) -> str:
        while True:
            dbName = input(">> ")

            if not fullmatch(r"([a-zA-Z0-9]|_)+", dbName):
                print(Fore.RED + "> Invalid database name, try again.\n")
                continue
            
            return dbName
    
    def get_sqlite3_database_path(self) -> str:
        while True:
            dbPath = input(">> ")

            if not fullmatch(r"(\/.+)*(\/.+\.db)+", dbPath):
                print(Fore.RED + "> Invalid path, try again.\n")
                continue
      
            return dbPath
    
    def get_database_ip(self) -> str:
        while True:
            dbIP = input(">> ")

            ipv4re = r"((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|localhost"
            ipv6re = r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"
            
            if not (fullmatch(ipv4re, dbIP) or fullmatch(ipv6re, dbIP)):
                print(Fore.RED + "> Invalid IP address, try again.\n")
                continue
      
            return dbIP
    
    def get_database_port(self) -> int:
        while True:
            dbPort = input(">> ")

            if dbPort not in (str(i) for i in range(1024, 65535)):
                print(Fore.RED + "> Invalid port number, try again (valid port numbers range from 1024 to 65535).\n")
                continue
      
            return int(dbPort)
    
    def get_database_username(self) -> str:
        while True:
            username = input(">> ")

            if username.isspace():
                print(Fore.RED + "> Invalid username, try again.\n")
                continue
      
            return username
    
    def get_database_password(self) -> str:
        while True:
            password = getpass(">> ")

            if password.isspace():
                print(Fore.RED + "> Invalid password, try again.\n")
                continue
      
            return password
    
    def get_origin_args(self) -> 'dict[str, str]':
        print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the original format of your database (database type from which you want to convert)?")
        print(Fore.CYAN + "> " + Fore.WHITE + "SQLite3 (1)  MySQL (2)   PostgreSQL (3)  MongoDB (4)\n")
        
        args = {"from": self.get_database_format()}

        if args["from"] == "sqlite3":
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the absolute file path of the original database? (remember that your Linux user requieres read permissions on this file).")
            print()
            args.update({"originDBPath": self.get_sqlite3_database_path()})
        
        else:
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the name of the original database?")
            print()
            args.update({"originDBName": self.get_database_name()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the original database server IP address? (both IPv4 and IPv6 supported).")
            print()
            args.update({"originHost": self.get_database_ip()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "On which port is the original database server running?")
            print()
            args.update({"originPort": self.get_database_port()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is your username for '{args['originDBName']}' database? (remember that your user requires read permissions over this database's schema and tables/views/indexes).")
            print()
            args.update({"originUser": self.get_database_username()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is your password for '{args['originUser']}'?")
            print()
            args.update({"originUser": self.get_database_password()})
        
        return args

    def get_destination_args(self) -> 'dict[str, str]':
        print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the destination format of your database (database type to which you want to convert)?")
        print(Fore.CYAN + "> " + Fore.WHITE + "SQLite3 (1)  MySQL (2)   PostgreSQL (3)  MongoDB (4)\n")

        args = {"to": self.get_database_format()}

        if args["to"] == "sqlite3":
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the absolute file path of the destination database? (remember that your Linux user requieres read-write permissions on this file).")
            print()
            args.update({"destinationDBPath": self.get_sqlite3_database_path()})
        
        else:
            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the name of the destination database?")
            print()
            args.update({"destinationDBName": self.get_database_name()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "What is the destination database server IP address? (both IPv4 and IPv6 supported).")
            print()
            args.update({"destinationHost": self.get_database_ip()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + "On which port is the destination database server running?")
            print()
            args.update({"destinationPort": self.get_database_port()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is your username for '{args['destinationDBName']}' database? (remember that your user requires read-write permissions over this database's schema and tables/views/indexes).")
            print()
            args.update({"destinationUser": self.get_database_username()})

            print("\n" + Fore.CYAN + "> " + Back.CYAN + Fore.BLACK + f"What is your password for '{args['destinationUser']}'?")
            print()
            args.update({"destinationUser": self.get_database_password()})

        return args      
            
if __name__ == "__main__":
    cli = CommandLineInterface()
    cli.main()
        