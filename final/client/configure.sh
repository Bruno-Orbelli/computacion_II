#!/bin/sh

echo "Installing required database connectors..."

apt-get update
apt-get install -y apt-utils sqlite3 unixodbc unixodbc-dev libsqliteodbc libpq-dev libodbc1 odbcinst openssl odbc-postgresql libsqlite3-dev make
cp connectors/mysql-connector-odbc-8.0.32-linux-glibc2.28-x86-64bit/lib/libmyodbc8* /usr/lib/x86_64-linux-gnu/odbc/
connectors/mysql-connector-odbc-8.0.32-linux-glibc2.28-x86-64bit/bin/myodbc-installer -d -a -n "MySQL" -t "DRIVER=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so;"

apt-get clean && rm -rf /var/lib/apt/lists/*

echo "Configuring odbcinst.ini..."

odbcinst="
[SQLite Unicode Driver]
Description=SQLite ODBC Driver
Driver=libsqliteodbc.so
Setup=libsqliteodbc.so
UsageCount=1

[SQLite3 Unicode Driver]
Description=SQLite3 ODBC Driver
Driver=libsqlite3odbc.so
Setup=libsqlite3odbc.so
UsageCount=1

[PostgreSQL ANSI Driver]
Description=PostgreSQL ODBC driver (ANSI version)
Driver=psqlodbca.so
Setup=libodbcpsqlS.so
Debug=0
CommLog=1
UsageCount=1

[PostgreSQL Unicode Driver]
Description=PostgreSQL ODBC driver (Unicode version)
Driver=psqlodbcw.so
Setup=libodbcpsqlS.so
Debug=0
CommLog=1
UsageCount=1

[MySQL 8.0.32 Unicode Driver]
Driver=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so
UsageCount=1
"

echo "$odbcinst" > /etc/odbcinst.ini

echo "Installing required Python dependencies..."
pip install -r python_requirements.txt

echo "All set!"