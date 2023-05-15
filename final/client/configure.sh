#!/bin/sh

echo "Installing required database connectors..."

apt-get install -y unixodbc unixodbc-dev libsqliteodbc libpq-dev odbcinst openssl odbc-postgresql
cp connectors/mysql-connector-odbc-8.0.32-linux-glibc2.28-x86-64bit/lib/libmyodbc8* /usr/lib/x86_64-linux-gnu/odbc/
connectors/mysql-connector-odbc-8.0.32-linux-glibc2.28-x86-64bit/bin/myodbc-installer -d -a -n "MySQL" -t "DRIVER=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so;"

echo "Configuring odbcinst.ini..."

odbcinst="
[MySQL 8.0.32 Unicode Driver]
Driver=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so
UsageCount=1

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
Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbca.so
Setup=libodbcpsqlS.so
Debug=0
CommLog=1
UsageCount=1

[PostgreSQL Unicode Driver]
Description=PostgreSQL ODBC driver (Unicode version)
Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
Setup=libodbcpsqlS.so
Debug=0
CommLog=1
UsageCount=1"

echo $odbcinst > /etc/odbcinst.ini

echo "Installing required Python dependencies..."
pip install -r python_requirements.txt

echo "All set!"