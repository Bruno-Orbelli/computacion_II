## Migrating databases with ConverSQL
In this document, you'll find a basic guide for using ConverSQL, with some information regarding its main features. ConverSQL is a client/server app which can migrate database files between different database formats and SQL and NoSQL database engines in a simple and accesible manner.

### Table of contents

1. [Getting started](#1-getting-started)
2. [ConverSQL's environment configuration](#2-conversqls-environment-configuration)
3. [Using the interactive CLI app](#3-using-the-interactive-cli-app)
    1. [Selecting your origin database](#31-selecting-your-origin-databse)
    2. [Selecting the objects to export](#32-selecting-the-objects-to-export)
    3. [Specifying a destination database](#33-specifying-a-destination-database)
4. [The *Conversion* process](#4-the-conversion-process)
5. [Results](#5-results)

### 1. Getting started
To correctly start and launch ConverSQL, you can refer to the [following document](https://github.com/Bruno-Orbelli/computacion_II/blob/master/final/install).

### 2. ConverSQL's environment configuration
ConverSQL's configuration can be changed via the .env and docker/dockerenv.txt files, allowing for some degree of customization. The defined environment varibles are the following:

- `SERVER_IP_PROTOCOL`: (4, 6 or 10) \- If set to 10, two ConverSQL server instances will be started (at 0.0.0.0 and ::0) in the server machine, accepting connections from both IPv4 and IPv6 clients. Otherwise, a single IPv4 or IPv6 instance will be launched. (DEFAULT = 10)
- `SERVER_IPV4_ADDRESS` \- IPv4 address of the server machine. (DEFAULT = '127.0.0.1')
- `SERVER_IPV6_ADDRESS` \- IPv6 address of the server machine. (DEFAULT = '::1')
- `SERVER_IPV4_PORT` \- Port nº of the IPv4 server instance. (DEFAULT = 20111)
- `SERVER_IPV6_PORT` \- Port nº of the IPv6 server instance. (DEFAULT = 20112)
- `SERVER_CONNECTION_TIMEOUT` \- Time (in seconds) the server instance will wait for input before timeouting a client. (DEFAULT = 10)
- `SERVER_LOG_ENABLED`: (0 or 1) \- If set to 1, server instances will log their interactions in /main/log.txt file inside the sender_receiver container, which can then be accessed via `docker cp` or `docker exec`. (DEFAULT = 1)
- `SERVER_ALLOCATED_CORES` \- Nº of CPU cores allocated to the server to process requests. A value of 1 will result in a non-concurrent server. (DEFAULT = 6)

- `CLIENT_RETRY_ATTEMPTS` \- ConverSQL client will wait for server responses for an arbitrary amount of time, before moving on to another task (if possible). If the amount of times the client attempts to read its socket unsuccesfully exceeds this number, it will end its connection to the server. The counter is reset with each succesful read. It is recommended to keep this number high, as the client jumps quickly between tasks. (DEFAULT = 10000)
- `CLIENT_PREFERED_PROTOCOL`: (4 or 6) \- If set to 4, the client will first attempt to connect to the IPv4 server instance. If this is unsuccesful, it will then try to connect to the IPv6 server instance (and vice versa). (DEFAULT = 4)

- `BROKER_ADDRESS` \- Task broker's service name or IP address inside the Conversql server network, used by Celery (recommended not to modify). (DEFAULT = "redis")
- `BROKER_PORT` \- Task broker instance's port inside the Conversql server network (recommended not to modify). (DEFAULT = 6379)

- `NESTED_VIEW_ITERATION_LIMIT` \- As database views can be nested and depend on other views, this number limits the depth of search the client does when determining which view depends on which. Most database engines do not allow for a level of nesting greater than 32, so this number is high enough to ensure all dependent views will be accounted for. (DEFAULT = 32)
- `MIGRATED_DB_PERMISSIONS` \- Access permissions over the resulting migrated databases (not yet implemented). (DEFAULT = "default")
- `ALREADY_EXISTENT_DB_BEHAVIOUR`: (default, overwrite, append, append-and-replace) \- If the file path or the database name provided for the migration corresponds to an already-existing file o database, this variable defines the client's behaviour: *default* will abort the migration, *overwrite* will fully overwrite the already-existing database or file, *append* will only migrate the objects whose names do not match with already-existing objects, and *append-and-replace* will both export objects with non-matching names and replace those that already exist, if the names match. (DEFAULT = "default")

Bear in mind that .env and docker/dockerenv.txt must be consistent at all times, sharing the same value for each env. variable and preserving their respective formats.
Any change made to either of them must be updated on the other.

### 3. Using the interactive CLI app
The database migration process is comprised of several steps:

#### 3.1. Selecting your origin databse
When booting the app with `./launch.sh`, you will be asked for a series of parameters:
- Database type (available: SQLite3, MySQL, PostgreSQL and MongoDB).
- Database file path (for serverless database engines like SQLite3).
- Database server's address (for database engines with servers).
- User credentials (if needed).

With these, the client will attempt to connect to the origin database. Your Linux or database user must have read or SELECT permissions over it.

#### 3.2. Selecting the objects to export
After succesfully connecting to the origin database, you can choose to perform a full migration (exporting all tables/collections, views, indexes) or selecting which objects to convert from those available. In the case of tables/collections, the application allows for setting a limit and an offset/skip of rows for each one of them, with the syntax *'\<table or collection name\>' -l \<limit\> -osk \<offset/skip\>*. For all other objects, just specify their names.

**NOTE**: Depending on the value set for `NESTED_VIEW_ITERATION_LIMIT`, some deeply-nested views might not appear as available objects in this stage.

#### 3.3. Specifying a destination database
Finally, you will be prompted to specify the same paramaters as in [3.1](#31-selecting-your-origin-databse) for your destination database. If the file path or database name provided matches with an already-existing file or database, the client will behave based on the value of `ALREADY_EXISTENT_DB_BEHAVIOUR`. In such a case, your Linux or  database user must have write or INSERT permissions over this object; otherwise, a new file/database will be created from scratch.

### 4. The *Conversion* process
ConverSQL's server implements [Celery](https://docs.celeryq.dev/en/stable/), an asynchronous distributed system to process vast amounts of requests messages concurrently, with [Redis](https://redis.io/) as its message broker. The client will asynchronously send every selected database object's data as a conversion request, which will then be accordingly processed to produce a response with the converted data.

### 5. Results
Once all requests have been correctly processed, the appropiate statements will be executed to persist the converted data into the specified database, preserving the original constraints, relations and requirements. From then on, you can access and modify it without any issues.