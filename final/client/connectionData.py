drivers = {
    "sqlite": "SQLite Unicode Driver",
    "sqlite3": "SQLite3 Unicode Driver",
    "mysql": "MySQL 8.0.32 Unicode Driver",
    "postgresql": "PostgreSQL Unicode Driver"
}

connStrs = {
    "sqlite3": "Database={0};LongNames=0;Timeout=1000;NoTXN=0;SyncPragma=NORMAL;StepAPI=0;",
    "mysql": "Server={host};Port={port};Database={dbName};Uid={user};Pwd={password};",
    "postgresql": "Server={host};Port={port};Database={dbName};Uid={user};Pwd={password};",
    # "postgresql": "User ID={user};Password={password};Host={host};Port={port};Database={dbName};Pooling=true;Min Pool Size=0;Max Pool Size=100;Connection Lifetime=0;"
}