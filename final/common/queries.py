SQLDbStructureQueries = {
    "sqlite3": "SELECT sql FROM sqlite_master WHERE type='table' AND name='{0}'",
    "mysql": "SHOW CREATE TABLE `{0}`",
}

SQLObjectsNameQueries = {
    "sqlite3": {
        "table": "SELECT name FROM sqlite_master WHERE sql NOTNULL AND type='table'",
        "view": "SELECT name, sql FROM sqlite_master WHERE sql NOTNULL and type='view'",
        "index": "SELECT name, tbl_name FROM sqlite_master WHERE sql NOTNULL and type='index'",
    },
    "mysql": {
        "table": "SELECT TABLE_NAME FROM TABLES WHERE TABLE_SCHEMA='{0}' AND TABLE_TYPE='BASE TABLE'",
        "view": "SELECT TABLE_NAME, VIEW_DEFINITION FROM VIEWS WHERE TABLE_SCHEMA='{0}'",
        "index": "SELECT INDEX_NAME, TABLE_NAME FROM STATISTICS WHERE INDEX_SCHEMA='{0}' AND INDEX_NAME!='PRIMARY'"
    },
    "postgresql": {
        "table": "SELECT tablename FROM pg_tables WHERE schemaname='public'",
        "view": "SELECT viewname, definition FROM pg_views WHERE schemaname='public'",
        "index": "SELECT indexname, tablename FROM pg_indexes WHERE schemaname='public' AND indexname NOT LIKE '%\_pkey'"
    }
}

SQLDataQueries = {
    "sqlite3": "SELECT * FROM [{0}]",
    "mysql": "SELECT * FROM `{0}`",
    "postgresql": 'SELECT * FROM "{0}"',
}

SQLViewQueries = {
    "sqlite3": "SELECT sql FROM sqlite_master WHERE type='view' AND name='{0}'",
    "mysql": "SHOW CREATE VIEW `{0}`",
    "postgresql": "SELECT pg_get_viewdef('{0}', true)"
}

SQLIndexQueries = {
    "sqlite3": "SELECT sql FROM sqlite_master WHERE type='index' AND name='{0}'",
    "mysql": "SHOW CREATE TABLE `{0}`",
    "postgresql": "SELECT indexdef FROM pg_indexes WHERE indexname='{0}'"
}

mongodbAvailableQueryElems = {
    "find": ".find({0})",
    "limit": ".limit({1})",
    "skip": ".skip({2})",
    "getIndexes": ".index_information()",
    "getCollectionAndViewNames": ".list_collection_names()",
}