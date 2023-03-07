SQLDbStructureQueries = {
    "sqlite3": "SELECT sql FROM sqlite_master WHERE type='table' AND name='{0}'",
    "mysql": "SHOW CREATE TABLE `{0}`",
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
    "getIndexes": ".index_information()"
}