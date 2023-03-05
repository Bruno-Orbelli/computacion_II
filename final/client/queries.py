SQLDbStructureQueries = {
    "sqlite3": r"SELECT sql FROM sqlite_master WHERE type='table' AND name='{0}'",
    "mysql": r"SHOW CREATE TABLE `{0}`",
}

SQLDataQueries = {
    "sqlite3": r"SELECT * FROM [{0}]",
    "mysql": r"SELECT * FROM `{0}`",
    "postgresql": r'SELECT * FROM "{0}"',
}

SQLViewQueries = {
    "sqlite3": r"SELECT sql FROM sqlite_master WHERE type='view' AND name='{0}'",
    "mysql": r"SHOW CREATE VIEW `{0}`",
    "postgresql": r"SELECT pg_get_viewdef('{0}', true)"
}

SQLIndexQueries = {
    "sqlite3": r"SELECT sql FROM sqlite_master WHERE type='index' AND name='{0}'",
    "mysql": r"SHOW CREATE TABLE `{0}`",
}

mongodbAvailableQueryElems = {
    "find": ".find()",
    "limit": ".limit({0})",
    "skip": ".skip({0})"
}