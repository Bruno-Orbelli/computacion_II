SQLDbStructureQueries = {
    "sqlite3": r"SELECT sql FROM sqlite_master WHERE type='table' and name='{0}'",
    "mysql": r"SHOW CREATE TABLE {0}",
}

SQLdataQueries = {
    "sqlite3": r"SELECT * FROM [{0}]",
    "mysql": r"SELECT * FROM `{0}`",
    "postgresql": r'SELECT * FROM "{0}"',
}

mongodbAvailableQueryElems = {
    "find": ".find()",
    "limit": ".limit({0})",
    "skip": ".skip({0})"
}