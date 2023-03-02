dbStructureQueries = {
    "sqlite": r"SELECT sql FROM sqlite_master WHERE type='table' and name='{0}'",
    "mysql": r"SHOW CREATE TABLE {0}",
}

dataQueries = {
    "sqlite": r"SELECT * FROM [{0}]",
    "mysql": r"SELECT * FROM `{0}`",
    "postgresql": r'SELECT * FROM "{0}"'
}

limitOffsetAppend = {
    "sqlite": r" LIMIT ? OFFSET ?", 
    "mysql": r" LIMIT %s, %s",
    "postgresql": r" LIMIT %s OFFSET %s"
}
