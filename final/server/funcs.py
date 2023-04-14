from celery import Celery
from typing import Literal, Any
from re import sub

app = Celery("funcs", broker="redis://localhost", backend="redis://localhost:6379")

# Probar procesamiento con unidades de diferentes tamaños (diferentes chunks)

@app.task
def process_information():
    pass

def get_syntax_substitutions(originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], params: 'dict[str, str]'):
    subsDict = {
        ("mysql", "sqlite3") : {
            r'(?<!FOREIGN|PRIMARY) KEY .+,\n': "",
            r'ENGINE=.+': "",
            r'\s\`': " [",
            r'\`\s': "] ",
            r'\(\`': "([",
            r'\`\)': "])"
        },
        ("postgresql", "sqlite3"): {
            r'CREATE TABLE .*?\..*? \(': f"CREATE TABLE [{params['tableName']}] (",
            r'\\n    (?!ADD CONSTRAINT)': "\\n    [",
            r'(\n    \[[\w\s]*?) ': r"\1] ",
            r'"': ""
        }
    }
    return subsDict[(originDBType, destinationDBType)]

def get_natively_mapped_functions(originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], params: 'dict[str, str]'):
    nativelyMappedFunctions = {
        ("mysql", "sqlite3") : {
            "withSameName": {
                "ABS": "abs",
                "AVG": "avg",
                "CAST": "cast",
                "COUNT": "count",
                "DATE": "date",
                "IFNULL": "ifnull",
                "INSTR": "instr",
                "LENGTH": "length",
                "LOWER": "lower",
                "LTRIM": "ltrim",
                "NULLIF": "nullif",
                "JSON_ARRAY": "json_array",
                "MAX": "max",
                "MIN": "min",
                "REGEXP": "regexp",
                "REPLACE": "replace",
                "ROUND": "round",
                "RTRIM": "rtrim",
                "SIGN": "sign",
                "SUM": "sum",
                "TIME": "time",
                "UPPER": "upper",
            },
            "withDifferentName": {
                "ASCII": "unicode",
                "CHAR_LENGTH": "length",
                "CHARACTER_LENGTH": "length",
                "DIV": "/",
                "LAST_INSERT_ID": "last_insert_rowid",
                "LCASE": "lower",
                "JSON_ARRAYAGG": "json_group_array",
                "JSON_OBJECTAGG": "json_group_object",
                "MOD": "%",
                "OCTET_LENGTH": "length",
                "RLIKE": "regexp",
                "ROW_COUNT": "changes",
                "UCASE": "upper",
                "VERSION": "sqlite_version"
            },
            "mathWithSameName": {
                "ACOS": "acos",
                "ASIN": "asin",
                "ATAN2": "atan2",
                "CEIL": "ceil",
                "CEILING": "ceiling",
                "COS": "cos",
                "DEGREES": "degrees",
                "EXP": "exp",
                "FLOOR": "floor",
                "LN": "ln",
                "LOG2": "log2",
                "LOG10": "log10",
                "MOD": "mod",
                "PI": "pi",
                "POW": "pow",
                "POWER": "power",
                "RADIANS": "radians",
                "SIN": "sin",
                "SQRT": "sqrt",
                "TAN": "tan"
            }
        }
    }

    return nativelyMappedFunctions[(originDBType, destinationDBType)]["withDifferentName"]

def adapt_non_natively_mapped_functions(originalFuncName: str, funcArgs: 'list[str]', originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"]):
    nonNativelyMappedFunctions = {
        ("mysql", "sqlite3"): {
            "ATAN": ("ATAN({0} / {1})", "ATAN({0})"),
            "COT": "1 / TAN({0})",
            "LOG": ("LOG({0}, {1})", "LOG({0})"),
            "RAND": "SUBSTR((ABS(RANDOM() / CAST(9223372036854775807 AS FLOAT))), 0, 17)",
            "TRUNCATE": "CAST(printf('%.{0}}s', {1}) AS FLOAT)", 
            "GROUP_CONCAT": ("GROUP_CONCAT({0}, {1})", "GROUP_CONCAT({0})"),
            "VAR_POP": "SUM(({0}.{1} - sub.a) * ({0}.{1} - sub.a)) / (COUNT({0}.{1}) - 1) FROM (SELECT AVG({0}.{1}) AS a FROM {0}) AS sub",
            "VAR_SAMP": "SUM(({0}.{1} - sub.a) * ({0}.{1} - sub.a)) / (COUNT({0}.{1}) - 1) FROM (SELECT AVG({0}.{1}) AS a FROM {0}) AS sub",
            "VARIANCE": "SUM(({0}.{1} - sub.a) * ({0}.{1} - sub.a)) / (COUNT({0}.{1}) - 1) FROM (SELECT AVG({0}.{1}) AS a FROM {0}) AS sub",
            "MEMBER OF": "'{0}' IN (SELECT value FROM json_each({1})",
            "DIV": "CAST({0} / {1} AS INT)",
            "/": "CAST({0} AS FLOAT) / CAST({1} AS FLOAT)",
            "IF": "CASE WHEN {0} AND {1} IS NOT NULL THEN {1} ELSE {2} END",
            "ADDATE": "DATE({0}, {interval_and_amount})",
            "DATEADD": "DATE({0}, {interval_and_amount})",
            "ADDTIME": "DATETIME({0}, {date_modifiers_concatenated})",
            "CURDATE": "DATE('now', 'localtime')",
            "CURTIME": "TIME('now', 'localtime')",
            "CURRENT_TIMESTAMP": "DATETIME('now', 'localtime')",
            "NOW": "DATETIME('now', 'localtime')",
            "LOCALTIME": "DATETIME('now', 'localtime')",
            "LOCALTIMESTAMP": "DATETIME('now', 'localtime')",
            "SYSDATE": "DATETIME('now', 'localtime')",
            "DATEDIFF": "CAST((JULIANDAY({0}) - JULIANDAY({1})) AS INT)",
            "DATE_FORMAT": "STRFTIME({0}, {1})", # solo mapear si los especificadores están disponibles
            "DATE_SUB": "DATE({0}, {interval_and_amount})",
            "SUBDATE": "DATE({0}, {interval_and_amount})",
            "DATE_SUB": "DATE({0}, {interval_and_amount})",
            "DAY": r"CAST(STRFTIME('%d', {0}) AS INT)",
            "DAY_OF_MONTH": r"CAST(STRFTIME('%d', {0}) AS INT)",
            "DAYNAME": "SELECT CASE	WHEN strftime('%w', {0}) = '0' THEN 'Sunday' WHEN strftime('%w', {0}) = '1' THEN 'Monday' WHEN strftime('%w', {0}) = '2' THEN 'Tuesday'	WHEN strftime('%w', {0}) = '3' THEN 'Wednesday'	WHEN strftime('%w', {0}) = '4' THEN 'Thursday'	WHEN strftime('%w', {0}) = '5' THEN 'Friday' WHEN strftime('%w', {0}) = '6' THEN 'Saturday' END",
            "DAYOFWEEK": r"CAST(STRFTIME('%w', {0}, '+1 day') AS INT)",
            "DAYOFYEAR": r"CAST(STRFTIME('%j', {0}) AS INT)",
            "EXTRACT": "STRFTIME({0}, {1})", # solo mapear si los especificadores están disponibles
            "FROM_DAYS": "DATE('0000-01-01', '+{0} days')",
            "FROM_UNIXTIME": "STRFTIME({0}, {1}, 'unixepoch')", # solo mapear si los especificadores están disponibles
            "HOUR": r"CAST(STRFTIME('%H', {0}) AS INT)",
            "LAST_DAY": r"STRFTIME('%Y-%m-%d', {0}, '+1 month', 'start of month', '-1 day')",
            "MAKEDATE": "DATE('{0}-01-01', '+{1} day', '-1 day')",
            "MAKETIME": "TIME('00:00:00', '+{0} hours', '+{1} minutes', '+{2} seconds')",
            "MICROSECOND": "CAST(substr({0}, instr({0}, '.') + 1) AS INT)",
            "MINUTE": r"CAST(STRFTIME('%M', {0}) AS INT)",
            "MONTH": r"CAST(STRFTIME('%m', {0}) AS INT)",
            "MONTHNAME": "SELECT CASE WHEN STRFTIME('%m', {0}) = '01' THEN 'January' WHEN STRFTIME('%m', {0}) = '02' THEN 'February' WHEN STRFTIME('%m', {0}) = '03' THEN 'March' WHEN STRFTIME('%m', {0}) = '04' THEN 'April' WHEN STRFTIME('%m', {0}) = '05' THEN 'May' WHEN STRFTIME('%m', {0}) = '06' THEN 'June' WHEN STRFTIME('%m', '{0}) = '07' THEN 'July' WHEN STRFTIME('%m', {0}) = '08' THEN 'August' WHEN STRFTIME('%m', {0}) = '09' THEN 'September' WHEN STRFTIME('%m', {0}) = '10' THEN 'October' WHEN STRFTIME('%m', {0}) = '11' THEN 'November' WHEN STRFTIME('%m', {0}) = '12' THEN 'December' END",
            "QUARTER": "SELECT CASE	WHEN STRFTIME('%m', {0}) IN ('01', '02', '03') THEN 1 WHEN STRFTIME('%m', {0}) IN ('04', '05', '06') THEN 2	WHEN STRFTIME('%m', {0}) IN ('07', '08', '09') THEN 3 WHEN STRFTIME('%m', {0}) IN ('10', '11', '12') THEN 4 END",
            "SECOND": r"CAST(STRFTIME('%S', {0}) AS INT)",
            "SUBTIME": "DATETIME({0}, {date_modifiers_concatenated})",
            "TIMESTAMP": "DATETIME({0}, {datetime_modifiers_concatenated})",
            "TIMESTAMPADD": "DATETIME({0}, {datetime_modifiers_concatenated})",
            "TIMESTAMPDIFF": "SELECT (strftime('%Y', {0}) - strftime('%Y', {0})) * {weight0} + (strftime('%m', {1}) - strftime('%m', {1})) * {weight1}; OR SELECT (julianday({0}) - julianday({1})) / {timeConstant};",
            "TIME_TO_SEC": "SELECT strftime('%H', {0}) * 3600 + strftime('%M', {0}) * 60 + strftime('%f', {0})",
            "UTC_DATE": r"SELECT CAST(strftime('%Y%m%d', DATE('now')) AS INT) OR SELECT DATE('now')",
            "UTC_TIME": r"SELECT CAST(strftime('%H%M%s', TIME('now')) AS INT) OR SELECT DATE('now')",
        }
    }

@app.task
def convert_table_create_statement_to_sqlite3(createStatement: str, originDBType: Literal["mysql", "postgresql", "mongodb"], tableName: str = None):   
          
    # Adaptar cuestiones sintácticas básicas (caracteres para delimitar literales, información adicional innecesaria, etc)
    syntaxSubstitutions = get_syntax_substitutions(originDBType, "sqlite3", {"tableName": tableName})

    # Verificar qué funciones escalares y agregadas tienen su equivalente (y mapearlas) y cuáles deben quitarse del statement
    nativelyMappedFunctions = get_natively_mapped_functions(originDBType, "sqlite3", {})
    
    for pattern, substitution in syntaxSubstitutions.items():
        createStatement = sub(pattern, substitution, createStatement)
    
    for originalName, destinationName in nativelyMappedFunctions.items():
        createStatement = sub(originalName, destinationName, createStatement)
    
    return createStatement
        
@app.task
def convertSQLtoNoSQL(tableData, originType, destinationType):
    pass

@app.task
def convertNoSQLtoSQL(data, originType, destinationType):
    pass

if __name__ == "__main__":
    app.start()
