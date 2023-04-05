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
