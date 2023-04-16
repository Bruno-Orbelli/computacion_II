from celery import Celery
from typing import Literal, Any
from re import findall, match, search, split, sub
from anytree import Node, LevelOrderIter

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
                "CHAR": "char",
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
            "MONTHNAME": "CASE WHEN STRFTIME('%m', {0}) = '01' THEN 'January' WHEN STRFTIME('%m', {0}) = '02' THEN 'February' WHEN STRFTIME('%m', {0}) = '03' THEN 'March' WHEN STRFTIME('%m', {0}) = '04' THEN 'April' WHEN STRFTIME('%m', {0}) = '05' THEN 'May' WHEN STRFTIME('%m', {0}) = '06' THEN 'June' WHEN STRFTIME('%m', '{0}) = '07' THEN 'July' WHEN STRFTIME('%m', {0}) = '08' THEN 'August' WHEN STRFTIME('%m', {0}) = '09' THEN 'September' WHEN STRFTIME('%m', {0}) = '10' THEN 'October' WHEN STRFTIME('%m', {0}) = '11' THEN 'November' WHEN STRFTIME('%m', {0}) = '12' THEN 'December' END",
            "QUARTER": "CASE WHEN STRFTIME('%m', {0}) IN ('01', '02', '03') THEN 1 WHEN STRFTIME('%m', {0}) IN ('04', '05', '06') THEN 2 WHEN STRFTIME('%m', {0}) IN ('07', '08', '09') THEN 3 WHEN STRFTIME('%m', {0}) IN ('10', '11', '12') THEN 4 END",
            "SECOND": r"CAST(STRFTIME('%S', {0}) AS INT)",
            "SUBTIME": "DATETIME({0}, {date_modifiers_concatenated})",
            "TIMESTAMP": "DATETIME({0}, {datetime_modifiers_concatenated})",
            "TIMESTAMPADD": "DATETIME({0}, {datetime_modifiers_concatenated})",
            "TIMESTAMPDIFF": "(strftime('%Y', {0}) - strftime('%Y', {0})) * {weight0} + (strftime('%m', {1}) - strftime('%m', {1})) * {weight1}; OR (julianday({0}) - julianday({1})) / {timeConstant};",
            "TIME_TO_SEC": "strftime('%H', {0}) * 3600 + strftime('%M', {0}) * 60 + strftime('%f', {0})",
            "UTC_DATE": r"CAST(strftime('%Y%m%d', DATE('now')) AS INT) OR DATE('now')",
            "UTC_TIME": r"CAST(strftime('%H%M%S', TIME('now')) AS INT) OR TIME('now')",
            "UTC_TIMESTAMP": r"CAST(strftime('%Y%m%d%H%M%S', DATETIME('now')) AS INT) OR DATETIME('now')",
            "WEEKDAY": "CASE WHEN strftime('%w', {0}, 'localtime') - 1 == -1 THEN 6 ELSE strftime('%w', {0}, 'localtime') - 1 END",
            "YEAR": r"CAST(STRFTIME('%Y', {0}) AS INT)",
            "BIN": "WITH RECURSIVE cnt(x, y) AS (SELECT CAST({0} AS INTEGER), '' UNION ALL SELECT x / 2, CAST((x % 2) AS TEXT) || y	FROM cnt WHERE x > 0) SELECT ltrim(cnt.y, '0') AS binary_string FROM cnt ORDER BY length(binary_string) DESC LIMIT 1",
            "BIT_LENGTH": "LENGTH(HEX({0})) * 4",
            "CONCAT": "{argsWithConcOperator}",
            "CONCAT_WS": "group_concat(mockGroup, {0}) FROM ({selectWithAllItems})",
            "FIELD": "WITH RECURSIVE indexes(idx, str) AS (SELECT 0, {stringWithOneExtraDelim} UNION ALL SELECT idx + INSTR(str, ','), SUBSTR(str, INSTR(str, ',') + 1) FROM indexes WHERE str LIKE '%,%') SELECT COUNT(*) FROM indexes AS elem_index WHERE idx > 0 AND idx < instr({stringWithOneExtraDelim}, {0})",
            "FIND_IN_SET": "WITH RECURSIVE indexes(idx, str) AS (SELECT 0, {1} UNION ALL SELECT idx + INSTR(str, ','), SUBSTR(str, INSTR(str, ',') + 1) FROM indexes WHERE str LIKE '%,%') SELECT COUNT(*) FROM indexes AS elem_index WHERE idx > 0 AND idx < instr({1}, {0})",
            "HEX": r"HEX({0}) OR PRINTF('%x', {0})",
            "INSERT": "SUBSTR({0}, 1, {1} - 1) || {3} || SUBSTR({0}, {1} + {2})",
            "LEFT": "SUBSTR({0}, 1, {1})",
            "LOCATE": "CASE INSTR(substr({0}, {2}), {1}) WHEN 0 THEN 0 ELSE INSTR(substr({0}, {2}), {1}) + {2} - 1 END",
            "POSITION": "CASE INSTR(substr({0}, {2}), {1}) WHEN 0 THEN 0 ELSE INSTR(substr({0}, {2}), {1}) + {2} - 1 END", #adaptar notación in
            "LPAD": "WITH RECURSIVE lpad_helper(value, pad_char, len) AS (SELECT {0}, {2}, {1} UNION ALL SELECT REPLACE(pad_char || value, '  ', pad_char), pad_char, len - length(pad_char) FROM lpad_helper WHERE len > 0) SELECT substr(value, length(value) - {1} - 1)  FROM lpad_helper WHERE len = 0 AND length(value) >= 5",
            "MID": "SUBSTRING({0}, {1}, {2})",  # adaptar notación FOR/FROM
            "SUBSTRING": "SUBSTRING({0}, {1}, {2})",  # adaptar notación FOR/FROM
            "SUBSTR": "SUBSTRING({0}, {1}, {2})",  # adaptar notación FOR/FROM
            "OCT": r"PRINTF('%o', {0})",
            "QUOTE": r'"\'" || REPLACE(str, "\'", "\'") || "\'"',
            "REPEAT": "WITH RECURSIVE repeat(out, input, repetitions) AS (SELECT '', {0}, {1} UNION ALL SELECT out || input, input, repetitions - 1 FROM repeat WHERE repetitions > 0) SELECT out from repeat LIMIT 1 OFFSET {1}",
            "REVERSE": "WITH RECURSIVE inverse_string(original, inversed) AS (SELECT {0}, '' UNION ALL SELECT substr(original, 1, length(original) - 1), inversed || substr(original, length(original)) FROM inverse_string WHERE length(original) > 0) SELECT inversed FROM inverse_string ORDER BY inversed DESC LIMIT 1",
            "RPAD": "WITH RECURSIVE rpad_helper(value, pad_char, len) AS (SELECT {0}, {2}, {1} UNION ALL SELECT REPLACE(value || pad_char, '  ', pad_char), pad_char, len - length(pad_char) FROM rpad_helper WHERE len > 0) SELECT substr(value, 1, {1}) FROM rpad_helper WHERE len = 0 AND length(value) >= 5",
            "RIGHT": "SUBSTR({0}, {lengthString} - {amountOfChars} - 1)",
            "SPACE": "WITH RECURSIVE space(out, amount) AS (SELECT '', {0} UNION SELECT out || ' ', amount - 1 FROM space WHERE amount > 0) SELECT out FROM space ORDER BY out DESC LIMIT 1",
            "SUBSTRING_INDEX": ("WITH RECURSIVE split(str_with_extra_delimiter, delimiter, cnt, result) AS (SELECT '{0}{1}', '{1}', 0, '' UNION ALL SELECT CASE	WHEN cnt = 0 THEN str_with_extra_delimiter ELSE substr(str_with_extra_delimiter, instr(str_with_extra_delimiter, delimiter) + 1) END, delimiter, cnt + 1, result || CASE WHEN cnt = 0 THEN '' ELSE substr(str_with_extra_delimiter, 0, instr(str_with_extra_delimiter, delimiter)) || delimiter END FROM split WHERE cnt < {2} + 1 AND instr(str_with_extra_delimiter, delimiter) > 0) SELECT substr(result, 1, length(result) - 1) AS result FROM split ORDER BY result DESC LIMIT 1",
                                "WITH RECURSIVE inverse_split(str, delimiter, cnt) AS (SELECT '{0}', '{1}', 0 UNION ALL SELECT substr(str, instr(str, delimiter) + 1), delimiter, cnt - 1 FROM inverse_split	WHERE cnt > -99999999 AND instr(str, delimiter) > 0) SELECT CASE WHEN ((SELECT str FROM inverse_split ORDER BY length(str) ASC LIMIT 1 OFFSET {2} - 1) IS NULL) THEN (SELECT str FROM inverse_split ORDER BY length(str) DESC LIMIT 1) ELSE (SELECT str FROM inverse_split ORDER BY length(str) ASC LIMIT 1 OFFSET {2} - 1) END"
                                ),
            "TRIM": "RTRIM(LTRIM({0}, {1}), {2})",
            "UNHEX": "CAST(X'{0}' AS TEXT)",
            "BINARY": "CAST({0} AS BINARY)",
            "CONVERT": "CAST({0} AS {1})",  # en caso de using, no disponible
            "~": "~{0} + 18446744073709551616",
            "BIT_COUNT": "WITH RECURSIVE bit_cnt(bitNum, cnt) AS (SELECT (WITH RECURSIVE bin(x, y) AS (SELECT CAST({0} AS INTEGER), '' UNION ALL SELECT x / 2, CAST((x % 2) AS TEXT) || y FROM bin WHERE x > 0) SELECT ltrim(bin.y, '0') AS binary_string FROM bin ORDER BY length(binary_string) DESC LIMIT 1), 0	UNION ALL SELECT substr(bitNum, 2),	CASE WHEN substr(bitNum, 1, 1) = '1' THEN cnt + 1 ELSE cnt END FROM bit_cnt	WHERE length(bitNum) > 0) SELECT MAX(cnt) FROM bit_cnt",
            "DATABASE": "WITH RECURSIVE get_db_name(path, amount_of_previous_char) AS (SELECT (SELECT file FROM pragma_database_list), 1 UNION ALL SELECT substr(path, instr(path, '/') + 1), instr(path, '/') FROM get_db_name WHERE amount_of_previous_char > 0) SELECT substr(path, 0, instr(path, '.')) FROM get_db_name WHERE amount_of_previous_char = 0",
            "SCHEMA": "WITH RECURSIVE get_db_name(path, amount_of_previous_char) AS (SELECT (SELECT file FROM pragma_database_list), 1 UNION ALL SELECT substr(path, instr(path, '/') + 1), instr(path, '/') FROM get_db_name WHERE amount_of_previous_char > 0) SELECT substr(path, 0, instr(path, '.')) FROM get_db_name WHERE amount_of_previous_char = 0"
        }
    }
    
    for i, argument in enumerate(funcArgs):
        funcName = match(r"(\w+)(\(| )", argument).group(1)
        
        if funcName in nonNativelyMappedFunctions[(originDBType, destinationDBType)]:
            funcArgs[i] = adapt_non_natively_mapped_functions(funcName, extract_arguments(argument), originDBType, destinationDBType)
        
    return nonNativelyMappedFunctions[(originDBType, destinationDBType)][originalFuncName].format(*funcArgs)

def build_statement_functions_tree(rootStatement: 'str') -> Node:
    rootStatement = rootStatement.replace(" ", "")
    root = Node('root', args= rootStatement, maxReachedLevel= 999)
    nodesToProcess = [root]
    
    while nodesToProcess:
        for parentNode in nodesToProcess:
            if parentNode.maxReachedLevel > 1:
                build_function_and_argument_nodes(parentNode.args, parentNode)
        nodesToProcess = [nodeChild for node in nodesToProcess for nodeChild in node.children]
    
    return root

def build_function_and_argument_nodes(funcString: str, parentNode: Node) -> None:
    funcs, args = [], []
    auxArgsList = []
    tokenStr = ""
    level, maxReachedLevel = 0, 0
    
    for char in funcString:
        if level >= maxReachedLevel:
            maxReachedLevel = level
        
        if char == '(':
            if level == 0:
                funcs.append([tokenStr])
                tokenStr = ""
            
            level += 1
            
            if level == 1:
                continue
        
        elif char == ')':
            level -= 1
            if level == 0:
                auxArgsList.append(tokenStr)
                args.append(auxArgsList)
                tokenStr, auxArgsList = "", []
                continue
        
        elif char == ',':
            if level == 1:
                auxArgsList.append(tokenStr)
                tokenStr = ""
                continue

            elif level == 0:
                tokenStr = ""
                continue
        
        tokenStr += char
        
    i = 0
    for func, argList in zip(funcs, args):
        newNode = Node(name= f'{parentNode.name}{i}', parent= parentNode, function= func[0], args= ','.join(arg for arg in argList), maxReachedLevel= maxReachedLevel, adaptation=None)
        i += 1

def divide_statement_in_functions(funcString: str):
    pass
    

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

    funcTree = build_statement_functions_tree(createStatement)
    for funcNode in reversed([node for node in LevelOrderIter(funcTree)]):
        funcNode.adaptation = 

    return createStatement
        
@app.task
def convertSQLtoNoSQL(tableData, originType, destinationType):
    pass

@app.task
def convertNoSQLtoSQL(data, originType, destinationType):
    pass

if __name__ == "__main__":
    
    #build_statement_functions_tree('ATAN(ATAN(3, ATAN(2, ATAN(2,ATAN(3, 2)))), LOG(2)) + COS(COS(3))')
    #print(extract_arguments_and_functions('ATAN(ATAN(3, ATAN(2, 3)), LOG(2)) COS(COS(3))'))
    app.start()
