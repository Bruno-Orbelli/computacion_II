from ast import literal_eval
from os import getcwd
from dotenv import load_dotenv
from os import getenv
from os.path import dirname
from string import punctuation
from sys import path
from celery import Celery
from typing import Literal
from re import fullmatch, sub
from anytree import Node, LevelOrderGroupIter
from decimal import Decimal
import datetime

baseDir = dirname(getcwd())
try:
    path.index(baseDir)
except ValueError:
    path.append(baseDir)

from common.wrappers import time_excecution
from common.exceptions import InitializationError

load_dotenv()
brokerIp = ("BROKER_ADDRESS", getenv("BROKER_ADDRESS"))
brokerPort = ("BROKER_PORT", getenv("BROKER_PORT"))

if None in (brokerIp[1], brokerPort[1]):
    envVars = (brokerIp, brokerPort)
    envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
    raise InitializationError(
        f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
        )

brokerIp = brokerIp[1].replace('"', '')
brokerPort = int(brokerPort[1])

app = Celery("funcs", 
             broker=f"redis://{brokerIp}", 
             backend=f"redis://{brokerIp}:{brokerPort}", 
             task_serializer='json',
             result_serializer='json',
             accept_content = ['application/json']
            )

# Probar procesamiento con unidades de diferentes tamaños (diferentes chunks)

def get_syntax_substitutions(originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], params: 'dict[str, str]'):
    subsDict = {
        ("mysql", "sqlite3") : {
            r'(?<!FOREIGN|PRIMARY) KEY .+,\n': "",
            r'ENGINE=.+': "",
            r'(\s|,)\`': r"\1[",
            r'\`(\s|,)': r"]\1",
            r'\(\`': "([",
            r'\`\)': "])",
            r' DEFAULT ([^ \t\n\r\f\v,]+)([\s,])': r' DEFAULT(\1)\2',
            r' UNIQUE KEY \[\w+\] (\(\[\w+\]\))': r' UNIQUE\1'
        },
        ("postgresql", "sqlite3"): {
            r'CREATE TABLE .*?\..*? \(': f"CREATE TABLE [{params['tableName']}] (",
            r'\\n    (?!ADD CONSTRAINT)': "\\n    [",
            r'(\n    \[[\w\s]*?) ': r"\1] ",
            r'"': ""
        }
    }
    return subsDict[(originDBType, destinationDBType)]

def get_natively_mapped_functions(originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"]):
    nativelyMappedFunctions = {
        ("mysql", "sqlite3") : {
            "ABS": "ABS({0})",
            "AVG": "AVG({0})",
            "BIGINT": "BIGINT({0})",
            "BIT": "BIT({0})",
            "BLOB": "BLOB({0})",
            "CAST": "CAST({0} AS {1})",
            "CHAR": "CHAR({0})",
            "COUNT": "COUNT({0})",
            "DATE": ("DATE({0})", "DATE"),
            "DATETIME": "DATETIME({0})",
            "FLOAT": ("FLOAT({0}, {1})", "FLOAT({0})"),
            "IFNULL": "IFNULL({0}, {1})",
            "INSTR": "INSTR({0}, {1})",
            "INT": "INT({0})",
            "INTEGER": "INTEGER{0}",
            "LENGTH": "LENGTH({0})",
            "LOWER": "LOWER({0})",
            "LTRIM": "LTRIM({0})",
            "NULLIF": "NULLIF({0}, {1})",
            "JSON_ARRAY": "JSON_ARRAY({concatOfArgs})",
            "MAX": "MAX({0})",
            "MEDIUMINT": "MEDIUMINT({0})",
            "MIN": "MIN({0})",
            "REGEXP": "{0} REGEXP {1}",
            "REPLACE": "REPLACE({0}, {1}, {2})",
            "ROUND": "ROUND({0})",
            "RTRIM": "RTRIM({0})",
            "SET": "TEXT",
            "SIGN": "SIGN({0})",
            "SMALLINT": "SMALLINT({0})",
            "SUM": "SUM({0})",
            "TEXT": "TEXT({0})",
            "TIME": "TIME({0})",
            "TINYINT": "TINYINT({0})",
            "UPPER": "UPPER({0})",
            "ASCII": "UNICODE({0})",
            "CHAR_LENGTH": "LENGTH({0})",
            "CHARACTER_LENGTH": "LENGTH({0})",
            "DEC": "DEC({0}, {1})",
            "DECIMAL": "DECIMAL({0}, {1})",
            "DIV": "{0} / {1}",
            "DOUBLE": "DOUBLE({0}, {1})",
            "LAST_INSERT_ID": "LAST_INSERT_ROWID()",
            "LCASE": "LOWER({0})",
            "JSON_ARRAYAGG": "JSON_GROUP_ARRAY({0})",
            "JSON_OBJECTAGG": "JSON_GROUP_OBJECT({0}, {1})",
            "OCTET_LENGTH": "LENGTH({0})",
            "RLIKE": "{0} REGEXP {1}",
            "ROW_COUNT": "CHANGES()",
            "UCASE": "UPPER({0})",
            "VARBINARY": "VARBINARY({0})",
            "VARCHAR": "VARCHAR({0})",
            "VERSION": "SQLITE_VERSION()",
            "ACOS": "ACOS({0})",
            "ASIN": "ASIN({0})",
            "ATAN2": "ATAN2({0}, {1})",
            "CEIL": "CEIL({0})",
            "CEILING": "CEILING({0})",
            "COS": "COS({0})",
            "DEGREES": "DEGREES({0})",
            "EXP": "EXP({0})",
            "FLOOR": "FLOOR({0})",
            "LN": "LN({0})",
            "LOG2": "LOG2({0})",
            "LOG10": "LOG10({0})",
            "MOD": "{0} % {1}",
            "PI": "PI()",
            "POW": "POW({0}, {1})",
            "POWER": "POWER({0}, {1})",
            "RADIANS": "RADIANS({0})",
            "SIN": "SIN({0})",
            "SQRT": "SQRT({0})",
            "TAN": "TAN({0})"
        }
    }

    return nativelyMappedFunctions[(originDBType, destinationDBType)]

def get_non_natively_mapped_functions(originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"]) -> 'dict[str, str]':
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
            # "MEMBER OF": "'{0}' IN (SELECT value FROM json_each({1})",
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
            "DECIMAL": "CAST({0} AS NUMERIC)",
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
            "ENUM": "TEXT",
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
            # "BINARY": "CAST({0} AS BINARY)", maldito mysql
            "CONVERT": "CAST({0} AS {1})",  # en caso de using, no disponible
            "~": "~{0} + 18446744073709551616",
            "BIT_COUNT": "WITH RECURSIVE bit_cnt(bitNum, cnt) AS (SELECT (WITH RECURSIVE bin(x, y) AS (SELECT CAST({0} AS INTEGER), '' UNION ALL SELECT x / 2, CAST((x % 2) AS TEXT) || y FROM bin WHERE x > 0) SELECT ltrim(bin.y, '0') AS binary_string FROM bin ORDER BY length(binary_string) DESC LIMIT 1), 0	UNION ALL SELECT substr(bitNum, 2),	CASE WHEN substr(bitNum, 1, 1) = '1' THEN cnt + 1 ELSE cnt END FROM bit_cnt	WHERE length(bitNum) > 0) SELECT MAX(cnt) FROM bit_cnt",
            "DATABASE": "WITH RECURSIVE get_db_name(path, amount_of_previous_char) AS (SELECT (SELECT file FROM pragma_database_list), 1 UNION ALL SELECT substr(path, instr(path, '/') + 1), instr(path, '/') FROM get_db_name WHERE amount_of_previous_char > 0) SELECT substr(path, 0, instr(path, '.')) FROM get_db_name WHERE amount_of_previous_char = 0",
            "SCHEMA": "WITH RECURSIVE get_db_name(path, amount_of_previous_char) AS (SELECT (SELECT file FROM pragma_database_list), 1 UNION ALL SELECT substr(path, instr(path, '/') + 1), instr(path, '/') FROM get_db_name WHERE amount_of_previous_char > 0) SELECT substr(path, 0, instr(path, '.')) FROM get_db_name WHERE amount_of_previous_char = 0"
        }
    }

    return nonNativelyMappedFunctions[(originDBType, destinationDBType)]
           
def map_native_and_non_native_functions(originalFuncName: str, funcArgs: 'list[str]', originDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"], destinationDBType: Literal["mysql", "sqlite3", "postgresql", "mongodb"]):
    nativelyMappedFunctions = get_natively_mapped_functions(originDBType, destinationDBType)
    nonNativelyMappedFunctions = get_non_natively_mapped_functions(originDBType, destinationDBType)
   
    charsToRemove = punctuation.replace("_", "")
    pattern = f"[{'|'.join(char for char in charsToRemove)}]"
    correctedFunctionName = sub(r'{0}'.format(pattern), "", originalFuncName).upper()
        
    if correctedFunctionName in nativelyMappedFunctions:
        return nativelyMappedFunctions[correctedFunctionName].format(*funcArgs)
    
    elif correctedFunctionName in nonNativelyMappedFunctions:
        mappedFunction = nonNativelyMappedFunctions[correctedFunctionName]
        if isinstance(mappedFunction, tuple):
            try:
                return mappedFunction[0].format(*funcArgs)
            except IndexError:
                return mappedFunction[1].format(*funcArgs)
        
        else:
            return mappedFunction.format(*funcArgs)
    
    else:
        return 'NULL'

def build_statement_functions_tree(rootStatement: 'str') -> Node:
    root = Node('root', oldArgs= rootStatement.strip(), newArgs= rootStatement.strip(), maxReachedLevel= 999)
    nodesToProcess = [root]
    
    while nodesToProcess:
        for parentNode in nodesToProcess:
            if parentNode.maxReachedLevel > 1:
                build_function_and_argument_nodes(parentNode.oldArgs, parentNode)
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
            if level == 0 and tokenStr[-1] != " ":
                funcs.append([tokenStr])
                tokenStr = ""
            
            elif tokenStr[-1] == " ":
                tokenStr = ""
                continue
            
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

        elif char == ';':
                tokenStr = ""
                continue
        
        elif fullmatch(r'\W', char) and char not in (" ", ".", "'"):
            continue

        tokenStr += char
        
    i = 0
    for func, argList in zip(funcs, args):
        correctedFunc = func[0].split(" ")[-1].strip()
        newNode = Node(name= f'{i}', parent= parentNode, function= correctedFunc, oldArgs= ';'.join(arg.strip() for arg in argList), newArgs= ';'.join(arg.strip() for arg in argList), maxReachedLevel= maxReachedLevel, adaptation=None)
        i += 1

def adapt_functions_tree_to_sqlite3(funcTree: Node, originDBType: Literal["mysql", "postgresql", "mongodb"]) -> Node:
    for sameLevelNodes in reversed([[node for node in children] for children in LevelOrderGroupIter(funcTree)]):
        for funcNode in sameLevelNodes:
            if funcNode.name != 'root':
                funcNode.adaptation = map_native_and_non_native_functions(funcNode.function, funcNode.newArgs.split(";"), originDBType, "sqlite3")
                parentArgs = funcNode.parent.newArgs.split(";")
                
                for i, arg in enumerate(parentArgs):
                    if arg in (f"{funcNode.function}({funcNode.oldArgs.replace(';', ',')})", f"{funcNode.function}('{funcNode.oldArgs.replace(';', ',')}')"):
                        parentArgs[i] = funcNode.adaptation
                
                funcNode.parent.newArgs = ';'.join(arg for arg in parentArgs)
            
            else:
                for child in funcNode.children:
                    childOldArgs = f"{child.oldArgs.replace(';', ',')}"
                    funcNode.newArgs = funcNode.newArgs.replace(f"{child.function}({childOldArgs})", 
                                                                f"{child.adaptation}")
        
    return funcTree

@app.task
@time_excecution
def convert_table_create_statement_to_sqlite3(createStatement: str, originDBType: Literal["mysql", "postgresql", "mongodb"], tableName: str, requestParams: 'dict[str, str]'):
    # Adaptar cuestiones sintácticas básicas (caracteres para delimitar literales, información adicional innecesaria, etc)
    syntaxSubstitutions = get_syntax_substitutions(originDBType, "sqlite3", {"tableName": tableName})
    
    for pattern, substitution in syntaxSubstitutions.items():
        createStatement = sub(pattern, substitution, createStatement)
    
    # Verificar qué funciones escalares y agregadas tienen su equivalente (y mapearlas) y cuáles deben quitarse del statement
    funcTree = adapt_functions_tree_to_sqlite3(build_statement_functions_tree(createStatement), originDBType)

    requestParams.update({"originDbType": originDBType})
    createStatement = createStatement.replace(rf'{funcTree.oldArgs}', rf'{funcTree.newArgs}')

    return (createStatement, requestParams)
        
@app.task
@time_excecution
def create_table_insert_statement_for_sqlite3(tableRows: 'list[str]', originDBType: Literal["mysql", "postgresql", "mongodb"], tableName: str, requestParams: 'dict[str, str]'):    
    colNames = literal_eval(tableRows[0])
    
    if len(colNames) == 1:
        colNames = f"({colNames[0]})"
        correctedRowList = []
        
        for tableRow in tableRows[1::]:
            '''rowList = list(literal_eval(tableRow)[0])
            
            for i, value in enumerate(rowList):
                if isinstance(value, datetime.datetime):
                    rowList[i] = eval(value)'''
            
            correctedRowList.append(f"({literal_eval(tableRow)[0]})".replace("'NULL'", "NULL"))
            
        jointValues = ",".join(rowStr for rowStr in correctedRowList)
    
    else:
        for i, tableRow in enumerate(tableRows):
            if i != 0:
                aux = list(eval(tableRow))
                aux = [str(value) if any([isinstance(value, datetime.date), 
                                          isinstance(value, datetime.datetime), 
                                          isinstance(value, datetime.time), 
                                          isinstance(value, Decimal)]) else value for value in aux]
                tableRows[i] = str(tuple(aux))
        
        jointValues = ','.join(tableRow.replace("'NULL'", "NULL") for tableRow in tableRows[1::])

    insertStatement = f"INSERT INTO {tableName} {colNames} VALUES {jointValues};"

    requestParams.update({"originDbType": originDBType})
    
    return (insertStatement, requestParams)

if __name__ == "__main__":
    app.start()
