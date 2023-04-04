from celery import Celery
from typing import Literal
from re import sub

app = Celery("funcs", broker="redis://localhost", backend="redis://localhost:6379")

# Probar procesamiento con unidades de diferentes tamaños (diferentes chunks)

@app.task
def process_information():
    pass

@app.task
def convert_table_create_statement_to_sqlite3(createStatement: str, originDBType: Literal["mysql", "postgresql", "mongodb"], tableName: str):   
    
    if originDBType == "mysql":
         
        # Adaptar cuestiones sintácticas básicas (caracteres para delimitar literales, información adicional innecesaria, etc)
        
        syntaxSubstitutions = {
            r'(?<!FOREIGN|PRIMARY) KEY .+,\n': "",
            r'ENGINE=.+': "",
            r'\s\`': " [",
            r'\`\s': "] ",
            r'\(\`': "([",
            r'\`\)': "])"
        }

        # Verificar qué funciones escalares y agregadas tienen su equivalente (y mapearlas) y cuáles deben quitarse del statement

        nativelyMappedFunctions = {
            "ABS": "abs",
            "ASCII": "unicode",
            "AVG": "avg",
            "CAST": "cast",
            "CHAR_LENGTH": "length",
            "CHARACTER_LENGTH": "length",
            "COUNT": "count",
            "DATE": "date",
            "DIV": "/",
            "IFNULL": "ifnull",
            "INSTR": "instr",
            "LCASE": "lower",
            "LENGTH": "length",
            "LOWER": "lower",
            "LTRIM": "ltrim",
            "NULLIF": "nullif",
            "JSON_ARRAYAGG": "json_group_array",
            "JSON_OBJECTAGG": "json_group_object",
            "MAX": "max",
            "MIN": "min",
            "MOD": "%",
            "OCTET_LENGTH": "length",
            "REGEXP": "regexp",
            "REPLACE": "replace",
            "RLIKE": "regexp",
            "ROW_COUNT": "changes",
            "ROUND": "round",
            "RTRIM": "rtrim",
            "SIGN": "sign",
            "SUM": "sum",
            "UCASE": "upper",
            "UPPER": "upper",
        }

        mathMappedFunctions = {
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

        # Tener en cuenta el +0 para obtener versiones float de las fechas
        
        # No mappeado nativamente
        # "ATAN": "atan" (transformar doble parámetro en uno; con un solo parámetro, el mapeo es directo)
        # "CONV" "printf" (para bases comunes - octal, hexadecimal, etc. - es mapeable con printf; caso contrario, no está disponible)
        # "COT": "1 / tan" (mapearlo como la inversa de la tangente)
        # "CRC32" (no disponible)
        # "LOG": "ln" || "log" (si posee un solo argumento, el mapeo es directo con ln; caso contrario, el mapeo es directo con log(base, num))
        # "RAND": "random" (ignorar el argumento y mapear como RANDOM, operado adecuadamente para devolver dentro del mismo rango de valores)
        # "TRUNCATE": "printf"? (averiguar si es posible truncar con printf y después hacer cast a int/float)
        # "BIT_AND", "BIT_OR", "BIT_XOR" (no disponible)
        # "GROUP_CONCAT": "group_concat" (en el caso de especificar un separador propio, adaptar el parámetro SEPARATOR; caso contrario, el mapeo es directo)
        # "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP": (construir las expresiones equivalentes para el cálculo de la dev. estándar con las funciones de SQLite)
        # "VAR_POP", "VAR_SAMP", "VARIANCE": (construir las expresiones equivalentes para el cálculo de la varianza con las funciones de SQLite)
        # "MEMBER OF": "json_each" (obtener valores del array con JSON_EACH y verficar si el valor pasado como parámetro está dentro del resultado)
        # "SOUNDS_LIKE" (no disponible)
        # "/": (ajustar la división para que sea exacta)
        # "IF": "CASE" (mapear IF a la expresión CASE equivalente)
        # "ADDDATE", "DATE_ADD (ojo con el interval)": "date" (mapear a la función DATE con los modificadores correspondientes)
        # "ADDTIME": "datetime" (mapear a la función DATETIME con los modificadores correspondientes)
        # "CONVERT_TZ" (no disponible)?
        # "CURDATE", "CURRENT_DATE": "date" (ajustar parámetros para que sea tiempo local)
        # "CURTIME", "CURRENT_TIME": "time" (ídem a lo de arriba)
        # "CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP", "SYSDATE": "datetime" (ídem a lo de arriba)
        # "DATEDIFF": "julianday" - "julianday" (mapear como una diferencia entre objetos JULIANDAY y castear como int)
        # "DATE_FORMAT": "strftime" (formatear la fecha con STRFTIME, adaptando las sustituciones no disponibles)
        # "DATE_SUB", "SUBDATE (ojo con el interval)": "date" (mapear a la función DATE con los modificadores correspondientes)
        # "DAY", "DAY_OF_MONTH": strftime (formatear la fecha con STRFTIME para obtener el número del día correspondiente)
        # "DAYNAME": strftime + CASE (obtener el nº de día y devolver salida en función con CASE)
        # "DAYOFWEEK" : strftime + 1 (ídem que con DAY y DAYOFMONTH pero sumándole 1)
        # "DAYOFYEAR" : strftime (ídem que con DAY y DAYOFMONTH con la sustitución correspondiente)
        # "EXTRACT": strftime (extraer y formatear la parte de la fecha correspondiente)
        # "FROM_DAYS": date + num of days (tomar la fecha base y sumar la cantidad de días especificada en el parámetro con DATE, restando 1 año)
        # "FROM_UNIXTIME": datetime (pasar la cant. de segundos como parámetro con el modificador 'unixepoch')
        # "GET_FORMAT": strftime (utilizar un CASE para los diferentes formatos disponibles y devolver el str con las sustituciones correspondientes)
        # "HOUR": strftime (formatear con la sustitución de hora)
        # "LAST_DAY": strftime + datetime (obtener el último día del mes con DATETIME + modificadores y extraer la parte de la fecha con STRFTIME o DATE)
        # "MAKEDATE": date (utilizar el año para pasar la primera fecha y restarle 1 día al modificador)
        # "MAKETIME": time (concatenar los parámetros en formato de tiempo y construir con TIME)
        # "MICROSECOND": strftime + CAST (obtener la parte decimal de la fecha y operar para obtener microsegundos) (ADVERTENCIA: precisión de milisegundos, las cifras después de 10 ** -3 se truncan)
        # "MINUTE": strftime (formatear con la sustitución de minuto)
        # "MONTH": strftime (formatear con la sustitución de mes)
        # "MONTHNAME": strftime + CASE (extraer el número de mes y utilizar CASE para devolver el nombre correcto)
        # "PERIOD_ADD": strftime + date + CAST (construir la fecha, agregarle la cantidad de meses con el modificador y recastearlo como int)
        # "PERIOD_DIFF": strftime + (date - date) + CAST (construir las fechas, restarlas y recastear con formato adecuado)
        # "QUARTER": strftime + CASE (extraer el número de mes y utilizar CASE para devolver el número de cuatrimestre)
        # "SECOND": strftime (formatear con la sustitución de segundo)
        # "STR_TO_DATE" (no disponible)
        # "SUBTIME": "datetime" (mapear a la función DATETIME con los modificadores correspondientes)
        # "TIME": "strftime" (formatear con las sutituciones de tiempo correspondientes)
        # "TIMEDIFF": "time + (julianday - julianday) + CAST" (obtener la cantidad de tiempo transcurrido entre dos fechas con la diferencia de JULIANDAY, castear como string y sumarlo como un modificador al constructor de TIME)
        # "TIMESTAMP": "datetime" (crear objeto DATETIME con la fecha y sumar la cantidad de tiempo correspondiente con modificadores en caso de segundo argumento)
        # "TIMESTAMPADD": "datetime" (ídem a lo de arriba)
        # "TIMESTAMPDIFF": "julianday - julianday + CAST" (calcular la diferencia entre dos JULIANDAY y castear el resultado a int, expresado en la unidad correspondiente)
        # "TIME_FORMAT": "strftime" (formatear el tiempo con STRFTIME, adaptando las sustituciones no disponibles)
        # "TIME_TO_SEC": "strftime" * values (obtener las diferentes partes de la hora y multiplicarlas por el valor adecuado)
        # "TO_DAYS": "julianday - julianday + CAST" (calcular la diferencia entre el JULIANDAY de la fecha y el de 0000-01-01 y castear a int)
        # "TO_SECONDS": "julianday - julianday * 86400 + CAST" (ídem al de arriba pero multiplicando para obtener segundos)
        # "UTC_DATE": "date" (mapeo casi directo a DATE('now'); agregar conversión cuando se suma 0)
        # "UTC_TIME": "time" (ídem a lo de arriba)
        # "UTC_TIMESTAMP": "datetime" (ídem a lo de arriba)
        # "WEEK": "strftime" (WARNING: ajustar los modos, de no ser posible solo implementar modo 1)
        # "WEEKDAY": "strftime + CAST" (utilizar STRFTIME con la sustitución de semana)
        # "WEEKOFYEAR" (no disponible en principio)
        # "YEAR": "strftime + CAST" (utilizar STRFTIME con la sustitución de año)
        # "YEARWEEK" (no disponible en principio)
        # "BIN": """WITH RECURSIVE cnt(x, y) AS (SELECT CAST(num AS INTEGER), '' UNION ALL SELECT x / 2, CAST((x % 2) AS TEXT) || y	FROM cnt WHERE x > 0) SELECT ltrim(cnt.y, '0') AS binary_string FROM cnt ORDER BY length(binary_string) DESC LIMIT 1;"""
        # "BIT_LENGTH": "length(hex()) * 4" (obtener la longitud del hexadecimal del string y multiplicarlo por 4)
        # "CHAR": "0x || hex(char())" (en su forma original en MySQL, es una combinación de hex y chat; con la palabra reservada USING, dependiendo del conjunto de chars, el mapeo es directo)
        # "CONCAT": "||" (utilizar el operador de concatenación y adaptar los parámetros apropiadamente)
        # "CONCAT_WS": "group_concat" (enunciar los elementos como parte de un grupo y especificar separador)
        # "ELT" (analizar si es posible)
        # "EXPORT_SET" (no disponible)
        # "FIELD": """WITH RECURSIVE indexes(idx, str) AS (SELECT 0, ',string1,string2,string3,string2' UNION ALL SELECT idx + INSTR(str, ','), SUBSTR(str, INSTR(str, ',') + 1) FROM indexes WHERE str LIKE '%,%') SELECT COUNT(*) + 1 FROM indexes AS elem_index WHERE idx > 0 AND idx < instr(',string1,string2,string3,string2', 'string2');"""
        # "FIND_IN_SET": (misma implementación que arriba, no hace falta construir la strlist previamente)
        # "FORMAT" (number): (mapearlo como un PRINTF o FORMAT concatenado con las cifras decimales en otro FORMAT)
        # "FROM_BASE64" (no disponible)
        # "HEX": "hex" || "printf" (si el argumento es str, el mapeo es directo; sino, utilizar PRINTF("%x", number))
        # "INSERT": """SELECT substr('original_string', 1, position - 1) || 'insert_string' || substr('original_string', position + length) AS new_string"""
        # "LEFT": "substr" (utilizar el index provisto para extraer los primeros n caracteres del string con SUBSTR)
        # "LOAD_FILE" (no disponible)
        # "LOCATE", "POSITION" (adaptar la notación IN para este último): """SELECT CASE INSTR(substr('foobarbar', starting_pos), 'bar')	WHEN 0 THEN 0 ELSE INSTR(substr('foobarbar', starting_pos), 'bar') + starting_pos - 1 END"""
        # "LPAD": """WITH RECURSIVE lpad_helper(value, pad_char, len) AS (SELECT 'hola', '%', 36 UNION ALL SELECT REPLACE(pad_char || value, '  ', pad_char), pad_char, len - length(pad_char) FROM lpad_helper WHERE len > 0) SELECT substr(value, 1, 40) FROM lpad_helper WHERE len = 0 AND length(value) >= 5;"""
        # "MAKE_SET" (en principio no disponible)
        # "MID", "SUBSTR", "SUBSTRING": "substr" (el mapeo es casi directo, adaptando la sintaxis en el caso de la notación FROM FOR)
        # "OCT": "printf" (mapeo directo con la sustitución de octal activada)
        # "ORD": "unicode" (si el caracter es de un solo byte, caso contrario, no disponible)
        # "RPAD": """WITH RECURSIVE lpad_helper(value, pad_char, len) AS (SELECT '', ' ', 5 UNION ALL SELECT REPLACE(value || pad_char, '  ', pad_char), pad_char, len - length(pad_char) FROM lpad_helper WHERE len > 0) SELECT substr(value, 1, 5) FROM lpad_helper WHERE len = 0 AND length(value) >= 5;"""
        # "QUOTE": """SELECT "'" || REPLACE(str, "'", "\'") || "'""""
        # "REPEAT": """WITH RECURSIVE repeat(out, input, repetitions) AS (SELECT '', 'ogString', 4 UNION ALL SELECT out || input, input, repetitions - 1 FROM repeat WHERE repetitions > 0) SELECT out from repeat LIMIT 1 OFFSET 4"""
        # "REVERSE": """WITH RECURSIVE inverse_string(original, inversed) AS (SELECT 'str', '' UNION ALL SELECT substr(original, 1, length(original) - 1), inversed || substr(original, length(original)) FROM inverse_string WHERE length(original) > 0) SELECT inversed FROM inverse_string ORDER BY inversed DESC LIMIT 1"""
        # "RIGHT": "substr" (contar la longitud del string para extraer los caracteres de la derecha)
        # "SOUNDEX" (no disponible)
        # "SPACE": """WITH RECURSIVE space(out, amount) AS (SELECT '', 10 UNION	SELECT out || ' ', amount - 1 FROM space WHERE amount > 0) SELECT out FROM space ORDER BY out DESC LIMIT 1"""
        # "SUBSTRING_INDEX": """WITH RECURSIVE split(str_with_extra_delimiter, delimiter, cnt, result) AS (SELECT '{string}', '{delimiter}', 0, '' UNION ALL	SELECT CASE	WHEN cnt = 0 THEN str ELSE substr(str, instr(str, delimiter) + 1) END, delimiter, cnt + 1, result || CASE WHEN cnt = 0 THEN '' ELSE substr(str, 0, instr(str, delimiter)) || delimiter END FROM split WHERE cnt < {amount_of_delimiters + 1} AND instr(str, delimiter) > 0) SELECT substr(result, 1, length(result) - 1) AS result FROM split ORDER BY result DESC LIMIT 1"""
                             # """ WITH RECURSIVE inverse_split(str, delimiter, cnt) AS (SELECT '{string}', '{delimiter}', 0 UNION ALL SELECT substr(str, instr(str, delimiter) + 1), delimiter, cnt - 1 FROM inverse_split	WHERE cnt > -99999999 AND instr(str, delimiter) > 0) SELECT CASE WHEN ((SELECT str FROM inverse_split ORDER BY length(str) ASC LIMIT 1 OFFSET {amount_of_delimiters - 1}) IS NULL) THEN (SELECT str FROM inverse_split ORDER BY length(str) DESC LIMIT 1) ELSE (SELECT str FROM inverse_split ORDER BY length(str) ASC LIMIT 1 OFFSET {amount_of_delimiters - 1}) END AS result"""
        # "TO_BASE64" (no disponible)
        # "TRIM": "rtrim + ltrim" (dependiendo de la palabra reservada, mappear como RTRIM, LTRIM o una combinación de ambas)
        # "UNHEX": """SELECT CAST(X'hex_string' AS TEXT);"""
        # "WEIGHT_STRING" (no disponible)
        # "REGEXP_LIKE": (mapear al operador REGEXP con las conversiones de parámetro apropiadas)
        # "REGEXP_INSTR", "REGEXP_REPLACE", "REGEXP_SUBSTR" (no disponible)
        # "MATCH" (no disponible)
        # "BINARY": "cast" (mapearlo como un casting a tipo binario)
        # "CONVERT": "cast" (mapeo directo, adaptando las palabras reservadas a las de CAST)
        # XMLFUNCTIONS (no disponibles)
        # "~": "SELECT (~ num + 18446744073709551616)"
        # "XOR", "^": (implementar con operadores existentes)
        # "WITH RECURSIVE bit_cnt(bitNum, cnt) AS (SELECT (WITH RECURSIVE bin(x, y) AS (SELECT CAST(121 AS INTEGER), '' UNION ALL SELECT x / 2, CAST((x % 2) AS TEXT) || y FROM bin WHERE x > 0) SELECT ltrim(bin.y, '0') AS binary_string FROM bin ORDER BY length(binary_string) DESC LIMIT 1), 0	UNION ALL SELECT substr(bitNum, 2),	CASE WHEN substr(bitNum, 1, 1) = '1' THEN cnt + 1 ELSE cnt END FROM bit_cnt	WHERE length(bitNum) > 0) SELECT MAX(cnt) FROM bit_cnt AS bitCnt;"






        
        
    elif originDBType == "postgresql":
        syntaxSubstitutions = {
            r'CREATE TABLE .*?\..*? \(': f"CREATE TABLE [{tableName}] (",
            r'\\n    (?!ADD CONSTRAINT)': "\\n    [",
            r'(\n    \[[\w\s]*?) ': r"\1] ",
            r'"': ""
        }
    
    for pattern, substitution in syntaxSubstitutions.items():
        createStatement = sub(pattern, substitution, createStatement)
    
    return createStatement
        
@app.task
def convertSQLtoNoSQL(tableData, originType, destinationType):
    pass

@app.task
def convertNoSQLtoSQL(data, originType, destinationType):
    pass

if __name__ == "__main__":
    app.start()
