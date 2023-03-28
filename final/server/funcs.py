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
            
            "ROW_COUNT": "changes",
            "ROUND": "round",
            "SIGN": "sign",
            
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

        # No mappeado nativamente
        # "ATAN": "atan" (transformar doble parámetro en uno; con un solo parámetro, el mapeo es directo)
        # "CONV" "printf" (para bases comunes - octal, hexadecimal, etc. - es mapeable con printf; caso contrario, no está disponible)
        # "COT": "1 / tan" (mapearlo como la inversa de la cotangente)
        # "CRC32" (no disponible)
        # "FORMAT" (number): (mapearlo como un PRINTF o FORMAT concatenado con las cifras decimales en otro FORMAT)
        # "HEX": "hex" || "printf" (si el argumento es str, el mapeo es directo; sino, utilizar PRINTF("%x", number))
        # "LOG": "ln" || "log" (si posee un solo argumento, el mapeo es directo con ln; caso contrario, el mapeo es directo con log(base, num))
        # "RAND": "random" (ignorar el argumento y mapear como RANDOM, operado adecuadamente para devolver dentro del mismo rango de valores)
        # "TRUNCATE": "printf"? (averiguar si es posible truncar con printf y después hacer cast a int/float)
        # ("CURDATE", "CURRENT_DATE"): "date" (ajustar parámetros para que sea tiempo local)


        
        
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
