import getopt
import sys

if __name__ == '__main__':
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "o:n:m:", ["operacion=", "op1=", "op2="])
    except getopt.GetoptError as e:
        print('error: {}'.format(e))
        sys.exit(404)
    elementos = ['', '', '']
    
    try:
        for elem in opts:
            if elem[0] in ('-n', '--op1'):
                if not elem[1].replace('-', '').isnumeric():
                    raise Exception('operando no entero ingresado')
                else:
                    elementos[0] = elem[1]
            elif elem[0] in ('-o', '--operacion'):
                if elem[1] not in ('+', '-', '*', '/'):
                    raise Exception('operación no válida ingresada')
                else:
                    elementos[1] = elem[1]
            else:
                if not elem[1].replace('-', '').isnumeric():
                    raise Exception('operando no entero ingresado')
                else:
                    elementos[2] = elem[1]
            
        ecuacion = '{} {} {}'.format(elementos[0], elementos[1], elementos[2])
        print('{} = {}'.format(ecuacion, eval(ecuacion)))
    
    except Exception as e:
        print('error: {}'.format(e))