import argparse

parser = argparse.ArgumentParser(description = 'Permite realizar las cuatro operaciones básicas (+, -, *, /) sobre dos operandos dados.')

parser.add_argument('-o', '--operation', required = True, choices = ['+', '-', '/', '*'], help = 'Operación a realizar (+, -, *, /).')
parser.add_argument('-n', required = True, type = int, help = 'Primer operando de la operación.')
parser.add_argument('-m', required = True, type = int, help = 'Segundo operando de la operación.')
args = parser.parse_args()

try:
    if args.operation == '/' and args.m == 0:
        raise Exception('division by 0')
    else:
        x = '{} {} {}'.format(args.n, args.operation, args.m)
        print('{} = {}'.format(x, eval(x)))
except Exception as e:
    print('calculadora.py: error: {}'.format(e))
