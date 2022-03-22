import argparse
import re

parser = argparse.ArgumentParser(description = 'Permite realizar las cuatro operaciones básicas (+, -, *, /) sobre dos operandos dados.',
exit_on_error = False)
parser.add_argument('-o', required = True, choices = ['+', '-', '/', '*'], help = 'Operación a realizar (+, -, *, /).')
parser.add_argument('-n', required = True, type = int, help = 'Primer operando de la operación')