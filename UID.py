import uuid

alpha = '1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
alphabet = list(reversed(sorted(set(alpha))))
lenalpha = len(alphabet)


def int_to_string(number, alphabet, padding=None):
    output = ""
    alpha_len = len(alphabet)
    while number:
        number, digit = divmod(number, alpha_len)
        output += alphabet[digit]
    if padding:
        remainder = max(padding - len(output), 0)
        output = output + alphabet[0] * remainder
    return output[::-1]

def string_to_int(string, alphabet):
    number = 0
    alpha_len = len(alphabet)
    for char in string:
        number = number * alpha_len + alphabet.index(char)
    return number

class UID(object):

    def __new__(cls, id=None, lenstr=8):
        maxint = lenalpha ** lenstr
        lenmaxint = len(str(maxint)) - 1
        maxint = int('9' * lenmaxint)
        # maxstr = int_to_string(maxint, alpha, lenstr)
        # print(f'lenalpha {lenalpha}  maxint {maxint:,}  lenmaxint {lenmaxint}')
        if id is None:  # If no integer or string input then generate a new UID
            self = object.__new__(cls)
            self._alpha = alpha
            self._lenalpha = lenalpha
            self._maxint = maxint
            self._lenmaxint = lenmaxint
            return self
        idlen = len(str(id))
        if isinstance(id, int):  # Return the string representation if an integer is provided
            if id > maxint:
                print(f'Integer {id:,} is > max integer {maxint:,} for string rep length {lenstr}')
                return None
            return int_to_string(id, alpha, lenstr)
        if isinstance(id, str):  # Return the integer representation if a string is provided
            if idlen > lenstr:
                print(f'String length {idlen} is > max string length {lenstr}')
                return None
            convint = string_to_int(id, alpha)
            if convint > maxint:
                print(f'String {id} is not valid, int value {convint:,} > max int value {maxint:,}')
                return None
            return convint


    def __init__(self, thing=None, lenstr=8):
        self._lenstr = lenstr
        self._int = int(str(uuid.uuid4().int)[-self._lenmaxint:])
        self._str = int_to_string(self.int, self._alpha, self._lenstr)
        # print(f'intlen {self._lenmaxint}  int {self._int}  str {self._str}')

    def __int__(self):
        return self._int

    def __str__(self):
        return self._str

    @property
    def str(self):
        return self._str

    @property
    def lenstr(self):
        return self._lenstr

    @property
    def int(self):
        return self._int

    @property
    def maxint(self):
        return self._maxint

    @property
    def lenmaxint(self):
        return self._lenmaxint

    @property
    def alphabet(self):
        return self._alpha

    @property
    def lenalpha(self):
        return self._lenalpha

