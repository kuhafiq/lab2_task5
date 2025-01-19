import re
from function_app import myApp

@myApp.activity_trigger()
def mapper(data):
    line_num, line = data
    words = re.findall(r'\b\w+\b', line.lower())  # Tokenize
    return [(word, 1) for word in words]
