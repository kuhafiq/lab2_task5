import re
from app import myApp  # Import from app/init.py

@myApp.activity_trigger()
def mapper(data):
    line_num, line = data
    words = re.findall(r'\b\w+\b', line.lower())  # Tokenize words
    return [(word, 1) for word in words]
