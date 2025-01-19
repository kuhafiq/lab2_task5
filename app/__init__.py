import azure.functions as func
import azure.durable_functions as df

# Create the Durable Function App with anonymous authentication level
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Define the activity function for mapper
@myApp.activity_trigger()
def mapper(data):
    line_num, line = data
    words = re.findall(r'\b\w+\b', line.lower())  # Tokenize words
    return [(word, 1) for word in words]

# Define the activity function for reducer
@myApp.activity_trigger()
def reducer(data):
    word, counts = data
    return (word, sum(counts))
