from function_app import myApp

@myApp.activity_trigger()
def reducer(data):
    word, counts = data
    return (word, sum(counts))
