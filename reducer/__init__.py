from app import myApp  # Import from app/init.py

@myApp.activity_trigger()
def reducer(data):
    word, counts = data
    return (word, sum(counts))
