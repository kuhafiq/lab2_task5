from app import myApp  # Import from app/init.py

@myApp.activity_trigger()
def shuffler(data):
    shuffle_dict = {}
    for word_counts in data:
        for word, count in word_counts:
            if word not in shuffle_dict:
                shuffle_dict[word] = []
            shuffle_dict[word].append(count)
    return shuffle_dict
