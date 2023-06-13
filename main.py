from pymongo import MongoClient

if __name__ == '__main__':
    cluster = "mongodb+srv://29560633:'n_9omBqS'@cecs323.vy1mine.mongodb.net/?retryWrites=true&w=majority"
    variable = MongoClient(cluster)