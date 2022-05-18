from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, orderID,amount) = line.split(',')
        yield userID, float(amount)

    def reducer(self, userID, amount):
        yield userID, sum(amount)

if __name__ == '__main__':
    MRRatingCounter.run()
