from mrjob.job import MRJob
from mrjob.step import MRStep

class MRRatingCounter(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_cust,
                   reducer=self.reducer_amount_by_customer),
            MRStep(mapper=self.mapper_make_amounts_cust,
                   reducer=self.reducer_output_results)
        ]
    def mapper_get_cust(self, key, line):
        (userID, orderID,amount) = line.split(',')
        yield userID, float(amount)

    def reducer_amount_by_customer(self, userID, amount):
        yield userID, sum(amount)
    
    def mapper_make_amounts_cust(self, custID,  Totalamount):
        yield '%04.02f'%float(Totalamount), custID
        
    def reducer_output_results(self,  Total, customerIDs):
        for customerID in customerIDs:
            yield customerID, Total
if __name__ == '__main__':
    MRRatingCounter.run()
