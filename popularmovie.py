from mrjob.job import MRJob
from mrjob.step import MRStep

class Mostpopularmovie(MRJob):

    def configure_options(self):
        super(Mostpopularmovie, self).configure_options()
        self.add_file_option("--item", help = "path to u.item")

    def steps(self):
        return [
            MRStep(mapper = self.mapper_1,
                   reducer_init = self.reducer_init,
                   reducer = self.reducer_count_ratings),
            MRStep(reducer = self.reducer_maxfinder)
        ]

    def mapper_1(self, _,line):
        (userID, movieID, rating, timestamp) = line.split("\t")
        yield movieID,1

    def reducer_init(self):
        self.moviemap = {}

        with open("u.item") as f:
            for line in f:
                fields = line.split("|")
                self.moviemap[fields[0]] = fields[1].decode('utf-8', 'ignore')

    def reducer_count_ratings(self, key, val):
        yield None, (sum(val), self.moviemap[key])

    def reducer_maxfinder(self, key, val):
        yield max(val)


if __name__ == "__main__":
    Mostpopularmovie.run()