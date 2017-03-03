from mrjob.job import MRJob
from mrjob.step import MRStep

class Popsuperhero(MRJob):

    def configure_options(self):
        super (Popsuperhero, self).configure_options()
        self.add_file_option("--item", help = "path to file")

    def steps(self):
        return [
            MRStep(mapper = self.mapper_1,
                   reducer = self.reducer_1),
            MRStep(mapper = self.mapper_2,
                   mapper_init = self.load_name_dict,
                   reducer = self.reducer_2)
        ]

    def mapper_1(self,_,line):
        fields = line.split()
        heroID = fields[0]
        numfriends = len(fields)-1
        yield heroID, numfriends

    def reducer_1(self, heroID, numfriends):
        yield heroID, sum(numfriends)

    def mapper_2(self, heroID, sumfriends):
        hero = self.heronames[int(heroID)]
        yield None,(sumfriends, hero)

    def reducer_2(self, key, tup):
        yield max(tup)

    def load_name_dict(self):
        self.heronames = {}

        with open("Marvel-Names.txt") as f:
            for line in f:
                fields = line.split('"')
                heroID = int(fields[0])
                self.heronames[heroID] = fields[1].decode('utf-8', 'ignore')


if __name__ == "__main__":
    Popsuperhero.run()
