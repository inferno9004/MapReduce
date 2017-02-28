from mrjob.job import MRJob

class MRwordcounter(MRJob):

	def mapper(self, key, line):
		words = line.split()
		for word in words:
			word = unicode(word, "utf-8", errors="ignore")
			yield word.lower(),1

	def reducer(self, word, count):
		yield word, sum(count)

if __name__ == '__main__':
	MRwordcounter.run()