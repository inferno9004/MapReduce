from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRsortedwordcount(MRJob):

	def steps(self):
		return [
				MRStep(mapper = self.mapper_get_words,
						reducer = self.reducer_count_words),
				MRStep(mapper = self.mapper_make_counts_key,
						reducer = self.reducer_output_words)
				]

	def mapper_get_words(self, key, line):
		words = re.compile(r"[\w']+").findall(line)
		for word in words:
			yield word.lower(), 1

	def reducer_count_words(self, word, count):
		yield word, sum(count)

	def mapper_make_counts_key(self, word, val):
		yield '%04d'%int(val), word

	def reducer_output_words(self, count, words):
		for word in words:
			yield count, word

if __name__ == '__main__':
	MRsortedwordcount.run()