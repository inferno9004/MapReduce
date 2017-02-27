from mrjob.job import MRJob

class MRFriends(MRJob):
	def mapper(self, key, line):
		(ID, name, age, numFriends) = line.split(',')
		yield age, float(numFriends)

	def reducer(self, age, numFriends):
		total_friends = 0
		counter = 0
		for x in numFriends:
			total_friends += x
			counter += 1

		yield age, total_friends/counter

if __name__ == "__main__":
	MRFriends.run()
