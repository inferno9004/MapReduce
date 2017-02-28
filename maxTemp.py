from mrjob.job import MRJob

class MRmaxTemp(MRJob):

	def ToFaren(self, temp):
		celcius = float(temp)/10.0
		far = (celcius*1.8) +32.0
		return far

	def mapper(self, key, line):
		(location, date, type, val, a,b,c,d) = line.split(',')
		if (type == "TMIN"):
			temperature = self.ToFaren(val)
			yield location, temperature

	def reducer(self, location, tems):
		yield location,max(tems)

if __name__ == '__main__':
	MRmaxTemp.run()
