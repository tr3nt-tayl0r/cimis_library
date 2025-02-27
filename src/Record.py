class Record:
	def __init__(self, station_id, start_date, end_date, df):
		self.station_id = station_id
		self.start_date = start_date
		self.end_date = end_date
		self.df = df

	def to_csv(self, filename):
		self.df.to_csv(filename, index=False)	

	def 	