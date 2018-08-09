import os
import traceback
import datetime
from datetime import timedelta

from GitRepoAnalyzer import GitRepoAnalyzer

class UserRateAnalyzer(GitRepoAnalyzer):

	def __init__(self, git_repo, output_dir):
		super(UserRateAnalyzer, self).__init__(git_repo, output_dir)
		self.first_day = git_repo.start_date - timedelta(days=1)
		self.last_day = git_repo.last_modified_date + timedelta(days=1)
		self.repo_duration = (self.last_day - self.first_day).days + 1

	class AuthorData:
		def __init__(self, author):
			self.author = author
			self.lines_added = 0
			self.lines_removed = 0

	class TimeSliceData:
		def __init__(self):
			self.time_slices = {}

		def add_author_data(self, time_slice, author, line_change):

			tsd = None
			if time_slice in self.time_slices:
				tsd = self.time_slices[time_slice]
			else:
				tsd = {}
				self.time_slices[time_slice] = tsd

			ad = None
			if author in tsd:
				ad = tsd[author]
			else:
				ad = UserRateAnalyzer.AuthorData(author)
				tsd[author] = ad

			if line_change == 1:
				ad.lines_added += 1
			elif line_change == -1:
				ad.lines_removed += 1	



	def analyze(self):

		time_slice_date = UserRateAnalyzer.TimeSliceData()
		for gfile in self.git_repo.get_all_files():
			for line in gfile.lines:
				if line is not None:
					time_slice_date.add_author_data(line.date_added.replace(hour=1, minute=0, second=0, microsecond=0), line.author, 1)
			for line in gfile.deleted_lines:
				if line is not None:
					time_slice_date.add_author_data(line.date_removed.replace(hour=1, minute=0, second=0, microsecond=0), line.remover, -1)

		for gfile in self.git_repo.get_all_removed_files():
			for line in gfile.deleted_lines:
				if line is not None:
					time_slice_date.add_author_data(line.date_removed.replace(hour=1, minute=0, second=0, microsecond=0), line.remover, -1)


		if not os.path.exists(self.output_dir):
			os.mkdir(self.output_dir)
		data_file = open(self.output_dir + "/user_change_rates.cvs", "wb")

		#print("date, author, lines_added, lines_removed")
		data_file.write("date, author, lines_added, lines_removed")
		for day, data in time_slice_date.time_slices.iteritems():
			for author, author_data in data.iteritems():
				data_line = '{},{},{},{}\n'.format(day, author, author_data.lines_added, author_data.lines_removed)
				data_file.write(data_line)

		data_file.close()