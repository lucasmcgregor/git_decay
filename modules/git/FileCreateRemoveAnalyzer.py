import os
import traceback
import datetime
from datetime import timedelta

from GitRepoAnalyzer import GitRepoAnalyzer


class FileCreateRemoveAnalyzer(GitRepoAnalyzer):

	def __init__(self, git_repo, output_dir):
		super(FileCreateRemoveAnalyzer, self).__init__(git_repo, output_dir)
		self.first_day = git_repo.start_date - timedelta(days=1)
		self.last_day = git_repo.last_modified_date + timedelta(days=1)
		self.repo_duration = (self.last_day - self.first_day).days + 1


	def get_added_date(self, gfile):
		return gfile.date_added

	def get_removed_date(self, gfile):
		return gfile.date_removed

	def get_removed_duration(self, gfile, day):
		grd = (gfile.date_removed - self.git_repo.start_date).days
		#print "DELETED DURATION: day: {0}, sop: {1}, rd:{2}, dd:{3}".format(day, self.git_repo.start_date, gfile.date_removed, grd)

		return grd

	def get_existing_duration(self, gfile, day):
		return (day - gfile.date_added).days  + 1

	def get_day_stats(self, file_list, day, day_count, action_date_function, duration_function):

		files_today = 0
		files_in_state = 0
		files_in_state_durations = []
		duration_totals = [0]*10
		avg_state_lifetime = 0

		for gfile in file_list:

			if action_date_function(gfile) < day:
				files_in_state += 1

				duration = duration_function(gfile, day)
				files_in_state_durations.append(duration)

				if action_date_function(gfile) > (day - timedelta(days=1)):
					files_today += 1

				totals_index = 0
				if day_count > 0:
					totals_index = (duration*10) // day_count
					if totals_index > 9:
						totals_index = 9

				#print "      DAY: {0}, DR: {1}, D: {2}, DC:{3}, I:{4}".format(day, gfile.date_removed, duration, day_count, totals_index)

				duration_totals[totals_index] = duration_totals[totals_index] + 1

		if (len(files_in_state_durations)>0):
			avg_state_lifetime = sum(files_in_state_durations) / len(files_in_state_durations)

		return (files_today, files_in_state, avg_state_lifetime, duration_totals)

	def analyze(self):


		if not os.path.exists(self.output_dir):
			os.mkdir(self.output_dir)
		data_file = open(self.output_dir + "/file_create_remove.cvs", "wb")


		data_file.write("date, files_created_today, files_removed_today, files_in_existence, files_in_removed, avg_lifetime, avg_removetime, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10\n")

	
		for day_count in range(0,self.repo_duration):
			day = (self.first_day + timedelta(days=day_count)).replace(hour=23, minute=59, second=59, microsecond=0)


			(files_created_today, files_existing, avg_lifetime, existing_tenths) = self.get_day_stats(
				self.git_repo.get_all_files(), day, day_count, self.get_added_date, self.get_existing_duration)

			(files_removed_today, files_removed, avg_removetime, removed_tenths) = self.get_day_stats(
				self.git_repo.get_all_removed_files(), day, day_count, self.get_removed_date, self.get_removed_duration)


			data_line = "{0},{1},{2},{3},{4},{5},{6},".format(day.strftime('%Y-%m-%d'), files_created_today, files_removed_today, files_existing, files_removed, avg_lifetime, avg_removetime)
			for v in existing_tenths:
				data_line += "{0},".format(v)

			for v in removed_tenths:
				data_line += "{0},".format(v)

			data_line += "\n"
			data_file.write(data_line)

		data_file.close()


