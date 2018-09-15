import os
import traceback
import datetime
from datetime import timedelta
from collections import namedtuple

from GitRepoAnalyzer import GitRepoAnalyzer

class DecayData:
	def __init__(self, create_date, path, days_remaining_in_repo):
		self.create_date = create_date
		self.path = path
		self.days_remaining_in_repo = days_remaining_in_repo
		self.lines_created = 0
		self.time_to_death_count = []

		for x in range(0, self.days_remaining_in_repo+1):
			self.time_to_death_count.append(0)

	def add_line(self, time_to_death):
		self.lines_created += 1

		if (time_to_death is not None):
			count = self.time_to_death_count[time_to_death] + 1
			self.time_to_death_count[time_to_death] = count



class DateToGroupingToPathToLines:

	def __init__(self, start_date, end_date):
		self.decay_data = {}
		self.start_date = start_date
		self.end_date = end_date
		self.repo_duration = (self.end_date - self.start_date).days + 1

		#date_range = [self.start_date + datetime.timedelta(days=x) for x in range(0, self.repo_duration)]
		#for day in date_range:
		#	self.decay_data[day.replace(hour=0, minute=0, second=0, microsecond=0)] = {}

	def append_line_data(self, create_date, path, time_to_death):
		DecayDataKey = namedtuple('DecayDataKey' , 'create_day path')
		key = DecayDataKey(create_day=create_date, path=path)

		decay_data_record = None
		if (key in self.decay_data):
			decay_data_record = self.decay_data[key]

		if (decay_data_record is None):
			days_remaining = (self.end_date - create_date).days
			decay_data_record = DecayData(create_date, path, days_remaining)
			self.decay_data[key] = decay_data_record

		decay_data_record.add_line(time_to_death)



	def process_gfile(self, gfile, path_level):

		## MIGHT BE TRUNCATED FOR GROUPING
		path = "/"
		if (path_level > 0):
			path_parts = gfile.get_path().split("/")
			if (len(path_parts) > path_level):
				path = "/".join(path_parts[:path_level])
			else:
				path = gfile.get_path()

		#print "PROCESSING DECAY DATA FOR: {0}, {1}".format(gfile.get_path(), len(gfile.get_lines()))

		all_lines = gfile.get_removed_lines() + gfile.get_lines()
		for gline in all_lines:

			if gline is not None and gline.date_added is not None:
				create_date = gline.date_added.replace(hour=0, minute=0, second=0, microsecond=0)

				lifespan = None
				if gline.date_removed is not None:
					lifespan = (gline.date_removed - gline.date_added).days
				else:
					lifespan = 



				
					#print "     appending line: day={0}, path={1}, days_to_death={2}"\
					#	.format(create_date, path, days_to_death)

				self.append_line_data(create_date, path, days_to_death)


class LineDecayRateAnalyzer(GitRepoAnalyzer):


	def __init__(self, git_repo, path_level=0):
		self.git_repo = git_repo
		self.first_day = git_repo.start_date - timedelta(days=1)
		self.last_day = git_repo.last_modified_date + timedelta(days=1)
		self.repo_duration = (self.last_day - self.first_day).days + 1
		self.path_level = path_level

	def analyze(self):


		date_grouped_lines = DateToGroupingToPathToLines(self.first_day, self.last_day)

		for gfile in self.git_repo.get_all_files():
			#print "Analyzing file: {0}".format(gfile.path)
			date_grouped_lines.process_gfile(gfile, self.path_level)

		print "create_day, path, lines_in_cohort, day_in_cohort, lines_dead, pct_decay"
		for key, value in date_grouped_lines.decay_data.items():

			for x in range(0, len(value.time_to_death_count)):
				pct_decay = (value.time_to_death_count[x]/value.lines_created)

				print "{0}, {1}, {2}, {3}, {4}, {5}".format \
					(value.create_date, \
					value.path, \
					value.lines_created, \
					x, \
					value.time_to_death_count[x], \
					pct_decay)


			# find length of repo
			# cycle through days
			# open all active files
			# find all lines created for this day
			# calculate removed date
			# open all removed files
			# find all lines created on this day
			# and calculate length
			# remote: date, group_path, ttl, full_path

			# create_date, path, time_to_death (nullable)
			# create_date, path, lines_created, lines_removed, pct decay, 10-100 decay
