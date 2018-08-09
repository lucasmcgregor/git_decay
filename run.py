#!/usr/bin/env python


__author__ = ['[Lucas McGregor](http://lucasmcgregor.com)']

"""
Will find the decay rate of a code base in git
git log --patch --reverse
"""

## for items created on day x
## at day y, how many are alive
## how many are removed

import sys
sys.path.append("./modules/")

import traceback
import datetime
from datetime import timedelta
import re
import json

#import git.gitDTO
from git.PatchEventObj import PatchEventObj
from git.gitDTO import *



def process_event(e, git_repo_object):

	#print "Just received event: date={0}, old_path={1}, new_path={2}, start_index={3}, patch_lines={4}".format(
	#	e.date, e.old_path, e.new_path, e.patch_start_index, e.patched_lines)

	print "Author: {0} added:{1} and removed:{2} lines".format(e.author, e.lines_added, e.lines_removed)

	if (e.is_delete):
		git_repo_object.remove_file(e.old_path, e.date)

	elif (e.is_rename):
		git_repo_object.rename_file(e.old_path, e.new_path, e.date)

	elif (e.new_path is not None):

		git_file = None

		if (git_repo_object.contains_file(e.new_path)):
			git_file = git_repo_object.get_file(e.new_path)
		else:
			git_file = GitFileObj(e.new_path, e.date)
			git_repo_object.add_file(git_file)


		patch_length = e.lines_added + e.patch_start_index
		git_file.check_and_extend_line_array(patch_length)
		line_index = 0

		#print "Extending git patch for file:{0} to length:{1}".format(git_file.get_path(), patch_length)

		for line in e.patched_lines:
			if (line == 1):
				#print("    adding line: {0}").format((line_index + e.patch_start_index))
				## On add, increment the index
				line_index = line_index + 1
				git_file.add_line((line_index + e.patch_start_index), e.date)
			elif (line == -1):
				#print("    removing line: {0}").format((line_index + e.patch_start_index))
				# this is a removal, so do not increment the index
				# a subsequent remove will use the same index to pull the NEXT line
				git_file.remove_line((line_index + e.patch_start_index), e.date)
			else:
				# no-op line, so just increment the index
				line_index = line_index + 1




class GitLogParser:

	def __init__(self, path):
		self.path=path
		self.git_repo_object = GitRepoObj()


	def parse(self):

		# patch event for the current commit
		commit_patch_event = PatchEventObj()
		# patch event that is being parsed
		patch_event = None


		#new_path = None
		#old_path = None
		#patch_start_index = 0
		in_patch_mode = False
		in_delete_mode = False

		line_count = 0
		with open(self.path) as data_file:
			for line in data_file:

				print "PARSING LINE: {0}".format(line_count+1)

				try:

					## THESE ARE THE COMMIT LEVEL ATTRIBUTES:
					### COMMIT
					### AUTHOR
					### DATE

					## READ A NEW PATCH
					if (line.startswith('commit ')):

						if (patch_event is not None and patch_event.is_populated):
							process_event(patch_event, self.git_repo_object)

						commit_patch_event = PatchEventObj()	

					if (line.startswith('Date: ')):
						dateParts = re.split("\s*", line)
						dateString = dateParts[2] + " " + dateParts[3] + " " + dateParts[5] + " " + dateParts[4] 
						commit_patch_event.date = datetime.datetime.strptime(dateString, '%b %d %Y %H:%M:%S')

					if (line.startswith('Author: ')):
						commit_patch_event.author = line[8:].strip()


					## DIFF WILL START A NEW PATCH EVENT
					## BASED ON THE COMMIT PATCH


					if (line.startswith('diff --git a/')):

						if (patch_event is not None and patch_event.is_populated):
							process_event(patch_event, self.git_repo_object)


						## START A NEW PATCH EVENT BASED ON THE COMMIT EVENT
						patch_event = commit_patch_event.sparse_copy()

						# most basic way to get paths
						# may be overwritten by more sure parsers later
						name_parts = re.split("\s*", line)
						patch_event.old_path = name_parts[2][2:].strip()
						patch_event.new_path = name_parts[3][2:].strip()

						in_patch_mode = False
						in_delete_mode = False

					## READ THE INDEX
					if line.startswith('@@ -'): # and not patch_event.is_delete):
		
							## THIS IS A MULTI-INDEX PATCH
							## PROCESS THE CURRENT EVENT AND SPAWN A NEW ONE BASED ON 
							## THIS CURRENT PATCH
							if in_patch_mode:

								patch_event2 = patch_event.sparse_copy()
								if patch_event.is_populated:
									process_event(patch_event, self.git_repo_object)

								patch_event = patch_event2
								
							index_parts = re.split('\s*', line)
							lines_removed = index_parts[1]
							lines_added = index_parts[2]

							start_lines_removed = re.search('\-([0-9]*)[,]*[0-9]*', lines_removed).group(1)
							start_lines_added = re.search('\+([0-9]*)[,]*[0-9]*', lines_added).group(1)
							
							psi = start_lines_added if (start_lines_added < start_lines_removed) else start_lines_removed;
							patch_event.patch_start_index = int(psi)

							in_patch_mode = True




					if (line.startswith('deleted file mode ')):
						patch_event.is_delete = True
						in_delete_mode = True

					if (line.startswith('new file mode ')):
						patch_event.is_create = True

					if (line.startswith('rename from ')):
						patch_event.is_rename = True
						patch_event.old_path = line[12:].strip()

					if (line.startswith('rename to ')):
						patch_event.new_path = line[10:].strip()

					## READ IF AN EXISTING FILE
					if (line.startswith('--- a/')):
						old_path = line[6:].rstrip()
						patch_event.old_path = old_path

					## READ THE FILE NAME
					if (line.startswith('+++ b/')):

						new_path = line[6:].rstrip()
						patch_event.new_path = new_path
						#print "New Filename {0}".format(new_path)


					if (in_patch_mode):
						if line.startswith('+'):
							patch_event.patched_lines.append(1)
							patch_event.lines_added += 1
							patch_event.is_populated = True
						elif line.startswith('-'):
							patch_event.patched_lines.append(-1)
							patch_event.lines_removed += 1
							patch_event.is_populated = True
						elif re.match(r'\s', line):
							patch_event.patched_lines.append(0)
							#patch_event.lines_added += 1 # these lines are not added, but will show up in the indexs


				except:
					print "Error in line: {0}, {1}".format(line_count, sys.exc_info()[0])
					exc_type, exc_value, exc_traceback = sys.exc_info()
					traceback.print_tb(exc_traceback, limit=10, file=sys.stdout)

				line_count += 1



		process_event(patch_event, self.git_repo_object)
		return self.git_repo_object

class DecayData:
	def __init__(self):
		self.lines_created = 0
		self.time_to_deaths = []

	def add_line(self, time_to_death):
		self.lines_created += 1

		if (time_to_death is not None):
			self.time_to_death.append(time_to_death)



class DateToGroupingToPathToLines:

	def __init__(self, start_date, end_date):
		self.dates = {}
		self.start_date = start_date
		self.end_date = end_date
		self.repo_duration = (self.end_date - self.start_date).days + 1

		date_range = [self.start_date + datetime.timedelta(days=x) for x in range(0, self.repo_duration)]
		for day in date_range:
			self.dates[day.replace(hour=0, minute=0, second=0, microsecond=0)] = {}

	def append_line_data(self, day, path, time_to_death):
		decay_data = self.dates[day]

		if (decay_data is None):
			decay_data = DecayData()
			self.dates[day] = decay_data

		decay_data.add_line(time_to_death)



	def process_gfile(self, gfile):

		## MIGHT BE TRUNCATED FOR GROUPING
		path = gfile.get_path()

		#print "PROCESSING DECAY DATA FOR: {0}, {1}".format(gfile.get_path(), len(gfile.get_lines()))


		for gline in gfile.get_lines():

			if gline is not None and gline.date_added is not None:
				created_day = gline.date_added.replace(hour=0, minute=0, second=0, microsecond=0)

				time_to_death = None
				if gline.date_removed is not None:
					time_to_death = (gline.date_removed - gline.date_added).hours

				#print "    data: {0}, {1}, {2}".format(created_day, path, time_to_death)
				#self.add_line_data(created_day, path, time_to_death)
			#else:
				#print "    data: is None"




class LineDecayRateAnalyzer():

	def __init__(self, git_repo):
		self.git_repo = git_repo
		self.first_day = gro.start_date - timedelta(days=1)
		self.last_day = gro.last_modified_date + timedelta(days=1)
		self.repo_duration = (self.last_day - self.first_day).days + 1

	def analyze(self):


		date_grouped_lines = DateToGroupingToPathToLines(self.first_day, self.last_day)

		for gfile in self.git_repo.get_all_files():
			print "Analyzing file: {0}".format(gfile.path)
			date_grouped_lines.process_gfile(gfile)


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



class FileCreateRemoveAnalyzer:

	def __init__(self, git_repo):
		self.git_repo = git_repo
		self.first_day = gro.start_date - timedelta(days=1)
		self.last_day = gro.last_modified_date + timedelta(days=1)
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

		print "date, files_created_today, files_removed_today, files_in_existence, files_in_removed, avg_lifetime, avg_removetime, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10"

	
		for day_count in range(0,self.repo_duration):
			day = (self.first_day + timedelta(days=day_count)).replace(hour=23, minute=59, second=59, microsecond=0)


			(files_created_today, files_existing, avg_lifetime, existing_tenths) = self.get_day_stats(
				self.git_repo.get_all_files(), day, day_count, self.get_added_date, self.get_existing_duration)

			(files_removed_today, files_removed, avg_removetime, removed_tenths) = self.get_day_stats(
				self.git_repo.get_all_removed_files(), day, day_count, self.get_removed_date, self.get_removed_duration)



			print "{0},{1},{2},{3},{4},{5},{6},".format(day.strftime('%Y-%m-%d'), files_created_today, files_removed_today, files_existing, files_removed, avg_lifetime, avg_removetime),
			for v in existing_tenths:
				print "{0},".format(v),

			for v in removed_tenths:
				print "{0},".format(v),

			print ""



if len(sys.argv) > 2:
    print("Usage: <path to log file>")
    sys.exit()

dataFilePath = sys.argv[1]



print "Parsing data log: {0}".format(dataFilePath)
parser = GitLogParser(dataFilePath)
gro = parser.parse()

print "FILES IN REPO: {0}".format(len(gro.files))
print "FILES IN REMOVED FROM REPO: {0}".format(len(gro.removed_files))
print "REPO START DATE: {0}".format(gro.start_date)
print "REPO LAST MODIFED DATE: {0}".format(gro.last_modified_date)

duration = (gro.last_modified_date - gro.start_date).days + 1
print "REPO DURATION: {0}".format(duration)

#file_create_remove_analyzer = FileCreateRemoveAnalyzer(gro)
#file_create_remove_analyzer.analyze()
line_decay_rate_alalyzer = LineDecayRateAnalyzer(gro)
line_decay_rate_alalyzer.analyze()



