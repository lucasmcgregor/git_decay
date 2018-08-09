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

import os
import traceback
import datetime
from datetime import timedelta
import re
import json

from git.PatchEventObj import PatchEventObj
from git.UserRateAnalyzer import UserRateAnalyzer
from git.FileCreateRemoveAnalyzer import FileCreateRemoveAnalyzer
from git.GitLogParser import GitLogParser
from git.gitDTO import *

OUTPUT_DIR = "output"


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










if len(sys.argv) > 2:
    print("Usage: <path to log file>")
    sys.exit()

dataFilePath = sys.argv[1]



print "Parsing data log: {0}".format(dataFilePath)
parser = GitLogParser(dataFilePath)
gro = parser.parse()
duration = (gro.last_modified_date - gro.start_date).days + 1


print "FILES IN REPO: {0}".format(len(gro.files))
print "FILES IN REMOVED FROM REPO: {0}".format(len(gro.removed_files))
print "REPO START DATE: {0}".format(gro.start_date)
print "REPO LAST MODIFED DATE: {0}".format(gro.last_modified_date)
print "REPO DURATION: {0}".format(duration)

user_rate_analyzer = UserRateAnalyzer(gro, OUTPUT_DIR)
user_rate_analyzer.analyze()

file_create_remove_analyzer = FileCreateRemoveAnalyzer(gro, OUTPUT_DIR)
file_create_remove_analyzer.analyze()

#ine_decay_rate_alalyzer = LineDecayRateAnalyzer(gro)
#line_decay_rate_alalyzer.analyze()



