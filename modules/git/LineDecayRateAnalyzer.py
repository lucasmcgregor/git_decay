import os
import traceback
import datetime
from datetime import timedelta
from collections import namedtuple

from GitRepoAnalyzer import GitRepoAnalyzer

class LineDecayRateAnalyzer(GitRepoAnalyzer):

	def __init__(self, git_repo, output_dir):
		super(LineDecayRateAnalyzer, self).__init__(git_repo, output_dir)
		self.first_day = git_repo.start_date - timedelta(days=1)
		self.last_day = git_repo.last_modified_date + timedelta(days=1)
		self.repo_duration = (self.last_day - self.first_day).days + 1

	def process_gfile(self, gfile):

		#print "PROCESSING DECAY DATA FOR: {0}, {1}".format(gfile.get_path(), len(gfile.get_lines()))

		all_lines = gfile.get_removed_lines() + gfile.get_lines()
		for gline in all_lines:
			self.process_gline(gline, gfile.path)


	def process_gline(self, gline, path):

		if gline is not None and gline.date_added is not None:
			create_date = gline.date_added.replace(hour=0, minute=0, second=0, microsecond=0)

			lifespan = None
			if gline.date_removed is not None:
				lifespan = (gline.date_removed - gline.date_added).days
			else:
				lifespan = (self.last_day - gline.date_added).days

			print "Path, Creator, Remover, Created, Removed, Lifespan"
			print "{0}{1}{2}{3}{4}{5}".format(path, gline.author, gline.remover, gline.date_added, gline.date_removed, lifespan)


	def analyze(self):

		for gfile in self.git_repo.get_all_files():
			self.process_gfile(gfile)
