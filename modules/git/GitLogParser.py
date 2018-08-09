import sys
import os
import traceback
import datetime
from datetime import timedelta
import re
import json

from git.PatchEventObj import PatchEventObj
from git.gitDTO import *


class GitLogParser(object):

	def __init__(self, path):
		self.path=path
		self.git_repo_object = GitRepoObj()

	def process_event(self, e, git_repo_object):

		#print "Just received event: date={0}, old_path={1}, new_path={2}, start_index={3}, patch_lines={4}".format(
		#	e.date, e.old_path, e.new_path, e.patch_start_index, e.patched_lines)

		#print "Author: {0} added:{1} and removed:{2} lines".format(e.author, e.lines_added, e.lines_removed)

		if (e.is_delete):
			git_repo_object.remove_file(e.old_path, e.date, e.author)

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
					git_file.add_line((line_index + e.patch_start_index), e.date, e.author)
				elif (line == -1):
					#print("    removing line: {0}").format((line_index + e.patch_start_index))
					# this is a removal, so do not increment the index
					# a subsequent remove will use the same index to pull the NEXT line
					git_file.remove_line((line_index + e.patch_start_index), e.date, e.author)
				else:
					# no-op line, so just increment the index
					line_index = line_index + 1


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

				#print "PARSING LINE: {0}".format(line_count+1)

				try:

					## THESE ARE THE COMMIT LEVEL ATTRIBUTES:
					### COMMIT
					### AUTHOR
					### DATE

					## READ A NEW PATCH
					if (line.startswith('commit ')):

						if (patch_event is not None and patch_event.is_populated):
							self.process_event(patch_event, self.git_repo_object)

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
							self.process_event(patch_event, self.git_repo_object)


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
									self.process_event(patch_event, self.git_repo_object)

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



		self.process_event(patch_event, self.git_repo_object)
		return self.git_repo_object