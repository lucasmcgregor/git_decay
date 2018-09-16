## MODULE FOR GIT DATA OBJECTS
class GitRepoObj:

	def __init__(self):
		self.files = {}
		self.removed_files = {}
		self.start_date = None
		self.last_modified_date = None

	def get_file(self, path):
		return self.files.get(path)

	def get_all_files(self):
		return self.files.values()

	def get_all_removed_files(self):
		return self.removed_files.values()

	def contains_file(self, path):
		return path in self.files

	def add_file(self, git_file):
		self.files[git_file.get_path()] = git_file

		if self.start_date is None:
			self.start_date = git_file.date_added
			self.last_modified_date = git_file.date_added
		elif git_file.date_added < self.start_date:
			self.start_date = git_file.date_added

		if git_file.date_added > self.last_modified_date:
			self.last_modified_date = git_file.date_added


	def remove_file(self, path, date, author):

		if (path in self.files):
			gfile = self.files.get(path)
			gfile.date_removed = date
			lines_removed = gfile.remove_all_lines(date, author)

			self.removed_files[path] = gfile
			del self.files[path]
		#else:
		#	print "    FILE NOT FOUND!!!"

		if date > self.last_modified_date:
			self.last_modified_date = date

		#print "    length after: {0}".format(len(self.files))

	def rename_file(self, old_path, new_path, date):
		#print "RENAMING FILE: {0}: {1} to {2}".format(date, old_path, new_path)
		gfile = self.files.get(old_path)

		if gfile is not None:
			gfile.last_modified_date = date
			gfile.path = new_path

			del self.files[old_path]
			self.files[new_path] = gfile
		#else:
		#	print "    FILE NOT FOUND FOR RENAME!!! {0}".format(old_path)


class GitFileLineObj:

		def __init__(self, date_added, author):
			self.date_added = date_added
			self.date_removed = None
			self.author = author
			self.remover = None


		def remove_line(self, date_removed, author):
			self.date_removed = date_removed
			self.remover = author


class GitFileObj:

	def __init__(self, path, date_added, directory=False):
		self.path = path
		self.date_added = date_added
		self.date_removed = None
		self.directory = directory
		self.lines = []
		self.deleted_lines = []

	def get_path(self):
		return self.path;

	def add_line(self, line_number, date_added, author):
		line = GitFileLineObj(date_added, author)
		self.lines.insert(line_number, line)

	def remove_line(self, line_number, date_removed, author):
		if (len(self.lines) > line_number):
			line = self.lines[line_number]
			del self.lines[line_number]

		else:
			line = GitFileLineObj(date_removed, author)

		if (line is not None):

			line.remove_line(date_removed, author)
			self.deleted_lines.append(line)

	def remove_all_lines(self, date_removed, author):
		rv = len(self.lines)
		for i in range(len(self.lines)):
			self.remove_line(0, date_removed, author)

		return rv

	def get_lines(self):
		return self.lines

	def get_removed_lines(self):
		return self.deleted_lines


	def check_and_extend_line_array(self, length):

		elements_to_add  = length - len(self.lines)
		if (elements_to_add > 0):
			self.lines.extend([None]*elements_to_add)