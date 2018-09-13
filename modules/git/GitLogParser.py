	def __init__(self, path, user_mapping):
		self.user_mapping = user_mapping
		author = e.author
		if self.user_mapping is not None and e.author in self.user_mapping:
			author = self.user_mapping[e.author]

			git_repo_object.remove_file(e.old_path, e.date, author)
					git_file.add_line((line_index + e.patch_start_index), e.date, author)
					git_file.remove_line((line_index + e.patch_start_index), e.date, author)
						match = re.search(r'<([\w\.\-\_]+@[\w\.\-\_]+)>$', author)
						if match:
							author_email = match.group(1)
						else:
							author_email = author


							start_lines_removed = re.search(r'\-([0-9]*)[,]*[0-9]*', lines_removed).group(1)
							start_lines_added = re.search(r'\+([0-9]*)[,]*[0-9]*', lines_added).group(1)