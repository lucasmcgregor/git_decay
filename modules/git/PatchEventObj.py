class PatchEventObj:

	def __init__(self):
		self.date = None
		self.new_path = None
		self.old_path = None
		self.is_create = False
		self.is_delete = False
		self.is_rename =  False
		self.is_populated = False
		self.author = None
		self.patch_start_index = 0
		self.lines_added = 0
		self.lines_removed = 0
		self.patched_lines = []

	def sparse_copy(self):
		patch_event = PatchEventObj()
		patch_event.date = self.date
		patch_event.new_path = self.new_path
		patch_event.old_path = self.old_path
		patch_event.is_rename = self.is_rename
		patch_event.is_delete = self.is_delete
		patch_event.is_create= self.is_create
		patch_event.author = self.author

		return patch_event
