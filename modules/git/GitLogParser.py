		#print "Just received event: date={0}, old_path={1}, new_path={2}, start_index={3}, patch_lines={4}, delete={5}".\
		#	format(e.date, e.old_path, e.new_path, e.patch_start_index, e.patched_lines, e.is_delete)



					## READ THE FILE NAME, IF DEV/NULL -- THIS IS A DELETE
					if (line.startswith('+++ /dev/null')):
						patch_event.is_delete = True
						in_delete_mode = True