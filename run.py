import os
from git.UserRateAnalyzer import UserRateAnalyzer
from git.FileCreateRemoveAnalyzer import FileCreateRemoveAnalyzer
from git.GitLogParser import GitLogParser
OUTPUT_DIR = "output"

duration = (gro.last_modified_date - gro.start_date).days + 1

user_rate_analyzer = UserRateAnalyzer(gro, OUTPUT_DIR)
user_rate_analyzer.analyze()

file_create_remove_analyzer = FileCreateRemoveAnalyzer(gro, OUTPUT_DIR)
file_create_remove_analyzer.analyze()

#ine_decay_rate_alalyzer = LineDecayRateAnalyzer(gro)
#line_decay_rate_alalyzer.analyze()