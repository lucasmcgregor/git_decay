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
from git.LineDecayRateAnalyzer import LineDecayRateAnalyzer
from git.GitLogParser import GitLogParser
from git.gitDTO import *

OUTPUT_DIR = "output"

if len(sys.argv) > 3:
    print("Usage: <path to log file> <path to mapping file>")
    sys.exit()

dataFilePath = sys.argv[1]
userMappingFilePath = sys.argv[2]


## read in user mapping file and generate mapping dict
user_mapping_file = open(userMappingFilePath, 'r')
user_tuples = re.findall(r'([\w\.\-@+]*),[\s]*([\w\.\-@+]*)', user_mapping_file.read(), re.M)

user_mapping = {}
for user_tuple in user_tuples:
	user_mapping[user_tuple[0]] = user_tuple[1]


print "Parsing data log: {0}".format(dataFilePath)
parser = GitLogParser(dataFilePath, user_mapping)
gro = parser.parse()
duration = (gro.last_modified_date - gro.start_date).days + 1


print "FILES IN REPO: {0}".format(len(gro.files))
print "FILES IN REMOVED FROM REPO: {0}".format(len(gro.removed_files))
print "REPO START DATE: {0}".format(gro.start_date)
print "REPO LAST MODIFED DATE: {0}".format(gro.last_modified_date)
print "REPO DURATION: {0}".format(duration)

## THESE PYTHON ANALYZERS WERE REPLACES WITH PYSPARK
#user_rate_analyzer = UserRateAnalyzer(gro, OUTPUT_DIR)
#user_rate_analyzer.analyze()

#file_create_remove_analyzer = FileCreateRemoveAnalyzer(gro, OUTPUT_DIR)
#file_create_remove_analyzer.analyze()


## THIS ANALYZER IS ONLY ONE LEFT, IT GENERATES THE MOST GRAINULAR DATA
## LOG THAT IS USED BY PYSPARK
line_decay_rate_alalyzer = LineDecayRateAnalyzer(gro, OUTPUT_DIR)
line_decay_rate_alalyzer.analyze()



