#!/usr/bin/env python
"""
This is a simple script to extact all the authors from your git logs
It will generate a CSV of lowered email address, lowered email address
It is common to find that a single real user has used multiple email addresses 
in a git repo, hence will look like multiple people.

Change the second column to the single account you want to map to, for example:
email_a, email_a
email_b, email_b
email_c, email_a
email_d, email_a

This mapping will tell the analyzers that emails a, c, and d are all a
"""

import sys
sys.path.append("./modules/")

import os
import traceback
import re


if len(sys.argv) > 2:
    print("Usage: <path to log file>")
    sys.exit()

dataFilePath = sys.argv[1]
file = open(dataFilePath, 'r')

authors = re.findall(r'Author: [\w\.\-\s_]*<([\w\.\-_]+@[\w\.\-_]+)>', file.read(), re.M)

cleaned_authors = {}
for author_tuple in authors:
	cleaned_authors[author_tuple.lower()] = True

for key, value in cleaned_authors.iteritems():
	print "{0},{0}".format(key)