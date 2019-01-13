# Git Decay Analyzer 

Python scripts for analyzing git history logs and caluclating statistics for rates of change.
This is initial work to identify which stats are meaningful.

Once finalized, it will be packaged with instructions to analyze code bases.


## Author
Lucas McGregor lucas.mcgregor@gmail.com

## License
Not licensed yet

## Instructions

### 1) Generate your author mapping table
First run the author mapping script
```commandline
./get_authors.py test_data/dt.log
```

Then edit the /output/authors_mapping.csv file to have the correct mapping.


### 2) Convert the git log to a usable data format
```commandline
./parse_git_log.py test_data/dt.log output/authors_mapping.cvs
###

```

### 3) Run the PySpark Analyzers
```commandline
/ss.sh process_line_decay.py
```




