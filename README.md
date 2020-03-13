#  Process - Parse - Merge CSV
1. Merge CSV
2. Process CSV for VLOOKUP

# Modules Required
1. [Python](https://www.python.org/downloads/)
2. [Pandas](https://pypi.org/project/pandas/)
3. [Numpy](https://pypi.org/project/numpy/)
4. [PySimpleGUI](https://pypi.org/project/PySimpleGUI/)
5. [PsUtill](https://pypi.org/project/psutil/)

# Run Setup to install required dependency
```
python setup.py
```

# Parameters in paramters.py file
## Common
1. VALUES_TO_BE_REPLACED_BY_NULL
2. CHUNK
3. PARENT_FILE_LOCATION
4. CHILD_FILE_LOCATION
5. PARENT_PRIMARY_KEY
6. CHILD_FOREIGN_KEY
## Merge
1. SOURCE_COLUMN_NAME
2. PARENT_SOURCE_VALUE
3. CHILD_SOURCE_VALUE
## VLookup
1. CHILD_COLUMN_TO_FETCH_DATA
2. PARENT_COLUMN_TO_PLACE_DATA

# Operation in run.py file
## Main
1. MERGE_DATA
2. REPLACE_DIFFERENCE
3. VLOOKUP
## Sub
1. MERGE_HEADERS
2. CLEAN_EMPTY_COLUMNS
3. ADD_SOURCE_COLUMN
4. OUTER_JOIN
5. OVERRIDE_PARENTDATA


# Run Merge Script
For running a script to merge 2 CSV
```
python run.py
```



## Example Merge
### Master Example.csv

| Id | Name |  Email  |
| -- | ---- | ------- | 
| 2  |  Bb  |         |
| 3  |      | C@b.com |
| 4  |  Db  | D@b.com |
| 6  |  Gb  |         |
| 8  |      | F@b.com |

### Child Example.csv

| EXT_Id | Name |  Email  |
| ------ | ---- | ------- | 
|   1    |  Ab  | a@b.com |
|   2    |      | b@b.com |
|   3    |  Cb  |         |
|   4    | asdf |as@df.com|
|   5    |      |         |

### CSV after Outer Join

| Id | Name |  Email  | EXT_Id |     SOURCE   |
| -- | ---- | ------- | ------ | ------------ |   
|  6 |  Gb  |         |        |    Master    |
|  2 |  Bb  | b@b.com |   2    | Master/Child |
|  3 |  Cb  | C@b.com |   3    | Master/Child |
|  4 |  Db  | D@b.com |   4    | Master/Child |
|  8 |      | F@b.com |        |    Master    |
|    |  Ab  | a@b.com |   1    |    Child     |
|    |      | a@b.com |   5    |    Child     |

### CSV after inner Join with override False

| Id | Name |  Email  | EXT_Id |     SOURCE   |
| -- | ---- | ------- | ------ | ------------ |   
|  2 |  Bb  | b@b.com |   2    | Master/Child |
|  3 |  Cb  | C@b.com |   3    | Master/Child |
|  4 |  Db  | D@b.com |   4    | Master/Child |

### CSV after inner Join with override True

| Id | Name |  Email  | EXT_Id |     SOURCE   |
| -- | ---- | ------- | ------ | ------------ |   
|  2 |  Bb  | b@b.com |   2    | Master/Child |
|  3 |  Cb  | C@b.com |   3    | Master/Child |
|  4 | asdf |as@df.com|   4    | Master/Child |



## Example VLOOKUP
### Target Example_new.csv
|Id|Name|
|-|-|
|1|a|
|2|b|
|3|c|
|4|d|
|5|e|
| |f|
| |g|
| |i|
| |j|

### Lookup Example_old.csv
|Old_Id|New_id|Name|
|-|-|-|
|1|100|a|
|1|1000|aa|
|2|200|b|
|3|300|c|
|4|400|d|
|5|500|e|
|6|600|FF|
|7|700|GG|
|8|800|HH|

### Lookup Processed Example_new.csv
|Id|Name|
|-|-|
| |f|
| |g|
| |i|
| |j|
|100|a|
|200|b|
|300|c|
|400|d|
|500|e|
