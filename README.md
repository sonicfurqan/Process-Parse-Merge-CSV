#  Process - Parse - Merge CSV

## Merge Features
 Merging csv by using outer/inner join.

 Identifys Duplicates by Key column fileds left key and right key

 Copys cell data from child to parent if empty or over writes parent cell

 Merges Cell Header of master and child csv 

 Keeps track of record source 

# Setup
1. Place master csv in Data/Master Folder 
2. Place child csv in Data/Child Folder
3. Make Sure master and child csv names are equal

# Pre run 
## variable values in parameters.py file
1. FILE_NAME : update this variable with file name of csv
2. VALUES_TO_BE_REPLACED_BY_NULL : array list which contains the values that will be replaced by null
3. PARENT_KEY_FILED : Key column value in master csv file using which record are searched in child csv
4. CHILD_KEY_FIELD : Key column value in child csv file using which records are matched and used to merge with parent record
5. COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED : column name that is added to merged csv representing record source is Master,Child or Master/Child
6. MERGE : Defaults to "inner" merge i.e only common records are merged and exported to merged csv. If set to "outer" common records are merged from Master and Child csv and unique records are added in merged csv
7. OVERRIDE: if it is set to True.data of child record  will be over written in master record cell even if exisits 

# Supported function 
## Variables values in merge.py file
1. MERGHEADERS : if it is set to True column's from parent and child are merged
2. DROPEMPTYHEADERS : if it is set to True column's that does not have any value are removed from csv
3. DATACLEANUP : if it is set to True values given in "VALUES_TO_BE_REPLACED_BY_NULL" array are replaced by null
4. DROPEMPTYROWS : if it is set to True any empty row from csv is removed
record

# Run
py merge.py



# Example
## Master Example.csv

| Id | Name |  Email  |
| -- | ---- | ------- | 
| 2  |  Bb  |         |
| 3  |      | C@b.com |
| 4  |  Db  | D@b.com |
| 6  |  Gb  |         |
| 8  |      | F@b.com |

## Child Example.csv

| EXT_Id | Name |  Email  |
| ------ | ---- | ------- | 
|   1    |  Ab  | a@b.com |
|   2    |      | b@b.com |
|   3    |  Cb  |         |
|   4    | asdf |as@df.com|
|   5    |      |         |

## CSV after Outer Join

| Id | Name |  Email  | EXT_Id |     SOURCE   |
| -- | ---- | ------- | ------ | ------------ |   
|  6 |  Gb  |         |        |    Master    |
|  2 |  Bb  | b@b.com |   2    | Master/Child |
|  3 |  Cb  | C@b.com |   3    | Master/Child |
|  4 |  Db  | D@b.com |   4    | Master/Child |
|  8 |      | F@b.com |        |    Master    |
|    |  Ab  | a@b.com |   1    |    Child     |
|    |      | a@b.com |   5    |    Child     |

## CSV after inner Join with override False

| Id | Name |  Email  | EXT_Id |     SOURCE   |
| -- | ---- | ------- | ------ | ------------ |   
|  2 |  Bb  | b@b.com |   2    | Master/Child |
|  3 |  Cb  | C@b.com |   3    | Master/Child |
|  4 |  Db  | D@b.com |   4    | Master/Child |

## CSV after inner Join with override True

| Id | Name |  Email  | EXT_Id |     SOURCE   |
| -- | ---- | ------- | ------ | ------------ |   
|  2 |  Bb  | b@b.com |   2    | Master/Child |
|  3 |  Cb  | C@b.com |   3    | Master/Child |
|  4 | asdf |as@df.com|   4    | Master/Child |
