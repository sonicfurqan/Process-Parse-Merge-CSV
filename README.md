# Merge CSV
 Merging csv by using outer join  

# Setup
1. Place master csv in Master Folder 
2. Place child csv in Child Folder
3. Make Sure csv names are same

# Pre run variable names to updated in support.py file
1. FILE_NAME : update this variable with file name
2. VALUES_TO_BE_REPLACED_BY_NULL : array list which contains the values that will be replaced by null
3. PARENT_KEY_FILED : Key column value in master csv file using which record are seached in child csv
4. CHILD_KEY_FIELD : Key column value in child csv file using which records are matched and used to merge with parent record
5. COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED : column name that is added to merged csv representing record is from master or child or in both records master/child

# Supported function 
## Variables in merge.py file
1. MERGHEADERS : if it is set to True column's from parent and child are merged
2. DROPEMPTYHEADERS : if it is set to True column's that does not have any value are removed from csv
3. DATACLEANUP : if it is set to True values given in "VALUES_TO_BE_REPLACED_BY_NULL" array are replaced by null
4. DROPEMPTYROWS : if it is set to True any empty row from csv is removed

# Run
py merge.py