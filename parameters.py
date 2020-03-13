# Common Parameters
VALUES_TO_BE_REPLACED_BY_NULL = ["unassigned", "N/A", " ", "", "nan"]
CHUNK = 1
PARENT_FILE_LOCATION = r"Data\Parent\Example.csv"
CHILD_FILE_LOCATION = r"Data\Child\Example.csv"
PARENT_PRIMARY_KEY = "Id"
CHILD_FOREIGN_KEY = "EXT_Id"

######################################################################
# Merge
SOURCE_COLUMN_NAME = "SOURCE"
PARENT_SOURCE_VALUE = "Parent"
CHILD_SOURCE_VALUE = "Child"
######################################################################
# Vlookup
CHILD_COLUMN_TO_FETCH_DATA = "Email"
PARENT_COLUMN_TO_PLACE_DATA = "Extra"
