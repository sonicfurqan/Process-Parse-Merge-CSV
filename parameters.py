# Common Parameters
VALUES_TO_BE_REPLACED_BY_NULL = ["unassigned", "N/A", " ", "", "nan"]
######################################################################
# Merge File Parameters
FILE_NAME = "Example"
PARENT_KEY_FILED = "Id"
CHILD_KEY_FIELD = "EXT_Id"
COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED = "SOURCE"
# Supports only inner or outer if not correct value defaults to inner
MERGE = "outer"
OVERRIDE = False
######################################################################
# Vlookup File Paramters
TARGET_FILE_NAME = "Example"
TARGET_FILE_KEY_COLUMN_NAME = "Id"
LOOKUP_FILE_KEY_COLUMN_NAME = "Old_Id"
LOOKUP_FILE_VALUE_COLUMN_NAME = "New_id"
