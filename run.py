from Support.utility import check_memory, read, clean_dataframe, add_header, create_file, append_to_file, printProgressBar
from parameters import (
    VALUES_TO_BE_REPLACED_BY_NULL, PARENT_PRIMARY_KEY, CHILD_FOREIGN_KEY, SOURCE_COLUMN_NAME, PARENT_SOURCE_VALUE, CHILD_SOURCE_VALUE, CHUNK,
    PARENT_FILE_LOCATION, CHILD_FILE_LOCATION, PARENT_COLUMN_TO_PLACE_DATA, CHILD_COLUMN_TO_FETCH_DATA
)
import pandas as pd
import numpy as np
import time

import PySimpleGUI as sg
sg.theme('DarkBlack')

# importing fileds


# True:Combains 2 Csv's Headers / False:Removes columns from chlid csv that are not present in master
MERGE_HEADERS = True
# True:Remove all empty rows and columns that are totaly empty / False: keeps emtpy values
CLEAN_EMPTY_COLUMNS = True
# True:Add new column in result that keeps shows record origin / False: Ignores columns
ADD_SOURCE_COLUMN = True
# True: Keeps data from both parent and child / False: Only keeps matching data
OUTER_JOIN = True
# True: dumps data from child csv to parent csv ignores empty value of child / False:Only fetches data from child csv when parent cell is empty
OVERRIDE_PARENTDATA = False

# True: merges data in parent csv from child csv / False: Ignores
# Note: REPLACE_DIFFERENCE and VLOOKUP should be false
MERGE_DATA = True
# True: compares parent and child csv cell if its not equal get data from child csv / False: Ignores
# Note: MERGE_DATA and VLOOKUP should be false
REPLACE_DIFFERENCE = False
# True: Copys data from child csv's spesfied column and place in parent csv's specified column/ False:Ignores
# Note: MERGE_DATA and VLOOKUP REPLACE_DIFFERENCE be false
VLOOKUP = False


# Resulting file name that contains data
PROCESSED_FILE_NAME = 'Result.csv'
# log file name that is genrated
LOG_FILE_NAME = 'Log.csv'

master_file_location = PARENT_FILE_LOCATION
child_file_location = CHILD_FILE_LOCATION


BATCH = CHUNK


def normalize_header(master_csv, child_csv, master_index, child_index, merge):
    master_colums = master_csv.columns
    child_colums = child_csv.columns

    # Getting Uniques
    unique_in_child = child_colums[~child_colums.isin(master_colums)]
    unique_in_master = master_colums[~master_colums.isin(child_colums)]

    print("---------Header Details-----------------")
    print("Unique columns in child %s " % len(unique_in_child))
    print("Unique columns in master %s " % len(unique_in_master))
    if merge:
        unique_colum_for_master = pd.DataFrame(
            columns=unique_in_child, dtype=object)
        unique_colum_for_child = pd.DataFrame(
            columns=unique_in_master, dtype=object)
        # Adding Unique coloums respectivly
        master_csv = pd.concat([master_csv, unique_colum_for_master], axis=1)
        print("%s columns added to master csv" % len(unique_in_child))
        child_csv = pd.concat([child_csv, unique_colum_for_child], axis=1)
        print("%s columns added to child csv" % len(unique_in_master))
    else:
        # removing index fields
        unique_in_child = unique_in_child[~unique_in_child.isin([child_index])]
        unique_in_master = unique_in_master[~unique_in_master.isin([
                                                                   master_index])]

        # Removing unique column from child
        child_csv = child_csv.drop(unique_in_child, axis=1)
        print("%s columns removed from child csv" % len(unique_in_child))
    print("--------------------------")
    return master_csv, child_csv


def get_nonIndexed(file, index_filed):
    # Returning all rows that has empty value in index column
    return file[file[index_filed].isna()].reset_index(drop=True)


def get_indexed(file, index_filed):
    # Returning file with new indexed column as passed
    return file[~file[index_filed].isna()].set_index(index_filed, drop=False)


start_time = time.time()

# Main Start
if MERGE_DATA:
    print("REPLACE_DIFFERENCE is disabled as MERGE_DATA is True")
    print("VLOOKUP is disabled as MERGE_DATA is True")
    REPLACE_DIFFERENCE = False
    VLOOKUP = False
elif REPLACE_DIFFERENCE:
    print("Merge is disabled as REPLACE_DIFFERENCE is True")
    print("VLookup is disabled as REPLACE_DIFFERENCE is True")
    MERGE_DATA = False
    VLOOKUP = False
elif VLOOKUP:
    print("MERGE_DATA is disabled as VLOOKUP is True")
    print("VLOOKUP is disabled as VLOOKUP is True")
    REPLACE_DIFFERENCE = False
    MERGE_DATA = False
"""
# Reading File
master_data = read(master_file_location,
                   VALUES_TO_BE_REPLACED_BY_NULL)
child_data = read(child_file_location,
                  VALUES_TO_BE_REPLACED_BY_NULL)
# Cleaning File
if CLEAN_EMPTY_COLUMNS:
    master_data = clean_dataframe(master_data)
    child_data = clean_dataframe(child_data)

check_memory(1)

# Merging Header
master_data, child_data = normalize_header(master_data, child_data, PARENT_PRIMARY_KEY,
                                           CHILD_FOREIGN_KEY, MERGE_HEADERS)

# Adding Source Column
if ADD_SOURCE_COLUMN:
    master_data = add_header(
        master_data, SOURCE_COLUMN_NAME, PARENT_SOURCE_VALUE)
    child_data = add_header(child_data, SOURCE_COLUMN_NAME, CHILD_SOURCE_VALUE)

if VLOOKUP:
    master_data = add_header(
        master_data, PARENT_COLUMN_TO_PLACE_DATA, "")

check_memory(2)

end_time = time.time()
print("---------CSV Details-----------------")
print("Master Header count=>", len(master_data.columns))
print("Child Header count=>", len(child_data.columns))
print("Master rows count=>", len(master_data.index))
print("Child rows count=>", len(child_data.index))
print("Reading CSV took %.2f seconds" % (end_time-start_time))
print("--------------------------")


start_time = time.time()
# Getting all rows that has null value in indexed column
master_non_indexed = get_nonIndexed(master_data, PARENT_PRIMARY_KEY)
child_non_indexed = get_nonIndexed(child_data, CHILD_FOREIGN_KEY)

# Indexing rows by specifed column name
master_data = get_indexed(master_data, PARENT_PRIMARY_KEY)
child_data = get_indexed(child_data, CHILD_FOREIGN_KEY)

end_time = time.time()
check_memory(3)

print("---------Dataframe Details-----------------")
print("Master Non indexed rows count=>", len(master_non_indexed))
print("Master rows count=>", len(master_data))
print("Child non indexed rows count=>", len(child_non_indexed))
print("Child rows count=>", len(child_data))
print("Proccesing CSV took %.2f seconds" % (end_time-start_time))
print("Master CSV Memory =>", master_data.memory_usage(index=True).sum())
print("Child CSV Memory =>", child_data.memory_usage(index=True).sum())
print("--------------------------")

# creating files
processed_data = pd.DataFrame(columns=master_data.columns)
log_data = pd.DataFrame(columns=["Id", "Comment"])
file_location = master_file_location.rsplit('\\', 1)[0]
file_location = file_location+'/Processed'
create_file(processed_data, PROCESSED_FILE_NAME, file_location)
create_file(log_data, LOG_FILE_NAME, file_location)


# Save Uniques to csv and delete dataframe
if OUTER_JOIN:
    processed_data = processed_data.append(
        child_non_indexed, ignore_index=True)
    processed_data = processed_data.append(
        master_non_indexed, ignore_index=True)
    append_to_file(processed_data, PROCESSED_FILE_NAME, file_location)
    print("Non Indexed Rows added to csv=>", len(processed_data))
    processed_data = processed_data.iloc[0:0]
del child_non_indexed
del master_non_indexed

check_memory(4)

Itration_count = 0
Itration_total = len(master_data.index)

child_columns = child_data.columns[~child_data.columns.isin(
    [CHILD_FOREIGN_KEY, SOURCE_COLUMN_NAME])]

for master_id in master_data.index:
    master_record_ref = master_data.loc[master_id]
    Itration_count = Itration_count + 1
    isMatch = False
    try:
        child_record_ref = child_data.loc[master_id]
        if type(child_record_ref) == pd.core.frame.DataFrame:
            child_record_ref = child_record_ref.iloc[0, :]

        if REPLACE_DIFFERENCE:
            for column in child_columns:
                if master_record_ref[column] != child_record_ref[column]:
                    master_record_ref[column] = child_record_ref[column]
                    log_data = log_data.append(
                        {
                            "Id": master_id,
                            "Comment": column+" Replaced from child"
                        },
                        ignore_index=True,
                    )
                    append_to_file(
                        log_data, LOG_FILE_NAME, file_location)
                    log_data = log_data.iloc[0:0]
        elif MERGE_DATA:
            for column in child_columns:
                if pd.notnull(child_record_ref[column]):
                    if OVERRIDE_PARENTDATA:
                        master_record_ref[column] = child_record_ref[column]
                    elif pd.isnull(master_record_ref[column]):
                        master_record_ref[column] = child_record_ref[column]
        elif VLOOKUP:
            master_record_ref[PARENT_COLUMN_TO_PLACE_DATA] = child_record_ref[CHILD_COLUMN_TO_FETCH_DATA]

        isMatch = True

        # Droping matched row
        child_data = child_data.drop(
            child_record_ref[CHILD_FOREIGN_KEY], axis=0)
    except KeyError as e:
        log_data = log_data.append(
            {
                "Id": master_id,
                "Comment": "Record not found in child"
            },
            ignore_index=True,
        )
        # Appending to csv and reset dataframe
        append_to_file(log_data, LOG_FILE_NAME, file_location)
        log_data = log_data.iloc[0:0]

    # Adding value to source column
    if ADD_SOURCE_COLUMN:
        if isMatch:
            master_record_ref[SOURCE_COLUMN_NAME] = PARENT_SOURCE_VALUE + \
                '/'+CHILD_SOURCE_VALUE

    # Adding Parent Records
    if OUTER_JOIN:
        processed_data = processed_data.append(
            master_record_ref, ignore_index=True)
    elif isMatch:
        processed_data = processed_data.append(
            master_record_ref, ignore_index=True)

    if Itration_count == Itration_total:
        # Adding Remaing Child Records
        if OUTER_JOIN:
            processed_data = processed_data.append(
                child_data, ignore_index=True)
        append_to_file(processed_data, PROCESSED_FILE_NAME, file_location)
        processed_data = processed_data.iloc[0:0]
    elif Itration_count == CHUNK:
        CHUNK = CHUNK + BATCH
        append_to_file(processed_data, PROCESSED_FILE_NAME, file_location)
        processed_data = processed_data.iloc[0:0]
        check_memory(Itration_count)

    printProgressBar(
        Itration_count, Itration_total, length=50,
    )
"""
