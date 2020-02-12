# importing dependency
import pandas as pd
import numpy as np
import PySimpleGUI as sg


import time
import sys
import traceback


# importing methods
from Support.utility import check_memory, read_file, printProgressBar, FOLDER, MERGEFOLDER, CHUNK, save

# importing fileds
from parameters import (
    FILE_NAME,
    PARENT_KEY_FILED,
    CHILD_KEY_FIELD,
    VALUES_TO_BE_REPLACED_BY_NULL,
    COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED,
    MERGE,
    OVERRIDE,
)

# declaring variables
Object = FILE_NAME
VALUES_TO_BE_REPLACED_BY_NAN = VALUES_TO_BE_REPLACED_BY_NULL
PARENT_ORG_UNIQUE_FILED = PARENT_KEY_FILED
CHILD_ORG_REF_FIELD = CHILD_KEY_FIELD
FIELD_NAME_TO_STORE_SOURCE = COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED
MERGE_TYPE = MERGE
OVERRIDE_MASTERDATA = OVERRIDE
# parameters for processing csv
MERGEHEADERS = True
DROPEMPTYHEADERS = True
DATACLEANUP = True
DROPEMPTYROWS = True


# defineing functions


def merge_header(master_csv, child_csv):
    master_colums = master_csv.columns
    child_colums = child_csv.columns
    unique_in_child = child_colums[~child_colums.isin(master_colums)]
    print("---------Header Details-----------------")
    print("%s columns form child are added to master" % len(unique_in_child))
    print("--------------------------")
    unique_colum_for_master = pd.DataFrame(
        columns=unique_in_child, dtype=object)

    return (
        pd.concat([master_csv, unique_colum_for_master], axis=1),
        child_csv
    )


def read(object_name):
    read_start = time.time()
    master_data, child_data = read_file(FOLDER, MERGEFOLDER, object_name)
    check_memory(0)
    if DATACLEANUP:
        master_data.replace(VALUES_TO_BE_REPLACED_BY_NAN, np.nan, inplace=True)
        child_data.replace(VALUES_TO_BE_REPLACED_BY_NAN, np.nan, inplace=True)
    if DROPEMPTYHEADERS:
        master_data.dropna(axis=1, how="all", inplace=True)
        child_data.dropna(axis=1, how="all", inplace=True)
    if DROPEMPTYROWS:
        master_data.dropna(axis=0, how="all", inplace=True)
        child_data.dropna(axis=0, how="all", inplace=True)
    if MERGEHEADERS:
        master_data, child_data = merge_header(master_data, child_data)
    master_coloum = {FIELD_NAME_TO_STORE_SOURCE: "Master"}
    child_coloum = {FIELD_NAME_TO_STORE_SOURCE: "Child"}
    master_data = master_data.assign(**master_coloum)
    child_data = child_data.assign(**child_coloum)
    read_end = time.time()
    print("---------CSV Details-----------------")
    print("Master Header count=>", len(master_data.columns))
    print("Child Header count=>", len(child_data.columns))
    print("Master rows count=>", len(master_data.index))
    print("child rows count=>", len(child_data.index))
    print("Reading CSV took %.2f seconds" % (read_end-read_start))
    print("--------------------------")

    check_memory(1)
    return (
        master_data[master_data[PARENT_ORG_UNIQUE_FILED].isna()
                    ].reset_index(drop=True),
        master_data[~master_data[PARENT_ORG_UNIQUE_FILED].isna()].set_index(
            PARENT_ORG_UNIQUE_FILED, drop=False
        ),
        child_data[child_data[CHILD_ORG_REF_FIELD].isna()
                   ].reset_index(drop=True),
        child_data[~child_data[CHILD_ORG_REF_FIELD].isna()].set_index(
            CHILD_ORG_REF_FIELD, drop=False
        ),
    )


def export(csv_data_frame):
    save(
        FOLDER + MERGEFOLDER + "Result/" + Object + ".csv", csv_data_frame
    )


def log(csv_data_frame):
    save(FOLDER + MERGEFOLDER + "Log/" + Object + ".csv", csv_data_frame)


if len(sys.argv) == 7:
    Object, PARENT_ORG_UNIQUE_FILED, CHILD_ORG_REF_FIELD, FIELD_NAME_TO_STORE_SOURCE, MERGE_TYPE, OVERRIDE_MASTERDATA = sys.argv[
        1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
    if OVERRIDE_MASTERDATA == "False":
        OVERRIDE_MASTERDATA = False
    else:
        OVERRIDE_MASTERDATA = True


# Main Program
MASTER_RECORDS_UNIQUES, MASTER_RECORDS, CHILD_RECORDS_UNIQUES, CHILD_RECORDS = read(
    Object
)
MERGED_RECORDS = pd.DataFrame(columns=MASTER_RECORDS.columns)
LOG = pd.DataFrame(columns=["REF KEY", "ORIGIN", "Comment"])
print("---------Dataframe Details-----------------")
print("Master Non indexed rows count=>", len(MASTER_RECORDS_UNIQUES))
print("Master rows count=>", len(MASTER_RECORDS))
print("Child non indexed rows count=>", len(CHILD_RECORDS_UNIQUES))
print("Child rows count=>", len(CHILD_RECORDS))
print("--------------------------")

# Save Uniques to csv
if MERGE_TYPE == "outer":
    MERGED_RECORDS = MERGED_RECORDS.append(
        MASTER_RECORDS_UNIQUES, ignore_index=True)
    MERGED_RECORDS = MERGED_RECORDS.append(
        CHILD_RECORDS_UNIQUES, ignore_index=True)

MERGED_RECORDS.to_csv(FOLDER + MERGEFOLDER + "Result/" + Object +
                      ".csv", index=False, mode="w")
LOG.to_csv(FOLDER + MERGEFOLDER + "Log/" +
           Object + ".csv", index=False, mode="w")

# reset dataframe as we dont need saved records in memory
MERGED_RECORDS = MERGED_RECORDS.iloc[0:0]
LOG = LOG.iloc[0:0]
del MASTER_RECORDS_UNIQUES
del CHILD_RECORDS_UNIQUES
print("Master CSV Memory =>", MASTER_RECORDS.memory_usage(index=True).sum())
print("Child CSV Memory =>", CHILD_RECORDS.memory_usage(index=True).sum())
check_memory(2)

CHUNK_INDEX = 0
Progress_sum = len(MASTER_RECORDS.index)
printProgressBar(
    CHUNK_INDEX, Progress_sum, length=50,
)
Duplicate_Not_Found_error = 0
read_start = time.time()

# helper variables
child_coluums = CHILD_RECORDS.columns
try:
    for master_id in MASTER_RECORDS.index:
        CHUNK_INDEX = CHUNK_INDEX + 1
        master_record_ref = MASTER_RECORDS.loc[master_id]
        isMatch = True
        try:
            child_record_ref = CHILD_RECORDS.loc[master_id]
            if type(child_record_ref) == pd.core.frame.DataFrame:
                child_record_ref = child_record_ref.iloc[0, :]

            # comparing each cell value based on header
            for field_name in child_coluums:
                if pd.notnull(child_record_ref[field_name]):
                    if OVERRIDE_MASTERDATA:
                        master_record_ref[field_name] = child_record_ref[field_name]
                    elif pd.isnull(master_record_ref[field_name]):
                        master_record_ref[field_name] = child_record_ref[field_name]
        except KeyError:
            isMatch = False
            Duplicate_Not_Found_error = Duplicate_Not_Found_error + 1
            LOG = LOG.append(
                {
                    "REF KEY": master_id,
                    "ORIGIN": "Master",
                    "Comment": "Refrence Record not found in child org",
                },
                ignore_index=True,
            )
            log(LOG)
            LOG = LOG.iloc[0:0]
        if MERGE_TYPE == "outer":
            master_record_ref[FIELD_NAME_TO_STORE_SOURCE] = (
                isMatch and "Master/Child" or "Master"
            )
            MERGED_RECORDS = MERGED_RECORDS.append(
                master_record_ref, ignore_index=True)
        elif isMatch:
            master_record_ref[FIELD_NAME_TO_STORE_SOURCE] = "Master/Child"
            MERGED_RECORDS = MERGED_RECORDS.append(
                master_record_ref, ignore_index=True)
        # writing data to csv in chunks of 200 records to reduce memory
        if CHUNK_INDEX == len(MASTER_RECORDS.index):
            print("\n")
            read_end = time.time()
            print("CHUNK COUNT => %s " % CHUNK_INDEX)
            print("Current Chunk took %.2f seconds to process" %
                  (read_end-read_start))
            print("Refrence not Found Count %s" % Duplicate_Not_Found_error)
            print("Remaing Child records %s" % len(CHILD_RECORDS))
            Duplicate_Not_Found_error = 0
            if MERGE_TYPE == "outer":
                temp = {FIELD_NAME_TO_STORE_SOURCE: "Child"}
                CHILD_RECORDS = CHILD_RECORDS.assign(**temp)
                MERGED_RECORDS = MERGED_RECORDS.append(
                    CHILD_RECORDS, ignore_index=True)
            export(MERGED_RECORDS)
            MERGED_RECORDS = MERGED_RECORDS.iloc[0:0]
            check_memory(CHUNK_INDEX)
        elif CHUNK_INDEX == CHUNK:
            print("\n")
            read_end = time.time()
            print("CHUNK COUNT => %s " % CHUNK)
            print("Current Chunk took %.2f seconds to process" %
                  (read_end-read_start))
            read_start = time.time()
            print("Refrence not Found Count %s" % Duplicate_Not_Found_error)
            Duplicate_Not_Found_error = 0
            export(MERGED_RECORDS)
            CHUNK = CHUNK + CHUNK
            MERGED_RECORDS = MERGED_RECORDS.iloc[0:0]
            check_memory(CHUNK)
        printProgressBar(
            CHUNK_INDEX, Progress_sum, prefix="Progress: "+str(CHUNK_INDEX), length=50,
        )
except Exception as e:
    print("Error", e)
    print(traceback.format_exc())
    print(traceback.print_stack())
