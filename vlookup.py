# importing dependency

import pandas as pd
import numpy as np

import time
import sys
import traceback


from parameters import (
    VALUES_TO_BE_REPLACED_BY_NULL,
    TARGET_FILE_NAME,
    TARGET_FILE_KEY_COLUMN_NAME,
    LOOKUP_FILE_NAME,
    LOOKUP_FILE_KEY_COLUMN_NAME,
    LOOKUP_FILE_VALUE_COLUMN_NAME,
)

# importing methods
from utility import check_memory, create_lookup_folders, printProgressBar, FOLDER, CHUNK

FOLDER = FOLDER+"VLOOKUP/"

# defineing methods


def export(csv_data_frame):
    csv_data_frame.to_csv(
        FOLDER + "Result/" + TARGET_FILE_NAME + ".csv", index=False, mode="a", header=False
    )


def log(csv_data_frame):
    csv_data_frame.to_csv(
        FOLDER + "Log/" + TARGET_FILE_NAME + ".csv", index=False, mode="a", header=False
    )


def read(master_file_name, child_file_name):
    read_start = time.time()
    create_lookup_folders()
    master_csv = FOLDER + "Parent/" + master_file_name + ".csv"
    child_csv = FOLDER + "Child/" + child_file_name + ".csv"
    check_memory(0)
    encoding_europ = "ISO-8859-1"
    encoding_default = "utf8"
    try:
        master_data = pd.read_csv(
            master_csv, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_default)
        child_data = pd.read_csv(
            child_csv, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_default)
    except:
        print("Fallback to europe encoding")
        master_data = pd.read_csv(
            master_csv, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_europ)
        child_data = pd.read_csv(
            child_csv, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_europ)
    # Data clean Up
    master_data.replace(VALUES_TO_BE_REPLACED_BY_NULL, np.nan, inplace=True)
    child_data.replace(VALUES_TO_BE_REPLACED_BY_NULL, np.nan, inplace=True)
    # Drop empty rows and colums
    master_data.dropna(axis=1, how="all", inplace=True)
    child_data.dropna(axis=1, how="all", inplace=True)
    master_data.dropna(axis=0, how="all", inplace=True)
    child_data.dropna(axis=0, how="all", inplace=True)

    child_columns_list = child_data.columns
    child_columns_list = child_columns_list[~child_columns_list.isin(
        [LOOKUP_FILE_VALUE_COLUMN_NAME, LOOKUP_FILE_KEY_COLUMN_NAME])]
    child_data.drop(child_columns_list, axis=1, inplace=True)

    read_end = time.time()
    print("---------CSV Details-----------------")
    print("Target Header count=>", len(master_data.columns))
    print("Lookup Header count=>", len(child_data.columns))
    print("Target rows count=>", len(master_data.index))
    print("Lookup rows count=>", len(child_data.index))
    print("Reading CSV took %.2f seconds" % (read_end-read_start))
    print("--------------------------")

    check_memory(1)
    return(
        master_data[master_data[TARGET_FILE_KEY_COLUMN_NAME].isna()
                    ].reset_index(drop=True),
        master_data[~master_data[TARGET_FILE_KEY_COLUMN_NAME].isna()].set_index(
            TARGET_FILE_KEY_COLUMN_NAME, drop=False),
        child_data.set_index(LOOKUP_FILE_KEY_COLUMN_NAME, drop=False))


if len(sys.argv) == 6:
    TARGET_FILE_NAME, TARGET_FILE_KEY_COLUMN_NAME, LOOKUP_FILE_NAME, LOOKUP_FILE_KEY_COLUMN_NAME, LOOKUP_FILE_VALUE_COLUMN_NAME = sys.argv[
        1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]


# main program
MASTER_NULL_RECORDS, MASTER_RECORDS, LOOKUP_RECORDS = read(
    TARGET_FILE_NAME, LOOKUP_FILE_NAME)

print("---------Dataframe Details-----------------")
print("Target empty keys count=>", len(MASTER_NULL_RECORDS))
print("Target rows count=>", len(MASTER_RECORDS))
print("Lookup rows count=>", len(LOOKUP_RECORDS))
print("--------------------------")

PROCESSED_RECORDS = pd.DataFrame(columns=MASTER_RECORDS.columns)
PROCESSED_RECORDS = PROCESSED_RECORDS.append(
    MASTER_NULL_RECORDS, ignore_index=True)
PROCESSED_RECORDS.to_csv(FOLDER + "Result/" + TARGET_FILE_NAME +
                         ".csv", index=False, mode="w")
PROCESSED_RECORDS = PROCESSED_RECORDS.iloc[0:0]

LOG = pd.DataFrame(columns=["REF KEY", "ORIGIN", "Comment"])
LOG.to_csv(FOLDER + "Log/" + TARGET_FILE_NAME + ".csv", index=False, mode="w")

print("Master CSV Memory =>", MASTER_RECORDS.memory_usage(index=True).sum())
print("Child CSV Memory =>", LOOKUP_RECORDS.memory_usage(index=True).sum())
check_memory(2)


CHUNK_INDEX = 0
Progress_sum = len(MASTER_RECORDS.index)
printProgressBar(
    CHUNK_INDEX, Progress_sum, length=50,
)
Lookup_not_found = 0
read_start = time.time()

# helper variables
master_coloums = MASTER_RECORDS.columns
child_coluums = LOOKUP_RECORDS.columns
try:
    for master_id in MASTER_RECORDS.index:
        CHUNK_INDEX = CHUNK_INDEX + 1
        master_record_ref = MASTER_RECORDS.loc[master_id]
        try:
            child_record_ref = LOOKUP_RECORDS.loc[master_id]
            if type(child_record_ref) == pd.core.frame.DataFrame:
                child_record_ref = child_record_ref.iloc[0, :]
            master_record_ref[TARGET_FILE_KEY_COLUMN_NAME] = child_record_ref[LOOKUP_FILE_VALUE_COLUMN_NAME]
        except KeyError:
            Lookup_not_found = Lookup_not_found + 1
            LOG = LOG.append(
                {
                    "REF KEY": master_id,
                    "ORIGIN": "Master",
                    "Comment": "Refrence Record not found in child csv",
                },
                ignore_index=True,
            )
            log(LOG)
            LOG = LOG.iloc[0:0]
        PROCESSED_RECORDS = PROCESSED_RECORDS.append(
            master_record_ref, ignore_index=True)
        # writing data to csv in chunks of 200 records to reduce memory
        if CHUNK_INDEX == len(MASTER_RECORDS.index):
            print("\n")
            read_end = time.time()
            print("CHUNK COUNT => %s " % CHUNK_INDEX)
            print("Current Chunk took %.2f seconds to process" %
                  (read_end-read_start))
            print("Refrence not Found Count %s" % Lookup_not_found)
            Lookup_not_found = 0
            export(PROCESSED_RECORDS)
            PROCESSED_RECORDS = PROCESSED_RECORDS.iloc[0:0]
            check_memory(CHUNK_INDEX)
        elif CHUNK_INDEX == CHUNK:
            print("\n")
            read_end = time.time()
            print("CHUNK COUNT => %s " % CHUNK)
            print("Current Chunk took %.2f seconds to process" %
                  (read_end-read_start))
            read_start = time.time()
            print("Refrence not Found Count %s" % Lookup_not_found)
            Lookup_not_found = 0
            export(PROCESSED_RECORDS)
            CHUNK = CHUNK + CHUNK
            PROCESSED_RECORDS = PROCESSED_RECORDS.iloc[0:0]
            check_memory(CHUNK)
        printProgressBar(
            CHUNK_INDEX, Progress_sum, prefix="Progress: "+str(CHUNK_INDEX), length=50,
        )
except Exception as e:
    print("Error", e)
    print(traceback.format_exc())
    print(traceback.print_stack())
