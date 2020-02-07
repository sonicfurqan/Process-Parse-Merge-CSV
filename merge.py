# importing dependency
import pandas as pd
import psutil
import gc
import dask.dataframe as dd
import numpy as np

# importing fileds
from support import (
    FILE_NAME,
    PARENT_KEY_FILED,
    CHILD_KEY_FIELD,
    VALUES_TO_BE_REPLACED_BY_NULL,
    COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED,
)

# declaring variables
Object = FILE_NAME
VALUES_TO_BE_REPLACED_BY_NAN = VALUES_TO_BE_REPLACED_BY_NULL
PARENT_ORG_UNIQUE_FILED = PARENT_KEY_FILED
CHILD_ORG_REF_FIELD = CHILD_KEY_FIELD
FIELD_NAME_TO_STORE_SOURCE = COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED
# parameters for processing csv
MERGHEADERS = True
DROPEMPTYHEADERS = True
DATACLEANUP = True
DROPEMPTYROWS = True

FOLDER = "Data/"
CHUNK = 200

# defineing functions


# clean memoery
def clean():
    n = gc.collect()
    print("--------------------------")
    print("Unreachable objects:", n)
    print("Remaining Garbage:")
    print(gc.garbage)
    print("--------------------------")


def check_memory(number):
    clean()
    print(
        "SEQUENCE => %d" % number,
        "MEMORY USED => %f Percent" % psutil.virtual_memory().percent,
        "Avilabel Memeory => %d" % psutil.virtual_memory().available,
        end="\n",
        sep=" | ",
    )


# Print iterations progress
def printProgressBar(
    iteration,
    total,
    prefix="Progress:",
    suffix="Complete:",
    decimals=1,
    length=100,
    fill="█",
    printEnd="\r",
):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print("\r%s |%s| %s%% %s" % (prefix, bar, percent, suffix), end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def merge_header(master_csv, child_csv):
    master_colums = master_csv.columns
    child_colums = child_csv.columns
    unique_in_child = child_colums[~child_colums.isin(master_colums)]
    unique_in_master = master_colums[~master_colums.isin(child_colums)]
    print("---------Header Details-----------------")
    print("%s columns form child are added to master" % len(unique_in_child))
    print("%s columns form master are added to child" % len(unique_in_master))
    print("--------------------------")
    unique_colum_for_master = pd.DataFrame(columns=unique_in_child, dtype=object)
    unique_colum_for_child = pd.DataFrame(columns=unique_in_master, dtype=object)

    return (
        pd.concat([master_csv, unique_colum_for_master], axis=1),
        pd.concat([child_csv, unique_colum_for_child], axis=1),
    )


def read(object_name):
    master_csv = FOLDER + "Master/" + object_name + ".csv"
    child_csv = FOLDER + "Child/" + object_name + ".csv"
    check_memory(0)
    master_data = pd.read_csv(master_csv, skip_blank_lines=True, sep=",", dtype=object,)
    child_data = pd.read_csv(child_csv, skip_blank_lines=True, sep=",", dtype=object,)
    if DATACLEANUP:
        master_data.replace(VALUES_TO_BE_REPLACED_BY_NAN, np.nan, inplace=True)
        child_data.replace(VALUES_TO_BE_REPLACED_BY_NAN, np.nan, inplace=True)
    if DROPEMPTYHEADERS:
        master_data.dropna(axis=1, how="all", inplace=True)
        child_data.dropna(axis=1, how="all", inplace=True)
    if DROPEMPTYROWS:
        master_data.dropna(axis=0, how="all", inplace=True)
        child_data.dropna(axis=0, how="all", inplace=True)
    if MERGHEADERS:
        master_data, child_data = merge_header(master_data, child_data)
    master_coloum = {FIELD_NAME_TO_STORE_SOURCE: "Master"}
    child_coloum = {FIELD_NAME_TO_STORE_SOURCE: "Child"}
    master_data = master_data.assign(**master_coloum)
    child_data = child_data.assign(**child_coloum)
    print("---------CSV Details-----------------")
    print("Master Header count=>", len(master_data.columns))
    print("Child Header count=>", len(child_data.columns))
    print("Master rows count=>", len(master_data.index))
    print("child rows count=>", len(child_data.index))
    print("--------------------------")
    check_memory(1)
    return (
        master_data[master_data[PARENT_ORG_UNIQUE_FILED].isna()].reset_index(drop=True),
        master_data[~master_data[PARENT_ORG_UNIQUE_FILED].isna()].set_index(
            PARENT_ORG_UNIQUE_FILED, drop=False
        ),
        child_data[child_data[CHILD_ORG_REF_FIELD].isna()].reset_index(drop=True),
        child_data[~child_data[CHILD_ORG_REF_FIELD].isna()].set_index(
            CHILD_ORG_REF_FIELD, drop=False
        ),
    )


def export(csv_data_frame):
    csv_data_frame.to_csv(
        FOLDER + "Merged/" + Object + ".csv", index=False, mode="a", header=False
    )


def log(csv_data_frame):
    csv_data_frame.to_csv(
        FOLDER + "Log/" + Object + ".csv", index=False, mode="a", header=False
    )


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

MERGED_RECORDS = MERGED_RECORDS.append(MASTER_RECORDS_UNIQUES, ignore_index=True)
MERGED_RECORDS = MERGED_RECORDS.append(CHILD_RECORDS_UNIQUES, ignore_index=True)
MERGED_RECORDS.to_csv(FOLDER + "Merged/" + Object + ".csv", index=False, mode="w")
LOG.to_csv(FOLDER + "Log/" + Object + ".csv", index=False, mode="w")
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
for master_id in MASTER_RECORDS.index:
    CHUNK_INDEX = CHUNK_INDEX + 1
    isMatch = True
    master_record_ref = MASTER_RECORDS.loc[master_id]
    try:
        isMatch = True
        child_record_ref = CHILD_RECORDS.loc[master_id]
        CHILD_RECORDS.drop(child_record_ref[CHILD_ORG_REF_FIELD], axis=0, inplace=True)
        # checking if master record column is empty then copy the value from the child
        for field_name in MASTER_RECORDS.columns:
            if MERGHEADERS:
                if pd.isnull(master_record_ref[field_name]) & pd.notnull(
                    child_record_ref[field_name]
                ):
                    master_record_ref[field_name] = child_record_ref[field_name]
            elif (
                field_name
                in MASTER_RECORDS.columns & field_name
                in CHILD_RECORDS.columns
            ):
                if pd.isnull(master_record_ref[field_name]) & pd.notnull(
                    child_record_ref[field_name]
                ):
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

    master_record_ref[FIELD_NAME_TO_STORE_SOURCE] = (
        isMatch and "Master/Child" or "Master"
    )
    MERGED_RECORDS = MERGED_RECORDS.append(master_record_ref, ignore_index=True)
    # writing data to csv in chunks of 200 records to reduce memory
    if CHUNK_INDEX == len(MASTER_RECORDS.index):
        print("\n")
        print("CHUNK COUNT => %s " % CHUNK_INDEX)
        print("Refrence not Found Count %s" % Duplicate_Not_Found_error)
        print("Remaing Child records %s" % len(CHILD_RECORDS))
        Duplicate_Not_Found_error = 0
        temp = {FIELD_NAME_TO_STORE_SOURCE: "Child"}
        CHILD_RECORDS = CHILD_RECORDS.assign(**temp)
        MERGED_RECORDS = MERGED_RECORDS.append(CHILD_RECORDS, ignore_index=True)
        export(MERGED_RECORDS)
        MERGED_RECORDS = MERGED_RECORDS.iloc[0:0]
        check_memory(CHUNK_INDEX)
    elif CHUNK_INDEX == CHUNK:
        print("\n")
        print("CHUNK COUNT => %s " % CHUNK)
        print("Refrence not Found Count %s" % Duplicate_Not_Found_error)
        Duplicate_Not_Found_error = 0
        export(MERGED_RECORDS)
        CHUNK = CHUNK + CHUNK
        MERGED_RECORDS = MERGED_RECORDS.iloc[0:0]
        check_memory(CHUNK)
    printProgressBar(
        CHUNK_INDEX, Progress_sum, length=50,
    )

