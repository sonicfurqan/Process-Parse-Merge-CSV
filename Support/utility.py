# importing dependency
import psutil
import gc
import os
import time
import pandas as pd


# Deifineing Variabled
FOLDER = "Data/"
CHUNK = 200
MERGEFOLDER = "MERGE/"
VLOOKUPFOLDER = "VLOOKUP/"
# Clear Merory


def __clean():
    n = gc.collect()
    print("--------------------------")
    print("Unreachable objects:", n)
    print("Remaining Garbage:", gc.garbage)


# Check Memory Consuption


def check_memory(number):
    __clean()
    print(
        "SEQUENCE => %d" % number,
        "MEMORY USED => %f Percent" % psutil.virtual_memory().percent,
        "Avilabel Memeory => %d" % psutil.virtual_memory().available,
        end="\n",
        sep=" | ",
    )
    print("--------------------------")


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
    percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                     (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print("\r%s |%s| %s%% %s" % (prefix, bar, percent, suffix), end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def __create_folder(FOLDER, SUBFOLDER):
    os.makedirs(FOLDER + SUBFOLDER + "/Parent", exist_ok=True)
    os.makedirs(FOLDER + SUBFOLDER + "/Child", exist_ok=True)
    os.makedirs(FOLDER + SUBFOLDER + "/Result", exist_ok=True)
    os.makedirs(FOLDER + SUBFOLDER + "/Log", exist_ok=True)


def read(file_name):
    encoding_europe = "ISO-8859-1"
    encoding_default = "utf8"
    try:
        return pd.read_csv(
            file_name, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_default)
    except:
        print("Fallback to europe Encoding")
        return pd.read_csv(
            file_name, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_europe)


def save(file_name, data):
    data.to_csv(
        file_name, index=False, mode="a", header=False
    )


def read_file(FOLDER, SUBFOLDER, file_name):
    __create_folder(FOLDER, SUBFOLDER)
    master_csv = FOLDER + SUBFOLDER + "Parent/" + file_name + ".csv"
    child_csv = FOLDER + SUBFOLDER + "Child/" + file_name + ".csv"
    master_data = read(master_csv)
    child_data = read(child_csv)
    return master_data, child_data
