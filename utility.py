# importing dependency
import psutil
import gc
import os

# Deifineing Variabled
FOLDER = "Data/"
CHUNK = 200


# Clear Merory


def clean():
    n = gc.collect()
    print("--------------------------")
    print("Unreachable objects:", n)
    print("Remaining Garbage:", gc.garbage)


# Check Memory Consuption


def check_memory(number):
    clean()
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


def create_merge_folders():
    os.makedirs(FOLDER + "MERGE/Parent", exist_ok=True)
    os.makedirs(FOLDER + "MERGE/Child", exist_ok=True)
    os.makedirs(FOLDER + "MERGE/Result", exist_ok=True)
    os.makedirs(FOLDER + "MERGE/Log", exist_ok=True)


def create_lookup_folders():
    os.makedirs(FOLDER + "VLOOKUP/Parent", exist_ok=True)
    os.makedirs(FOLDER + "VLOOKUP/Child", exist_ok=True)
    os.makedirs(FOLDER + "VLOOKUP/Result", exist_ok=True)
    os.makedirs(FOLDER + "VLOOKUP/Log", exist_ok=True)
