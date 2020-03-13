import psutil
import gc
import pandas as pd
import os


def read(file_name, additional_na_values):
    encoding_europe = "ISO-8859-1"
    encoding_default = "utf8"
    try:
        return pd.read_csv(
            file_name, index_col=False, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_default, na_values=additional_na_values)
    except:
        print("Fallback to europe Encoding")
        return pd.read_csv(
            file_name, index_col=False, skip_blank_lines=True, sep=",", dtype=object, encoding=encoding_europe,  na_values=additional_na_values)


def clean_dataframe(file):
    new_file = file.dropna(axis=1, how="all")  # Delete all empty columns
    new_file = new_file.dropna(axis=0, how="all")  # Delete all empty rows
    return new_file


def add_header(file, header_name, value):
    # Add new Coloum with Value to file
    column = {header_name: value}
    return file.assign(**column)


def create_file(file, fileName, location):
    # Create a direcotry and file
    os.makedirs(location, exist_ok=True)
    file.to_csv(location+'/'+fileName, index=False, mode="w")


def append_to_file(dataframe, fileName, location):
    dataframe.to_csv(
        location+'/'+fileName, index=False, mode="a", header=False
    )

# Check Memory Consuption


def check_memory(number):
    print("--------------------------")
    gc.collect()
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
