import subprocess
import sys

try:
    import PySimpleGUI
except ImportError:
    print("Trying to Install required module: PySimpleGUI\n")
    subprocess.call('pip install PySimpleGUI')

try:
    import pandas
except ImportError:
    print("Trying to Install required module: pandas\n")
    subprocess.call('pip install pandas')

try:
    import numpy
except ImportError:
    print("Trying to Install required module: numpy\n")
    subprocess.call('pip install numpy')

try:
    import psutil
except ImportError:
    print("Trying to Install required module: psutil\n")
    subprocess.call('pip install psutil')

py_command = "python"
if sys.version_info[0] < 3:
    py_command = "python3"
