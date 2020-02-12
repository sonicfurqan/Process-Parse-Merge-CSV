import os

try:
  import PySimpleGUI
except ImportError:
  print "Trying to Install required module: PySimpleGUI\n"
  os.system('pip install PySimpleGUI')

try:
  import pandas
except ImportError:
  print "Trying to Install required module: pandas\n"
  os.system('pip install pandas')

try:
  import numpy
except ImportError:
  print "Trying to Install required module: numpy\n"
  os.system('pip install numpy')

try:
  import psutil
except ImportError:
  print "Trying to Install required module: psutil\n"
  os.system('pip install psutil')

py_command="python"
if sys.version_info[0] < 3:
    py_command="python3"    

os.system("%s main.py",py_command)