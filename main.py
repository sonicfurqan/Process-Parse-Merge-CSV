import PySimpleGUI as sg
import os
import sys


from parameters import (
    FILE_NAME,
    PARENT_KEY_FILED,
    CHILD_KEY_FIELD,
    COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED,
)


from parameters import (
    TARGET_FILE_NAME,
    TARGET_FILE_KEY_COLUMN_NAME,
    LOOKUP_FILE_NAME,
    LOOKUP_FILE_KEY_COLUMN_NAME,
    LOOKUP_FILE_VALUE_COLUMN_NAME,
)


layout = [
    [sg.Text('Please enter parmeters for merge processing')],
    [sg.Text('Place Master File under Data/MERGE/Master folder')],
    [sg.Text('Place Child File under Data/MERGE/Child folder')],
    [sg.Text('File Name', size=(15, 1)), sg.InputText('Example')],
    [sg.Text('Master File Key Column', size=(15, 1)),
     sg.InputText('Id')],
    [sg.Text('Child File Key Column', size=(15, 1)),
     sg.InputText('EXT_Id')],
    [sg.Text('Merged File Source Column Name', size=(15, 1)),
     sg.InputText('SOURCE')],
    [sg.Checkbox('outer Join', default=True),  sg.Checkbox(
        'over write data', default=False)],
    [sg.OK()],
    [sg.Text('Please enter parmeters for vlookup processing')],
    [sg.Text('Place Master File under Data/VLOOKUP/Target folder')],
    [sg.Text('Place Child File under Data/VLOOKUP/Lookup folder')],
    [sg.Text('Target File Name', size=(15, 1)), sg.InputText('Example_new')],
    [sg.Text('Target File Key Column', size=(15, 1)),
     sg.InputText('Id')],
    [sg.Text('Loohup File Name', size=(15, 1)), sg.InputText('Example_old')],
    [sg.Text('Child File Key Column', size=(15, 1)),
     sg.InputText('Old_Id')],
    [sg.Text('Child File Value Column Name', size=(15, 1)),
     sg.InputText('New_id')],
    [sg.Submit()],

    [sg.Cancel()]
]

window = sg.Window('Enter Parameters').Layout(layout)
button, values = window.Read()
py_command="python"
if sys.version_info[0] < 3:
    py_command="python3"    


if button == "OK":
    FILE_NAME, PARENT_KEY_FILED, CHILD_KEY_FIELD, COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED, MERGE, OVERRIDE = values[
        0], values[1], values[2], values[3], values[4], values[5]
    data = os.system("%s merge.py %s %s %s %s %s %r" % (py_command,FILE_NAME, PARENT_KEY_FILED,
                                                        CHILD_KEY_FIELD, COLOUM_NAME_WHERE_RECORD_SOURCE_TO_BE_STORED, (
                                                            MERGE and "outer" or "inner"
                                                        ), OVERRIDE))
elif button == "Submit":
    TARGET_FILE_NAME, TARGET_FILE_KEY_COLUMN_NAME, LOOKUP_FILE_NAME, LOOKUP_FILE_KEY_COLUMN_NAME, LOOKUP_FILE_VALUE_COLUMN_NAME = values[
        6], values[7], values[8], values[9], values[10]
    data = os.system("%s vlookup.py %s %s %s %s %s " % (py_command,TARGET_FILE_NAME, TARGET_FILE_KEY_COLUMN_NAME,
                                                        LOOKUP_FILE_NAME, LOOKUP_FILE_KEY_COLUMN_NAME, LOOKUP_FILE_VALUE_COLUMN_NAME))
