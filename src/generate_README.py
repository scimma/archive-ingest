#!/usr/bin/env python3
"""
process README.template into README.md

This command uses python to gather the
non-static date and python formatting
to populate the tempate.

The populated template is printed on STDOUT.
"""

import subprocess
import datetime
import sys

# Collect data of interest

result = subprocess.run([" ./housekeeping.py"], shell=True, capture_output=True)
housekeeping_help = result.stdout.decode("utf-8")

result = subprocess.run([" ./houseutils.py"], shell=True, capture_output=True)
houseutils_help = result.stdout.decode("utf-8")

result = subprocess.run([" ./houseutils.py"], shell=True, capture_output=True)
housekeeping_run = result.stdout.decode("utf-8")


when = datetime.datetime.now().isoformat()

program = sys.argv[0]

#Get template, format it  and print to stdout
with open("README.template","r") as t:
    template = t.read()
    
x = template.format(
    housekeeping_help=housekeeping_help,
    houseutils_help=houseutils_help,
    housekeeping_run=housekeeping_run,
    date = when,
    program = program
    )
print(x)
