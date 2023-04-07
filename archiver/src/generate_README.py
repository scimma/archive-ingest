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

result = subprocess.run([" ./archive_ingest.py"], shell=True, capture_output=True)
archive_ingest_help = result.stdout.decode("utf-8")

result = subprocess.run([" ./ingestutils.py"], shell=True, capture_output=True)
ingestutils_help = result.stdout.decode("utf-8")

result = subprocess.run([" ./archive_ingest.py run -h"], shell=True, capture_output=True)
archive_ingest_run = result.stdout.decode("utf-8")


when = datetime.datetime.now().isoformat()

program = sys.argv[0]

#Get template, format it  and print to stdout
with open("README.template","r") as t:
    template = t.read()
    
x = template.format(
    archive_ingest_help=archive_ingest_help,
    ingestutils_help=ingestutils_help,
    archive_ingest_run=archive_ingest_run,
    date = when,
    program = program
    )
print(x)
