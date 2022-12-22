<!-- a comment -->
<!-- use generateREADME.py to process this into README.md -->

# Housekeeping Software

Housekeeping uses three system elements.

- Sources which  hopskotch or mocks
- Databases which are either AWS rds Postgres of mocks
- Stores which can be AWS S3 store or mocks

Realized (i.e non-mock) elements can be either from the SCiMMA
development or producion instances as appropriate.  The softare
is such that there is a configuraton to source  production hopskotch
while developing. 


## Housekeeping.py

Housekeeping.py is the program that loads the housekeeping archive.
Housekeepling.py can be run during development against the live ements
in the deevepoment instance or mock elements. It's pososbel to use the
the production HOP as a data source in development.

```
usage: housekeeping.py [-h] [-t TOML_FILE] [-l LOG_STANZA] {run,list} ...

Log messages and metadata to S3. provide metadate to a databse.

In production, log events from selected Hopscotch public
topics into S3 and log into a housekeeping postgres database.

to suport devleopment, Houskeeping.py  supports realized or mock
hopskotch, database and store elements.  These are defined in
toml files. Housekeeping.toml defines a variety of these sources. 

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

positional arguments:
  {run,list}
    run                 house keep w/(defaults) all mocks
    list                list stanzas

optional arguments:
  -h, --help            show this help message and exit
  -t TOML_FILE, --toml_file TOML_FILE
                        toml configuration file
  -l LOG_STANZA, --log_stanza LOG_STANZA
                        log config stanza

```

## Housekeepeing.toml

Housekeeping.toml holds configuration data for the producion and
development instnances of housekeeping.py, and for houseutils.py


##  Houseutils.py

Houseutils.py contains a number of sub funcitons used to test
and access the running system.

```
usage: houseutils.py [-h] [-t TOML_FILE] [-l LOG_STANZA]
                     {list,query_session,publish} ...

Utiliteis for housekeepign applications.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

positional arguments:
  {list,query_session,publish}
    list                list stanzas
    query_session       Launch a query session shell against AWS databases
    publish             publish some test data

optional arguments:
  -h, --help            show this help message and exit
  -t TOML_FILE, --toml_file TOML_FILE
                        toml configuration file
  -l LOG_STANZA, --log_stanza LOG_STANZA
                        log config stanza

``

## generate_README.py

Generate_README.py makes this  README by processing README.template.

This document was generated on 2022-12-22T15:27:51.991068 by ./generate_README.py
