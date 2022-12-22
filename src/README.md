<class 'str'>
[//]: # (Comment)
[//]: # (Comment)# use houseutils to process this template into README.
[//]: #

# Housekeeping.py

Housekeeping.py is the program that loads the housekeeping archive.
housekeepling.py can be run in development against eiths mock
elements.  live elements in the SCIMMA development instanitions, and
use the production HOP as a data source.

```
usage: housekeeping.py [-h] [-t TOML_FILE] [-l LOG_STANZA] {run,list} ...

Log messages and metadata to a database.

In production, log events from selected Hopscotch public
topics into the AWS-resident production housekeeping database.

Events may be also logged to the AWS-resident development
database, to a local sqilite database, or a "mock" database
that just discards them.

Events can be sourced from hopskotch, or from a "mock" source.  The
development direction for the mock source is to allow for stressing
the database engines and database provisioning to ensure they form a
robust store that is critical to SCiMMA operations.

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

# Housekeepeing.toml

Housekeeping.toml holds configuration data for the producion and
development instnances of housekeeping.py, and for houseutils.py


# Houseutils.py

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

This document was generated on 2022-12-22T14:59:39.933825
