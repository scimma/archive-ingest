
```
usage: housekeeping.py [-h] [-t TOML_FILE] [-l LOG_STANZA] {run,list,query,publish} ...

Log messages and metadata to a database.

In production, run is uses to log events from selected Hopscotch
public topics into the AWS-resident production housekeeping database.

Events may be also logged to the AWS-resident development
database,  or a "mock" database that ust discards them.

Events can be sourced from hopskotch, or from a "mock" source.  The
development direction for the mock source is to allow for stressing
the database engines and database provisioning to ensure they form a
robust store that is critical to SCiMMA operations, and to provide
a varaitey of messasges, to test that housekeeping is agnostic to
the format of messages, and header content.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

positional arguments:
  {run,list,query,publish}
    run                 house keep w/(defaults) all mocks
    list                list stanzas
    query               Launch a query shell against AWS databases
    publish             publish some test data

optional arguments:
  -h, --help            show this help message and exit
  -t TOML_FILE, --toml_file TOML_FILE
                        toml configuration file
  -l LOG_STANZA, --log_stanza LOG_STANZA
                        log config stanza
```

## run
```
usage: housekeeping.py run [-h] [-D DATABASE_STANZA] [-H HOP_STANZA]
                           [-S STORE_STANZA] [-t TOPIC]

optional arguments:
  -h, --help            show this help message and exit
  -D DATABASE_STANZA, --database_stanza DATABASE_STANZA
                        database-config-stanza
  -H HOP_STANZA, --hop_stanza HOP_STANZA
                        hopskotch config stanza
  -S STORE_STANZA, --store_stanza STORE_STANZA
                        storage config stanza
  -t TOPIC, --topic TOPIC
                        consume only this topic for consumption
```

## list
The list command simply prints the .toml file out as
a convenient memeory aid when specifying -D -H  ro -S switches

```
usage: housekeeping.py list [-h]

optional arguments:
  -h, --help  show this help message and exit
```
## query
The query subcommand conencts to the postgres database specified by
the -D option and starts and interactive  psql shell.
```
usage: housekeeping.py query [-h] [-D DATABASE_STANZA]

optional arguments:
  -h, --help            show this help message and exit
  -D DATABASE_STANZA, --database_stanza DATABASE_STANZA
                        database-config-stanza

```

## publish
The publish command publishes a canned set of messages
to the specied topic to the HOP instacens  instance
specifed by -H. It is meant for debugging use cases.
```
usage: housekeeping.py publish [-h] [-t TOPIC] [-H HOP_STANZA]

optional arguments:
  -h, --help            show this help message and exit
  -t TOPIC, --topic TOPIC
                        consume only this topic for consumption
  -H HOP_STANZA, --hop_stanza HOP_STANZA
                        hopskotch config stanza
```

