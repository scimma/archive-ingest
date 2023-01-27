# Hopskotch topic archiver

## Goal and use cases

Goal: Provide an accurate, binary-level archive that introduces no
addiional constraints on message or header to constraints of HopSkootch.

The archive supports use cases such as  1)restore a karka stream,
2) Provide the basis for follow-on value-added services, best served
by an archive rather than a  stream.

## Implementation overview.

Messages are acquired by the housekeeping app. The app listens to
public topics, subject to a veto-list.  The veto list is meant to
exclude utilites related to the operation of Hopskotch, for example
the heartbeat.

What is stored from these public messages?  1) Kafka messages 2)
select Kafaka metdata: topic, millisecond integer timestamp, and
headers are stored.

Stored headers always include an _id header.  If an _id is absent,
then housekeeping supplies a an _id header containing a UUID. If an
_id header is supplied it is assumed to be a uuid supplied by
HopSkotch software. These uuids are in binary.

How is the data stored?  All of the above are packed into a [BSON
formatted object] (https://bsonspec.org).  Unlike JSON, BSON allows for
the storage of binary blobs.  The packaging is such that a single BSON
object byte-stream represents a kafka message and its select
metadata. Housekeeping stores the BSON object in AWS S3. AWS
buckets in production are backed up.

Additionally, select data are stored in an AWS postgres database.
Database information includes the UUID, millisecond timestamp, topic,
internally generated serial number, bucket and key to the BSON object
used in S3. The production postgres database is backed up.

Headers and message payloads are stored only in the BSON object.
Consequently, queries based on message or header contents
are for follow on projects, not this archive implementation.

## Usage/development  Notes

Produciotn and development deployments are via containers.  Containers
are made via the Makefile in this directory.  There are AWS-Specifc
mechanismns fordeployed containers gaining AWS credentials. Destop
development is supported by running the housekeeping.py applicaiton
natively, using AWS credentails present supported by the AWS
devleopment kit, boto3.

Developers can test againist live systems in the
development area, mixedin with mocks of hop, the
databse, and the onject store.

Housekeeping.py also contains utilites to monitoring/ adminstering the
application. (See the readme in the src diretory for further
informaation).

## (interim) Container deployment example

The goal of this git repository is to make and push containers containing
the housekeeping application into into both the SCiMMA
AWS ecs repository and to push containers into the
docker.io/scimma/housekeeping repository.

Ultimatelu This is done in the context of a set of github actions.
These actions are not yet implemented.  Right now,
the Makefile can only push containers to aws, using the
following commands.

Makefile actions to push containers to gitub.io/scimma are commented out.

```
make container
make  push  GITHUB_REF=housekeeping-0.0.0
```


## Local development with Docker Compose

You can run this locally via Docker Compose without dependency on AWS or a remote database.

First copy `env.tpl` to `.env` and fill in your Hopskotch credentials.

Then launch the application with

```bash
docker compose up
```

Once the containers are bootstrapped, `docker exec` into the main container and run

```bash
$ docker exec -it hop-archiver bash

[root@90fd75dd29a4 src]$ ./housekeeping.py run -H hop-local -D local-db -S mock-store
```

Access the local database instance using:

```bash
$ docker exec -it hop-archiver-db bash
I have no name!@73d75c60f19a:/$ PGPASSWORD=$POSTGRESQL_PASSWORD psql -U $POSTGRESQL_USERNAME $POSTGRESQL_DATABASE
```

To start with a clean slate, run the following to destroy the database and any other persistent volumes:

```bash
docker compose down --remove-orphans --volumes
```



# Housekeeping Software

Housekeeping uses three system elements.

- Sources which  hopskotch or mocks
- Databases which are either AWS rds Postgres of mocks
- Stores which can be AWS S3 store or mocks

Realized (i.e non-mock) elements can be either from the SCiMMA
development or producion instances as appropriate.  The softare
is such that there is a configuraton to source  production hopskotch
while developing. 


## Credentials

Credentials for HOP, and the database are kept in AWS.

For the development instance HOP credntials whold be read/write,
to allow for injection of test data.   FOr production, credntials
for hop shodl be read only.



## Housekeeping.toml

Housekeeping.toml holds configuration data for the producion and
development instnances of housekeeping.py, and for houseutils.py


## Housekeeping.py

Housekeeping.py is the program that loads the housekeeping archive.
Housekeepling.py can be run during development against the live ements
in the deevepoment instance or mock elements. It's pososbel to use the
the production HOP as a data source in development.

```
usage: housekeeping.py [-h] [-t TOML_FILE] [-l LOG_STANZA] {run,list} ...

Log messages, metadata, and annotatopns to S3.
Record meta-data about stored data in relational
DB.  Omit duplicate due to more-tnan-once delivery or
cursor resets.

Run time configuration is via command line options,
including options to select configuration stanzas
from a "toml" configuration file.

Support testing via
- reading limited to a test topic
- reading from a mock topic
- in-line comparison of as-sent test data
  to as-recieved test data.
- in-line comparision of as-recieved data
  to as-stored data.

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

## Houseutils.py

Houseutils.py contains a number of sub funcitons used to test
and access the running system.

```
usage: houseutils.py [-h] [-t TOML_FILE] [-l LOG_STANZA]
                     {list,connect,status,publish,verify,inspect,uuids,db_logs,kube_logs,clean_tests,clean_duplicates}
                     ...

Utilities for housekeeping applications.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

positional arguments:
  {list,connect,status,publish,verify,inspect,uuids,db_logs,kube_logs,clean_tests,clean_duplicates}
    list                list stanzas
    connect             Launch a query session shell against AWS databases
    status              get a sense of what's happpening
    publish             publish some test data
    verify              check objects recored in DB exist
    inspect             inspect an object given a uuid
    uuids               print a list of uuids consistent w user supplied where
                        clause
    db_logs             print recent db logs
    kube_logs           print kube logs for devel or prod
    clean_tests         clean test messages from archive and hop purge the
                        databse and store. read the test topic in HOP dry
    clean_duplicates    clean duplicates from the archive

optional arguments:
  -h, --help            show this help message and exit
  -t TOML_FILE, --toml_file TOML_FILE
                        toml configuration file
  -l LOG_STANZA, --log_stanza LOG_STANZA
                        log config stanza

```

## Troubleshooting

If the reconfiguration of AWS secrets happens, then you need to re-make credentials.
thie erros indicates new credentials are needed:

```
botocore.errorfactory.ResourceNotFoundException:
An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation:
Secrets Manager can't find the specified secret value for staging label: AWSCURRENT
```

