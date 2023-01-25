# Housekeeping

## Goal and use cases

Goal: Provide an accurate, binary-level archive that introduces no
addiional constraints on message or header to constraints of HopSkootch.

The archive supports use cases such as  1)restore a karka stream,
2) Provide the basis for follow-on value-added services, best served
by an archive rather than a  stream.

##Implementation overview.

Messages are acquired by the housekeeping app. The app archives 
all  authorized by its credential excepting those to a veto-list
that is read at stre up. The veto list is meant to
exclude utilites related to the operation of Hopskotch, for example
the heartbeat.

What is stored from these public messages?  1) Kafka messages 2)
select Kafaka metdata: topic, millisecond integer timestamp, and
headers are stored.

Massese pubished using hop client 1.8.0 or later proved a UUIS
in the _id header.  Thsi UUD is expose to the  end user. If an
_id heder is absent, then housekeeping app supplies a UUID.

How is the data stored?  All of the above are packed into a [BSON
formatted object] (https://bsonspec.org).  Unlike JSON, BSON allows for
the storage of binary blobs.  The packaging is such that a single BSON
object byte-stream represents a kafka message and its select
metadata. "Annotations" produced by the housekepeing app are
stred in the bson for good measure. Wousekeeping stores the BSON
object in AWS S3. The primary AWS bucket iw production are backed up
by autoamtic AWS replication, 

Additionally, select data are stored in an AWS postgres database.
Database information includes the UUID, millisecond timestamp, topic,
internally generated serial number, bucket and key to the BSON object
used in S3. The production postgres database is backed up and
snap-shotted.

Headers and message payloads are stored only in the BSON object.
Consequently, queries based on message or header contents
are for follow on projects, not this archive implementation.

## Usage/development  Notes

Producition and development deployments are via containers.  Containers
are made via the Makefile in this directory.  There are AWS-Specifc
mechanismns for deployed containers gaining AWS credentials. Desktop
development is supported by running the housekeeping.py applicaition
natively, using AWS credentails present supported by the AWS
devleopment kit, boto3, and useng the development 

Developers can test againist live systems in the
development area, mixedin with mocks of hop.  The mock of hop produces
data that is not deterministically replicable, or difficult to replicate.
These include

- data from early versions of hop_client
- dat that is recieved more than once.

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
``

