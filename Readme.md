# Housekeeping

## Goal and use cases

Goal: Provide an accurate, binary-level archive that introduces no
addiional constraints on message or header to constraints of HopSkootch.

The archive supports use cases such as  1)restore a karka stream,
2) Provide the basis for follow-on value-added services, best served
by an archive rather than a  stream.

## Implementation overview.

The app archives all messages authorized by its credential, excepting
those listed on veto-list that is read at start up. The veto list is
meant to exclude utilities related to the operation of Hopskotch, for
example the heartbeat.

What is stored from these  messages?  1) Kafka messages 2)
select Kafaka metdata: topic, millisecond integer timestamp, and
headers are stored.

Messages pubished using hop client 1.8.0 or later provide a UUID
in the message's _id header.  This UUD is exposed to the  end user. If an
_id header is absent, then housekeeping app supplies a UUID.

How is the data stored?  All of the above are packed into a [BSON
formatted object] (https://bsonspec.org).  Unlike JSON, BSON allows
for the storage of binary blobs.  The packaging is such that a single
BSON object kafka message and its select metadata and
header.. "Annotations" produced by the houseeping app are stored in
the bson for good measure. Housekeeping stores the BSON object an  AWS
a primary S3 bucket. The primary AWS bucket is backed up by autoamtic
AWS replication to a second bucket.  Paths in the AWS bucket are of
the form <topic>/<year>/<month>/<day>/<uuid>.bson

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
- data that is recieved more than once.

Housekeeping.py also contains utilites to monitoring/ adminstering the
application. (See the readme in the src diretory for further
informaation).

## (interim) Container deployment example

The goal of this git repository is to make and push containers containing
the housekeeping application into into both the SCiMMA
AWS ecs repository and to push containers into the
docker.io/scimma/housekeeping repository for local contiainre testing.

### Version conventions

Version 0.0.0 is hardwired (in terrafrom) for a deployed
pod in the development Kubernets.

The ""most reset" tag in the sense of semantic versioning
begnning with "1."  is hardwired in terraform for production
veriskos

Version namespace 0.8.x is used to debug or develop such
deveops/container deployment as we have (OK this is naive).

### Production release checklist

- Test
- Checkin 
- Tag     example $ git tag -a v1.0.0 -m "my version 1.4"
- Deploy

### ad-hoc continer pushes during development

Makefile actions to push containers to gitub.io/scimma are commented out.

```
make container
make  push  GITHUB_REF=housekeeping-0.0.0
``

