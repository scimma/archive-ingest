# Housekeeping

## Goal and use cases

Goal: Provide an accurate, binary-level archive that introduces no
addiional constraints on message or header to constraints of HopSkootch.

The archive supports use cases such as  1)restore a kafka stream,
2) Provide the basis for follow-on value-added services, best served
by an archive rather than a  stream.

## Implementation overview.

The app archives all messages authorized by its credential, excepting
those listed on veto-list that is read at start up. The veto list is
meant to exclude utilities related to the operation of Hopskotch, for
example the heartbeat.

What is stored from these  messages?  1) Kafka messages 2)
select Kafaka metdata: topic, millisecond integer timestamp, and
headers. 3) "annotations" indicating events within the archiver are 
also stored.

Messages pubished using hop client 1.8.0 or later provide a UUID
in the message's _id header.  This UUD is exposed to the  end user. If an
_id header is absent, then archive_ingest app supplies a UUID.

How is the data stored?  All of the above are packed into a [BSON
formatted object] (https://bsonspec.org).  Unlike JSON, BSON allows
for the storage of binary blobs.  The packaging is such that a single
BSON object kafka message and its select metadata and
header and  "Annotations" produced by the houseeping app are stored in
the bson for good measure. Housekeeping stores the BSON object an  AWS
a primary S3 bucket. The primary AWS bucket is backed up by automatic
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

Production and development deployments are via containers.  Containers
are made via the Makefile in this directory.  There are AWS-Specifc
mechanismns for deployed containers gaining AWS credentials. Desktop
development is supported by running the housekeeping.py application
natively, using AWS credentails present supported by the AWS
development kit, boto3, and the like. 

Developers can test againist live systems in the
development area, mixedin with mocks of hop.  The mock of hop produces
data that is not deterministically replicable, or difficult to replicate.
These include

- data from early versions of hop_client
- data that is recieved more than once.

archive_ingest module also contains utilites to monitoring/ adminstering the
application. (See the readme in the src diretory for further
informaation).

## (interim) Container deployment example

The goal of this git repository is to make and push containers containing
the archive_ingest application into into both the SCiMMA
AWS ecs repository and to push containers into the
docker.io/scimma/archive_ingest repository for local contiainer testing.

### Version conventions

Version 0.0.0 is hardwired (in terrafrom) for a deployed
pod in the development Kubernetes.

The ""most resecent" tag in the sense of semantic versioning
begnning with "1."  is hardwired in terraform for production
versions.


### Production release checklist

- Test
- Checkin 
- Tag     example $ git tag -a v1.0.0 -m "my version 1.4"
- Deploy
- Test.

### ad-hoc continer pushes during development

make TAG=0.0.0 # devleopment version will pich this up.
kubectl <kill the pod so it take the new version>
