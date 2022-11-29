Housekeeping

Goal: Provide an accurate, binary-level archive that introduces no
addiional constraints on message or header to constraints of HopSkootch.

The archive supports use cases such as  1)restore a karfka stream,
2) Provide the basis for follow  on value-added services, best served
by an archive rather than a  stream.

Implementation overview.

Messages are acquired by the housekeeping app. The app listens to
public topics, subject to a veto-list.  The veto list is meant to
exclude utilites related to the operation of Hopskotch, for example
the heartbeat.

What is stored from these public messages?  1) Kafka messages 2)
select Kafaka metdata: topic, millisecond integer timestamp, and
headers are stored.

Stored headers always include an _id header.  If an _id is absent,
then housekeeping supplies a an _id header continging a UUID. If an
_id header is supplied it is assumed to be a uuid supplied by
HopSkotch software.

How is the data stored?  All of the above are packed into a BSON
formatted object (https://bsonspec.org).  Unlike JSON, BSON allows for
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



