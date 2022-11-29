Housekeeping

Goal: Provide an accurate, binary-level archive that makes no addiional
constraints on measge or header  content other than the constraints
of HopSkootch.

The archinve  satisfies  use cases such as  1)restore a karfka stream,
2) Provide the basis for follow  on value-added services, best served
by an archive rather than a  stream.

Implementation overview.

Messages are aquited by the housekeeping app. The app listens to
public topics, subject to a veto-list.  The veto list is meant to
exclude utilites realted to the opeartion of Hopsskotch, for example
the heartbeat.

What is stored from these public messages?  1) Kafaka messages 2)
select Kafaka metdata: topic, millisecond integer timestamp, and
headers are stored.  stored Headers always include an  _id header.
If an _id is absent, then housekeeping supplied a an _id header
continging a UUID. If an _id header is supplied it is assumed to
be a uuid supplied by HopSkootch software.

How is the data  stored?  All of the above are packed into a BSON
formatted object (https://bsonspec.org).  Unlike JOSN, BSON  allows
for the storage of binary blobs.  The packaging is such that a single
BSON object byte-stream represents a kafka message and it's
select metadata. Housekeeping stores the bson objet in AWS S3. Any AWS
buckets in production are  backed up.

Additionally, select data are stored in an AWS postgres database.
Database files include the UUID, millisecond timestamp, topic, bucket
and key to the bson object used in S3. Headers and message payloads are not
stored in the database. Consequently queries based on message content, and
headers are for follow on projects, not this archive.  The producion
postgres database is backed up.


