# Hopskotch archiver

This repository contains the tool which ingests data from Apache Kafka and places it in the Hopskotch long-term data archive. 

At this time, the archiver simply archives all messages on all topics for which it has read permission, unless a topic is included on its veto list. 
This means that archiving should be controlled via the permissions granted to the Kafka credential used by the archiver. 
Since public topics in Hopskotch are alway readable to all authenticated users, topics with high message rates but little long-term message value, like the system heartbeat, should be placed on the veto list to prevent the archiver consuming them.

## Environment and Configuration

This tool supports configuration via command-line options, environment variables, a TOML configration file, or any combination of these. Run `archive_ingest.py --help` for a full listing of supported options. 

Major comfiguration settings include:

- `--hop-hostname`/`HOP_HOSTNAME`: The name of the Kafka broker host
- `HOP_PASSWORD`: The password to use for authenticating with Hopskotch/Kafka if credentials are not being read from a file or an AWS secret; this option can only be set as an environment variable
- `--hop-aws-secret-name`/`HOP_AWS_SECRET_NAME`: The name of an AWS secret from which to read a  Hopskotch/Kafka credential
- `--hop-vetoed-topics`/`HOP_VETOED_TOPICS`: Topics which should not ever be archived (e.g. `sys.heartbeat`)
- `--db-type`/`DB_TYPE`: `sql` to use a PostgreSQL database directly, `aws` to use an AWS hosted PostgreSQL instance. 
- `DB_PASSWORD`: The database password, if connecting to a PostgreSQl database without special AWS handling; this option can only be set as an environment variable
- `--store-endpoint-url`/`STORE_ENDPOINT_URL`: The URL at which to connect to the object store. If not set, AWS S3 is assumed. 
- `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`: If not using AWS S3, these variables specify the credential pair used to authenticate with the object store. These can only be specified as environment variables. 
- `--store-primary-bucket`/`STORE_PRIMARY_BUCKET`: The primary object store bucket in which archived data should be placed
- `--store-backup-bucket`/`STORE_BACKUP_BUCKET`: The backup object store bucket
