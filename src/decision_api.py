"""
Support Poicy decsions about dupliate messages.

Djuplicate messages arise from more-than-once
delivery or a kafka cursor reset. The module is
also a place holded for future decision logic.

The modle trusts information in the Database.
The module does not consult S3, which has a
seperate disater recovery capabklity.

This moodule supports
- in-line decisions integrated into
  message acquisitions.
- decisions  intergrated into
  archive-oriented tools
"""

#################
# Batch decisions routines
#################

def get_client_uuid_duplicates(args, db):
    """
    list uuids that are duplicates of an original UUID
    
    This routine  detects messages having uuids
    generated on the hop _client_.
    Some uuids may have multiple duplicates.
    only one duplicate uuid is returned.
    
    """
    sql_client_side = f"""
       SELECT
         max(id), uuid, count(*)
        FROM
         messages
        GROUP By
         uuid
        HAVING
         count(*) > 1
        LIMIT {args["limit"]}
    """
    results = db.query(sql_client_side)
    return results

def get_server_uuid_duplicates(args, db):
    """
    list uuids that are duplicates of an original UUID
    
    This routine detects messages having uuids
    generated on the archive_ingest.py   _server_.
    """
    sql_server_side = f"""
       SELECT
         max(id), topic, size, timestamp, count(*)
        FROM
         messages
        GROUP By
         topic, size, timestamp
        HAVING
         count(*) > 1
        LIMIT {args["limit"]}
    """
    results = db.query(sql_server_side)
    return results


def is_content_identical (ids, db, store):
    "ensure that  messages have the same content"
    import zlib
    import bson
    list_text = "(" + ids.join(",")  + ")"
    sql = f"select bucket, key from messages where id in {list_text}"
    result = db.query(sql)
    contents = []
    for bucket, key in result:
        content = bson.loads(store.get(key))
        contents.append(content)
    crc_set = {zlib.crc32(c["message"]["content"]) for c in contents}
    n_items = len(crc_set)
    if n_items == 1: return True
    return False


###################
# routines to decide if a message is idenitcal to one ...
# already in the archive.
###################
 

def is_deemed_duplicate(annotations, metadata, db, store):
    "Decide if this message is a duplicate"
    # storage decision_data
    if annotations['con_is_client_uuid']:
        # Use the fact that client side UUID are unique
        duplicate = is_uuid_in_db(db, annotations['con_text_uuid'])
    else:
        # server side UUID.
        topic = metadata["topic"]
        timestamp = metadata["timestamp"]
        message_crc32 = annotations["con_message_crc32"]
        duplicate = exists_in_db(db, topic, timestamp, message_crc32)
    annotations["duplicate"] = duplicate
    return duplicate


def is_uuid_in_db(db, uuid):
    """
    Determine if this UUID is in the database
    """
    sql = f"""
       SELECT
        count(*)
       FROM
        messages
       WHERE
        uuid = '{uuid}'
    """
    result = db.query(sql)
    if result[0][0] ==   0 : return False
    return True

def exists_in_db(db, topic, timestamp, message_crc32):
    """
    server_side UUID, of it will change on
    redundant ingeests

    have we seen this before?  n.b we don;t
    trust object CRC or size becase we may
    have changes to annotatons saveed the
    object.

    """    
    sql = f"""
    SELECT
       count(*)
    FROM
       messages
    WHERE
      timestamp = {timestamp}
      AND
      topic = '{topic}'
      AND
      message_crc32 = '{message_crc32}'
    """
    result = db.query(sql)
    if result[0][0] == 0 : return False
    return True
