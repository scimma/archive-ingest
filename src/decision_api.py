"""

"""
import logging

def is_deemed_duplicate(annotations, metadata, db, store):
    "Decide if this message is a duplicate"

    # storage decision_data
    if annotations['con_is_client_uuid']:
        # Use the fact that client side UUID are unique
        duplicate = uuid_in_db(db, annotations['con_text_uuid'])
    else:
        # server side UUID.
        topic = metadata["topic"]
        timestamp = metadata["timestamp"]
        duplicate = exists_in_db(db, topic, timestamp)
    annotations["duplicate"] = duplicate
    logging.info(f"annotations: {annotations}")
    return duplicate


def uuid_in_db(db, uuid):
    """
    Determine if this UUID is in the database
    then
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

def exists_in_db(db, topic, timestamp):
    """
    server_side UUID, of it will change on
    redundant ingeests

    have we seen this before?  n.b we don;t
    trust CRC or size becase we may have changes to
    own metadata that is rolled up in the
    saved object.

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
    """
    result = db.query(sql)
    if result[0][0] == 0 : return False
    return True
