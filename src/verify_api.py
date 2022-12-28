import bson
import logging

def get_corner_cases():

    """
    The archive needs to be robust against a level of problems
    in message headers or payloads. This routine supplies corner
    case message headers and payloads for testing the archive.
     
    Normal Cases are decribed in  Hopskotch documentation as:
    
     Each header has a string key, and a binary or unicode value. A
     collection of headers may be provided either as a dictionary or as a
     list of (key, value) tuples. Duplicate header keys are permitted; the
     list representation is necessary to utilize this allowance.

     It is important to note that Hopskotch reserves all header names
     starting with an underscore (_) for internal use; users should not set
     their own headers with such names.

    The  archives should be robust in  face of all kinds of use that
    violates  our specification.
    Corner cases in violation of our specifcation, but should not
    crash the logger, and be handled in some defined way.
    
    """
    import hop
    import pdb
    import os
    import uuid
    import random

    random.seed(0)
    def repeatable_binary_bytes(n):
        ans = bytes(random.getrandbits(8) for _ in range(n))
        return ans
    
    def u(): return uuid.UUID(bytes=repeatable_binary_bytes(16)).bytes
    
    BIG_PAYLOAD_BYTES = int(3000000/4)
    big_byte_string = repeatable_binary_bytes(BIG_PAYLOAD_BYTES)

    d = {}
    ### message cases
    usecase = b"empty message"
    d[usecase] = [
        hop.models.Blob(b""),
        [("test", usecase)]
        ]

    usecase = b"empty list"
    d[usecase] = [
        hop.models.JSONBlob("[]"),
        [("test", usecase)]
    ]
    
    usecase = b"empty dict"
    d[usecase] = [
        hop.models.JSONBlob("{}"),
        [("test", usecase) ]
    ]

    usecase = b"Big Blob"
    d[usecase] = [
        hop.models.Blob(big_byte_string),
        [("test", usecase), ("payload_size", b"alot")]
    ]
    
    ### header cases
    usecase = b"many headers"
    d[usecase] = [
        hop.models.Blob(b"muliple headers"),
        [("test", usecase), ("dog", b"fido"), ("cat", b"fifi")]
    ]
    
    usecase = b"repeated keys"
    d[usecase] = [
        hop.models.Blob(b"repeated key"),
        [("test", usecase), ("key", b"v1"),("key", b"v3"),("key",b"v3")]
        ]

    usecase = b"None Valued Key"
    d[usecase] = [
        hop.models.Blob(b"None key value"),
        [("test", usecase), ("none", None)]
    ]
    
    usecase = b"Binary Valued Key"
    d[usecase] = [
        hop.models.Blob(b"binary key val"),
        [("test", usecase),("empty", b"")]                               ]

    
    ### tampering with reserved keys.
    usecase = b"Extra invalid _ids"
    d[usecase] = [
        hop.models.Blob(b"invalid _id"),
        [("test", usecase), ("_id", None), ("_id", b"text"), ("_id", b"")]
    ]
    
    usecase = b"Extra valid _ids"
    d[usecase] = [
        hop.models.Blob(b"valid _ids"),
        [("test", usecase),("_id", u()), ("_id", u()), ("_id", u())]    ]
    
    # end of test stream message
    end = [
        hop.models.Blob(b"end"),
        [("test", b"end")]
    ]
    
    return d, end

def get_from_header(header, key):
    """
    Return an array of values corresponding to key
    
    Recall that kafka allows the key to occur mulitple times.
    """
    values = [h[1] for h in header if h[0] == key]
    return values


def assert_ok(args, recieved_payload, recieved_metadata, text_uuid, storeinfo, db, store):
    """
    uses database an store to verify archive entries as they
    are created.

    Verification is from thr output of the housekeeping
    source object  to readback and comparison to the
    records in the archive.

    ie. does not verify errors in the hop ->
    output-of-housekeeping-"source call"
    but is lightweight to run.
    """
    if not args.verify : return

    #check DB match
    q = f"""select
               topic, timestamp, uuid, size, key
            from messages where uuid = '{text_uuid}'""";
    result = db.query(q)
    assert len(result) == 1   #uuid is unique in the databse.
    topic, timestamp, uuid, size, key   = result[0]
    assert uuid == text_uuid 
    assert timestamp == recieved_metadata["timestamp"] 
    assert topic == recieved_metadata["topic"] 

    recorded_object = store.get_object(key)
    assert len(recorded_object) == size # the size of the bundle     
    recorded = bson.loads(recorded_object)
    recorded_metadata = recorded["metadata"]
    
    # kafka provided metadata items agree.
    assert topic     == recorded_metadata["topic"]
    assert timestamp == recorded_metadata["timestamp"]

    
    # number of header items and header contents agree
    recieved_headers =  recieved_metadata["headers"]
    recorded_headers =  recorded_metadata["headers"]
    assert type(recorded_headers) == type(recieved_headers)
    assert len(recorded_headers) == len(recieved_headers)
    for key, value in recieved_headers:
        assert value in get_from_header(recorded_headers, key)

    
    # payload is the same
    recorded_payload = recorded["message"]
    assert type(recieved_payload) == type(recorded_payload)
    assert recieved_payload == recorded_payload



COMPARE_DICT = {}
def compare_corner_cases(as_recieved_payload, as_recieved_metadata):
    """
    Compare what housekeeping presents as recieved to original data
    sent.

    The supported sender is a special publish from houseutils,
    which sends data returned by get_corner_case(), defined abouve.
    """
    
    #first call -- get info on  the corner cases test that would have been sent.
    global COMPARE_DICT
    if not COMPARE_DICT: 
        COMPARE_DICT, end = get_corner_cases()

    as_recieved_header = as_recieved_metadata["headers"]
    # extract use case from the "test" header.
    # get the message and header which was sent.
    #import pdb; pdb.set_trace()
    this_use_case = get_from_header(as_recieved_header, "test")[0]
    logging.info(f"uses case {this_use_case}")
    as_sent_payload  = COMPARE_DICT[this_use_case][0].serialize()
    as_sent_header = COMPARE_DICT[this_use_case][1]

    # assert payload is the same
    assert as_sent_payload  == as_recieved_payload
    
    # Assert that every user supplied header item is present in the
    # recieved header. Note that HOP adds keys, it's not
    # the job of this module to check those are present/accurate,
    # This module's job is to assert what the user sent was recieved
    # accurately and presented for archiving
    
    for sent_key, sent_value in as_sent_header :
        assert sent_value in get_from_header(as_recieved_header, sent_key) 

