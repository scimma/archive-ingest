"""
Provide classes and API to sources of housekeeping data.

There are two types homeomorphic classes

One class accesses hopskotch. This class can be configured
via housekeeping.toml. It can access the production or
development versions of hop via different configurations. Hop
credentials  are stored in AWS secrets.

The other class  is "mock" source useful for
development and test.

The SourceFactory class supports choosing which class is
used at run-time.

All classes use a namespace object (args), such
as provided by argparse, as part of their interface.

"""
##################################
#   Sources 
##################################

import boto3
from random import random
import toml
import logging
import time
import random
import uuid
import json
import os
import certifi
import hop
from hop.io import Stream, StartPosition, list_topics

class SourceFactory:
    """
    Factory class to create Mock, or HOP data sources. 
    """

    def __init__(self, args):
        toml_data = toml.load(args.toml_file)
        config    = toml_data.get(args.hop_stanza, None)

        type = config["type"]
        #instantiate, then return source object of correct type.
        if type == "mock" : self.source =  Mock_source(args, config) ; return
        if type == "hop"  : self.source =  Hop_source(args, config)  ; return
        logging.fatal(f"source {type} not supported")
        exit (1)
        
    def get_source(self):
        "return the srouce sppecified in the toml file"
        return self.source

class Base_source:
    "base class for common methods"
    
    def __init__(self, args, config):
        pass
    

    def add_missing_headers(self, headers):
        "add the _id header (uuid) if its not present"
        if not headers :
            headers = [("_id", uuid.uuid4().bytes)]
        else:
            labels, values = zip(headers)
            if "_id" not in labels:
                headers.append(("_id", uuid.uuid4().bytes))
        return headers
    
class Mock_source(Base_source):
    """
    a mock source  that will support capacity testing.
    """

    def __init__(self, args, config):
        logging.info(f"Mock Source configured")
        import os
        #Move these to config file once we are happy
        small_text     =  b"500 Text " * 50      #500 bytes
        medium_text    =  b"5K Text   " * 500    #5000 bytes
        large_text     =  b"50K text  " * 5000     #50000 bytes
        xlarge_text    =  b"50K text  " * 50000   #500000 bytes
        max_text       =  b"50K text  " * 3000000  #3 meg
        
        small_binary   = os.urandom(500)       #500 bytes
        medium_binary  = os.urandom(5000)      #5000 bytes
        large_binary   = os.urandom(50000)      #50000 byte
        xlarge_binary  = os.urandom(500000)    #50000 byte
        max_binary     = os.urandom(3000000)   #3 meg
        self.messages  = [small_text, medium_text, large_text, xlarge_text, max_text, small_binary, medium_binary, large_binary, xlarge_binary, max_binary]
        self.n_sent    = 0
        self.n_events  = 200
        self.total_message_bytes = 0
        self.t0 =  time.time()
        super().__init__(args, config)
        
    def connect(self):
        pass
    
    def is_active(self):
        if self.n_events > 0 :
            self.n_events = self.n_events-1
            return True
        else:
            return False

    def get_next(self):
        "get next mock message"
        
        #put spread in times for S3 testing.
        early_time = int(time.time()) - (60*60*24*5)  
        late_time  = int(time.time()) + (60*60*24*5)
        #vary names near the root for S3 testing
        anumber  = random.randrange(0,20)
        for message in self.messages:
            #do fewer 
            import math
            message_size = len(message)
            #hack to scal down # interations with message size.
            n_iter = int(2000/math.log(message_size,1.8))
            total_b = 0
            t0 = time.time()
            for i in range(n_iter):
                anumber  = random.randrange(0,20)
                topic     = f"mockgroup{anumber}.mocktopic"
                timestamp = random.randrange(early_time,late_time)*1000
                headers = self.add_missing_headers([])

                metadata = {"timestamp" :timestamp,
                        "headers" : headers,
                        "topic" : topic
                        }

                payload = message
                total_b += len(payload)
                yield (payload, metadata)
            duration = int(time.time() - t0) 
            logging.info(f"msize, niter duration, totalb :{message_size}, {n_iter}, {duration}, {total_b}" )

    def record(self):
        if self.n_sent % 100 == 0 :
            delta = time.time() - self.t0
            logging.info(f"{self.n_sent} in {delta} total:{self.total_message_bytes:,}")
            self.t0 = time.time()

        self.n_sent += 1
        

class Hop_source(Base_source):
    " A class to source data from Hop"
    def __init__(self, args, config):
        self.args    = args
        toml_data    =   toml.load(args.toml_file)
        config       =   toml_data[args.hop_stanza]
        self.vetoed_topics = config["vetoed_topics"]
        #self.username      = config["username"]
        self.groupname     = config["groupname"]
        self.until_eos     = config["until_eos"]
        self.secret_name   = config["aws-secret-name"]
        self.region_name   = config["aws-secret-region"]
        self.authorize()
        self.base_url = (
                f"kafka://"   \
                f"{self.username}@" \
                f"{config['hostname']}:" \
                f"{config['port']}/"
            )

        self.refresh_url_every =  1000  # make this a config
        self.n_recieved = 0
        self.refresh_url()          #set ural with topics.
        super().__init__(args, config)
        
    def refresh_url(self):
        "initalize/refresh the list of topics to record PRN"
        #return if not not needed.
        if self.n_recieved  % self.refresh_url_every != 0: return
        if self.args.topic:
            #this implementation suposrt test and debug.
            topics = args.topic
        else: 
            # Read the available topics from the given broker
            topic_dict = list_topics(url=self.base_url, auth=self.auth)
        
            # Concatinate the avilable topics with the broker address
            # omitvetoed  topics
            topics = ','.join([t for t in topic_dict.keys() if t not in self.vetoed_topics])
        self.url = (f"{self.base_url}{topics}")
        logging.info(f"Hop Url (re)configured: {self.url} excluding {self.vetoed_topics}")
    
  
    def connect(self):
        # Instance :class:"hop.io.Stream" with auth configured via 'auth=True'
        start_at = StartPosition.EARLIEST
        stream = Stream(auth=self.auth, start_at=start_at, until_eos=self.until_eos)
        
        # Return the connection to the client as :class:"hop.io.Consumer" instance
        # THe commented uot group ID (below)) migh tbe useful in some development
        # environments it allows for re-consumption of all the existing events in all
        # the topics. 
        #group_id = f"{self.username}-{self.groupname}{random.randint(0,10000)}" 
        group_id = f"{self.username}-{self.groupname}" 
        self.client = stream.open(url=self.url, group_id=group_id)

    def authorize(self):
        "authorize using AWS secrets"
        from hop.auth import Auth
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )
        resp = client.get_secret_value(
            SecretId=self.secret_name
        )['SecretString']
        resp = json.loads(resp)
        self.username = resp["username"]
        logging.info (f"hopskotch username is: {self.username}")
        path = self.make_hop_auth_file(resp["username"], resp["password"])
        self.auth  = hop.auth.Auth(resp["username"], resp["password"])
        return
    
    def make_hop_auth_file(self, username, password):
        "make /tmp/auth.toml if it does not exist"
        auth_path = '/tmp/auth.toml'
        logging.info(f"making hop auth file {auth_path}")
        # must be in a container -- go make a file.
        with open(auth_path, "w") as out:
            out.write ( '[[auth]]\n')
            out.write (f'username = "{username}"\n')
            out.write (f'password = "{password}"\n')
            out.write ( 'protocol = "SASL_SSL"\n')
            out.write ( 'mechanism = "SCRAM-SHA-512"\n')
            pem_file =  os.path.join(os.path.dirname(certifi.__file__),"cacert.pem")
            out.write (f'ssl_ca_location = "{pem_file}"\n')
        os.chmod(auth_path, 0o600)
        return auth_path
    
        
    def is_active(self):
        return True

    def get_next(self):
        for result in self.client.read(metadata=True, autocommit=False):
            # What happens on error? GEt nothing back? None?
            # -- seems to stall in self.client.read
            # -- lack a full udnerstanding fo thsi case. 
 
            message = result[0].serialize()
            #message = result[0]
            if result[1].headers is None :
                headers = []
            else:
                headers = [h for h in result[1].headers]
            #correct this code when we see the implemenatipn of headers
            #
            headers = self.add_missing_headers([])

            #copy out the metadata of interest to what we save
            metadata = {"timestamp" : result[1].timestamp,
                        "headers" : headers,
                        "topic" : result[1].topic
                        }
            self.n_recieved  += 1
            self.refresh_url()
            #logging.info(f"topic, msg size, meta size: {metadata.topic} {len(message)}")
            yield (message, metadata)

    def publish(self, topic):
        """publish a few messages as a test fixture

           this is (obvously) underdeveloped
        """
        import subprocess
        for n in range(2):
            json = '{{"message" : "This is Blob %s"}}'.format(n)
            cmd = f"echo '{json}' | hop publish -f BLOB -t {self.base_url}{topic}"
            cmd = f"hop publish -f JSON -t {self.base_url}{topic} dog.json"
            logging.info(f"{cmd}")
            subprocess.run(cmd, shell=True)
        pass
    
    pass
