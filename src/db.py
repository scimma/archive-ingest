#!/usr/bin/python3

'''
pre-Alpha, Created on Jul 8, 2021

@author: Mahmoud Parvizi (parvizim@msu.edu)
'''

from random import random
import toml
import logging


from sqlalchemy import Column, Integer, String, create_engine, exc
from sqlalchemy.orm import Session, declarative_base

from hop.io import Stream, StartPosition, list_topics




Base = declarative_base()  
class MessagesTable(Base):
    
    """Defines the database table where message, metadata are inserted.

    """
    
    __tablename__ = 'Streamed_Messages'
    
    uid = Column(Integer, primary_key=True) #autoincremented unique id
    topic = Column(String, nullable=False)
    timestamp = Column(Integer)
    payload = Column(String)
    
    # def __repr__(self):
    #     return (
    #         f"Streamed_Messages(uid={self.uid!r}, "
    #         f"topic={self.topic!r}, "
    #         f"timestamp={self.timestamp!r}, "
    #         f"payload={self.payload!r})"
    #     )
        


class Settings:
    
    """Sets the configuration file path and loads the preconfigured settings.
    
    """
    
    def __init__(self, file=None):


        if file is None:
            self.file = "db.toml"
        else:
            self.file = file
        keys = ("log", "sql", "hop")
        toml_data = []
        for key in keys:
            with open(self.file, "r") as f:
                try:
                    toml_data.append(toml.loads(f.read())[key])   
                except KeyError:
                    raise RuntimeError(f"configuration file is missing"
                                       f" '[{key}]' table"
                    )
        self.log_data = toml_data[0]
        self.sql_data = toml_data[1]
        self.hop_data = toml_data[2]
            
    def make_log(self):
        
        """Sets up and initiates the program logger using python logging module.
        
        """
        
        try:
            filename=self.log_data["filename"] 
            level=self.log_data["level"] 
        except KeyError:
            raise RuntimeError(
                f"configuration file keys in '[log]' "
                f"are not configured correctly"
            )
        logging.basicConfig(filename=filename, level=level)
        logging.info("---Logging HOPSKOTCH payload and metadata to database:")


    def make_sql_url(self):
        try:
            return (
                f"{self.sql_data['rdms']}+"
                f"{self.sql_data['dbapi']}:///"
                f"{self.sql_data['file']}"
            )
        except KeyError:
            raise RuntimeError(
                f"configuration file keys in '[sql]' "
                f"are not configured correctly"
            )


    def make_hop_url(self):
        
        """Constructs a supported HOPSKOTCH broker url from the 'config' file and
        available broker specific topics.
        
             Returns : 'str' a supported HOPSKOTCH url.
             
         """
        
        try:
            url = (
                f"kafka://"
                f"{self.hop_data['username']}@"
                f"{self.hop_data['hostname']}:"
                f"{self.hop_data['port']}/"
            )
            admin_topics = self.hop_data["admin_topics"]
        except KeyError:
            raise RuntimeError(
                f"configuration file keys in '[hop]' "
                f"are not configured correctly"
            )
            
        # Read the available topics from the given broker    
        topic_dict = list_topics(url=url)
        
        # Concatinate the avilable topics with the broker address
        topics = ','.join([t for t in topic_dict.keys() if t not in admin_topics])
        return (f"{url}{topics}")


    def create_session(self):
    
        """Creates an sqlalchemy 'lazy connection' to the database and 
        implicitly begins a session.
        
        Agrs:
            url: The sqlalchemy connection url of the database to be opened.
        Returns:
            An sqlalchemy.orm session as a :class:'Session' instance.
            
        """
        
    
        # Create a 'lazy connection' to the database via sqlachemy.create_engine()
        try:
            echo = self.sql_data["echo"]
            pool_pre_ping = self.sql_data["pool_pre_ping"]
            future = self.sql_data["future"]
        except KeyError:
            raise RuntimeError(
                f"configuration file keys in '[sql]' "
                f"are not configured correctly"
            )
        engine = create_engine(
            url=self.make_sql_url(), 
            echo=echo,
            pool_pre_ping=pool_pre_ping,
            future=future 
        )
        
        # Create the table(s) defined via sqlalchemy.orm.declarative_base()
        Base.metadata.create_all(engine)
        
        # Return the database session begun implicitly
        return Session(engine)


    def create_current(self):
        
        """Creates an open connection to the Hopskotch event stream with 
        established defaults.
        
        Args:
            url: The HOPSKOTCH url to the stream to be opened.
            
        Returns:
            The hop.io connection to the client as a :class:'Consumer'
            instance.
        
        """
        
        try:
            username = self.hop_data["username"]
            groupname = self.hop_data["groupname"]
            until_eos = self.hop_data["until_eos"]
        except KeyError:
            raise RuntimeError(
                f"configuration file keys in '[hop]' "
                f"are not configured correctly"
            )
            
        # Instance :class:"hop.io.Stream" with auth configured via 'auth=True'
        start_at = StartPosition.EARLIEST
        stream = Stream(auth=True, start_at=start_at, until_eos=until_eos)
        
        # Return the connection to the client as :class:"hop.io.Consumer" instance
        # :meth:"random" included for pre-Aplha development only
        # Remove :meth:"random" for implemented versions
        group_id = f"{username}-{groupname}{random()}" 
        return stream.open(url=self.make_hop_url(), group_id=group_id)

    
    
    
def insert_messages(session, current):
        
    """Consumes message, metadata from HOPSKOTCH and commits 
    metadata.topic, metadata.timestamp, and message to the database.
    
    Args:
        session: The sqlalchemy.orm session as a :class:'Session' instance.
        current: The opened hop.io stream as a :class:'Consumer' instance.

    """

    for message, metadata in current.read(metadata=True, autocommit=False):
        
        # Create 'mapped' values to be inserted into the database
        values = MessagesTable(
            uid = None, #autoincremented integer as primary key
            topic = metadata.topic,
            timestamp = metadata.timestamp,
            payload = str(message) 
        )  
        
        # Attempt to insert values into the database
        try:
            session.add(values)
            session.commit() 
        except exc.OperationalError as database_error:
            session.rollback()  
            logging.critical(f"database_error = {database_error}")  
            raise RuntimeError(database_error)
            
        # Mark message as processed on commit()
        current.mark_done(metadata=metadata)   
  
             
def main(test, n):
    
    """Intiates the consumption of HOPSKOTCH messages, metadata and 
    inserts them into the database.
    
    Agrs:
        test: boolean flag for escaping the reconnect loop during unit testing
        n: number of iterations of the reconnect loop during unit testing 
    
    """ 
    
    settings = Settings()
    settings.make_log()
    
    if test:
        i = 0
        
    # Continually attempt to (re)connect to the HOPSKOTCH stream with the  
    # specified consumer group
    while True:
        if test:
            i += 1
            if i > n:
                
                # Break out of the reconnect loop during unit testing
                break 
            
        # Implicitly begin the sqlalchemy database connection   
        try:   
            session = settings.create_session()
        except Exception as database_error:
            logging.critical(f"database_error = {database_error}")  
            raise Exception(database_error)
            
        # Establish a 'current' i.e., an open HOPSKOTCH stream with the  
        # specified consumer group
        try:
            current = settings.create_current()
        except Exception as hopskotch_error:
            logging.warning(f"hopskotch_error = {hopskotch_error}")   
            continue
        
        # Begin inserting HOPSKOTCH streamed payload, metadata. 
        with session:
            with current:
                insert_messages(session=session, current=current)       
        
    if test:
            return session # Return session to :module:'test.db' for validation
        
          
def init():
    if __name__ == "__main__":
        main(test=False, n=None)
        
        
init()        
