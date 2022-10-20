'''
Created on Sep 28, 2021

@author: Mahmoud Parvizi (parvizim@msu.edu)
'''

from dataclasses import dataclass
from os import remove
from unittest.mock import patch
from sqlalchemy import select
import toml

import pytest

from hop.io import StartPosition
import db



@dataclass
class MockAdc:
    
    """Mock the tracking of processed messages via a consumer group."""
    
    processed_messages : list
    consumer_group : str = None
    disconnect : bool = False
    reconnect : bool = False
    

class MockAuth:
    
    """Mock client based auth options.
    
    Returns client-based auth options when called.
    Parameters
    ----------
    username : `str`
        Username to authenticate with.
    password : `str`
        Password to authenticate with.
    hostname : `str`, optional
        The name of the host for which this authentication is valid.
    protocol : 'str', optional
        The communication protocol to use.
    mechanism : `str`, optional
        The method to authenticate
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.
    """

    def __init__(self, auth_data):
        self._username = auth_data["username"]
        self._password = auth_data["password"]
        self._hostname = "prefix.hostname.org"
        self._protocol = auth_data["protocol"]
        self._mechanism = auth_data["mechanism"]
        self._ssl_ca_location = auth_data["ssl_ca_location"]


    @property
    def username(self):
        """The username for this credential."""
        return self._username
    
    @property
    def password(self):
        """The password for this credential."""
        return self._password
    
    @property
    def hostname(self):
        """The hostname with which this creential is associated."""
        return self._hostname
    
    @property
    def mechanism(self):
        """The authentication mechanism to use."""
        return self._mechanism
    
    @property
    def protocol(self):
        """The communication protocol to use."""
        return self._protocol
    
    @property
    def ssl(self):
        """Whether communication should be secured with SSL."""
        return self._protocol != None
    
    @property
    def ssl_ca_location(self):
        """The location of the Certfificate Authority data used for SSL."""
        return self._ssl_ca_location
    
 
class MockTopic:
    
    """Mock message topics.
    
    Args:
        name: A mocked topic to consume.
        message: A list of mocked messages for the given topic.
    
    """
    
    def __init__(self, name, messages):
        self.name = name
        self.messages = messages
        self.offset = 0
    
    
    def next_message(self):
        
        """Iterates through mocked messages of a given mocked topic.
        
        Returns a mocked message and a :class:'MockMetadata' instance.
        
        """
        
        if self.offset >= len(self.messages):
            return None
        mock_message = self.messages[self.offset]
        mock_metadata = MockMetadata(
            topic=self.name, 
            offset=self.offset, 
            timestamp=self.offset*10, 
            payload=mock_message
        )
        self.offset += 1
        return (mock_message, mock_metadata)
    
    
class MockStream:
    
    """Defines a mocked event stream.
    
    Sets up defaults used within the mocked client so that when a
    stream connection is opened, it will use defaults specified here.
    
    Args:
        auth: A 'bool' or :class:'MockAuth' instance. 
        start_at: The message offset to start at in read mode. 
        until_eos: Whether to listen to new messages forever (False) or stop
            when EOS is received in read mode (True). 
            
    """
    
    def __init__(self, auth, start_at, until_eos):
        
        # Set a bool flag for testing 'auth' as instance :class:'MockAuth'
        if isinstance(auth, MockAuth):
            self.auth = auth
        else:
            self.auth = False
            
        self.start_at = start_at  
        self.until_eos = until_eos
    
    
    def open(self, url, group_id):
        
        """Opens a connection to a mocked event stream.
        
        Args:
            url: Sets the mock broker URL to connect to.
            group_id: The consumer group ID from which to read.
                      
        Returns:
            An open connection to the mock client, 
            as a :class:'MockConsumer' instance.
        
        """
        
        self.url = url
        self.group_id = group_id
        
        # Simulate that a HOPSKOTCH disconnect occured during 
        # a previous instance of :meth:'MockStream.open'
        if MockAdc.reconnect:
            MockAdc.reconnect = False
            raise Exception("mock disconnect error")
        
        return MockConsumer(
            url=self.url,
            group_id=self.group_id,
            auth=self.auth,
            start_at=self.start_at,
            until_eos=self.until_eos
        )
    

class MockMetadata:
    
    """Mock metadata that accompanies a consumed message."""
    
    def __init__(self, topic, offset, timestamp, payload):
        self.topic = topic
        self.offset = offset
        self.timestamp = timestamp
        self._payload = payload
    
    @property
    def payload(self):
        """The metadata payload."""
        return self._payload
        
            
class MockConsumer:
    
    """An mock event stream opened for reading one or more mock topics.
    Instances of this class should be obtained from :meth:'MockStream.open'.
    
    Args:
        url: The list of mock broker URLs.
        group_id: The consumer group to join for reading messages.
        auth: A :class:'MockAuth' object specifying client authentication
            to use.
        start_at: The position in the mock topic stream at which to start
            reading, specified as a StartPosition object.
        until_eos: If false, keep the mock stream open to wait for more 
            messages after reading the last currently available message.
 
        """
    
    def __init__(self, url, group_id, auth, start_at, until_eos):

        self.url = url
        
        # Strip the random() extension used in pre-Alpha development
        self.group_id = group_id.strip("0123456789")
        
        # Check for an 'established' consumer group
        if MockAdc.consumer_group is None:
            MockAdc.consumer_group = self.group_id
            self.group = False
        elif MockAdc.consumer_group == self.group_id:
            self.group = True
        else:
            self.group = False
            
        self.auth = auth
        self.start_at = start_at
        self.until_eos = until_eos
        self.message_data = get_message_data()
        
    
    def read(self, metadata, autocommit):
        
        """Read mocked messages from a mock stream.
        
        Args:
            metadata: Whether to receive message metadata alongside messages.
            autocommit: Whether messages are automatically marked as handled
                via `mark_done` when the next message is yielded. 
                Defaults to True.
            
        """
        
        
        self.metadata = metadata
        self.autocommit = autocommit
        
        # Create a list of keys for use in tracking messages
        # as processed for the given consumer group
        self.key_list = []
        for key in self.message_data:
            self.key_list.append(key)
            
        for key, values in self.message_data.items():
            
            # Simulate a HOPSKOTCH disconnect when 'disconnect=True'
            if MockAdc.disconnect and key == (self.key_list[-1]):
                MockAdc.disconnect = False # Terminate the diconnect
                MockAdc.reconnect = True # Initiate the recconect
                break
            
            self.mock_topic = MockTopic(
                name=key,
                messages=values
            )
            
            # If 'autocommit' is set to True, mark all messages as processed 
            if self.autocommit:
                for message in values:
                    MockAdc.processed_messages.append(message)
            while True:
                data = self.mock_topic.next_message()
                if data is None:
                    break
                
                # Test for messages previously consumed by a
                # given consumer group
                if self.group and data[0] in MockAdc.processed_messages:
                    continue
                if self.metadata:
                    yield data
                else:
                    yield data[0]
                
                    
    def mark_done(self, metadata):
        
        """Marks a message as fully-processed.
        
        Args:
            metadata: A :class:'MockMetadata' instance.
            
        """
        
        MockAdc.processed_messages.append(metadata.payload)

    def close(self):
        pass
    
    def __iter__(self):
        yield from self.read()
        
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class MockToml:

    """Defines the mocked 'auth' and 'config' toml files used to 
    log streamed HOPSKOTCH messages into the mock database.

    """

    def __init__(self, tmp_path):  
        
        # Set path using the pytest 'tmp_path' decorator
        self.test_temp_dir = tmp_path / "toml"
        self.test_temp_dir.mkdir()
        
        # Mock the 'auth' file
        self.test_auth_toml = self.test_temp_dir / "auth.toml" 
        self.test_auth_toml.write_text("""
        [auth]
        username = "username-1a2b3cd4"
        password = "password"
        protocol = "ABCD_ABC"
        mechanism = "ABCDE-ABC-123"
        ssl_ca_location = "/some/dir/"
        """)
        
        # Mock the 'config' file
        self.test_config_toml = self.test_temp_dir / "config.toml" 
        self.test_config_toml.write_text("""
        [log]
        filename = "test_db.log" 
        level = "DEBUG"

        [sql]
        file = "temp.db" 
        rdms = "sqlite"
        dbapi = "pysqlite"
        echo = true
        pool_pre_ping = true
        future = false 
        
        [hop]
        username = "username-1a2b3cd4"
        hostname = "prefix.hostname.org"
        groupname = "hopstream"
        port = 1234
        until_eos = false 
        admin_topics = ["admin.one", "admin.two"] 
        list_topics = [
            "topic.one", 
            "topic.two", 
            "topic.three", 
            "admin.one", 
            "admin.two"
        ]    
        topics = "topic.one,topic.two,topic.three"
        """)
        
        # Mock a .toml file with the correct tables, but incorrect keys
        self.wrong_toml = self.test_temp_dir / "wrong.toml"
        self.wrong_toml.write_text("""
        [log]
        key = "wrong"
        [sql]
        key = "wrong"
        [hop]
        key = "wrong"  
        """)
        
        # Mock an uncorrectly formatted .toml file
        self.not_even_wrong_toml = self.test_temp_dir / "not_even_wrong.toml"
        self.not_even_wrong_toml.write_text("""
        [wrong]
        key = "wrong"
        """)
        
        self.sql_data = self.load_toml(
            toml_file=self.test_config_toml,
            key="sql"
        )
        self.hop_data = self.load_toml(
            toml_file=self.test_config_toml,
            key="hop"
        )
        
        # Set the path to the mock database file
        self.sql_file = f"{self.test_temp_dir / self.sql_data['file']}"
        
        # Construct a mocked sqlalchemy url
        self.test_sql_url = (
        f"{self.sql_data['rdms']}+"
        f"{self.sql_data['dbapi']}:///"
        f"{self.sql_data['file']}"
        )
        
        # Construct a mocked HOPSKOTCH broker url
        self.list_topics_url = (
        f"kafka://"
        f"{self.hop_data['username']}@"
        f"{self.hop_data['hostname']}:"
        f"{self.hop_data['port']}/"
        )
        self.test_hop_url = (
            f"{self.list_topics_url}"
            f"{self.hop_data['topics']}"
        )
        
        # Construct a mocked HOPSKOTCH consumer group
        self.group_id = (
            f"{self.hop_data['username']}-"
            f"{self.hop_data['groupname']}"
        )
        
        # Construct an ordered list of mocked topic and mocked messages 
        self.topics = []
        self.messages = []
        self.message_data = get_message_data()
        for key in self.message_data.keys(): 
            n = 1
            while n <= len(self.message_data):  
                self.topics.append(key)
                n += 1
            for payload in self.message_data[key]:
                self.messages.append(payload)
    
                
    def make_sql_url(self):  
        
        """Returns the mocked sqlalchemy url from the mocked 'config' file."""
        
        return (
        f"{self.sql_data['rdms']}+"
        f"{self.sql_data['dbapi']}:///"
        f"{self.sql_file}"
        )
    
    
    def make_hop_url(self):  
        
        """Returns the mocked sqlalchemy url from the mocked 'config' file."""
        
        return (self.test_hop_url)   
       
    def make_bad_url(self):  
        
        """Returns an unsupported sqlalchemy url."""
        
        return "bad_url" 
          
       
    def list_topics(self, url):
        
        """List the accessible topics on the mock broker.
    
        Args:
            url: The mock broker URL. 

        Returns:
            A dictionary of mocked topic names.
            
        Raises:
            ValueError: If the broker url is unsupported.
        
        """
        
        if url != self.list_topics_url:
            raise ValueError(f"Unsupported url: {url} in db.list_topics()")
        mock_topic_dict = {}
        for key in self.hop_data["list_topics"]:
            mock_topic_dict[key] = "value"
        return mock_topic_dict
        
            
    def load_auth(self, auth_file=None):
        
        """Configures an Auth instance given a configuration file.
        
        Args:
            auth_file: Path to a configuration file, loading from
                the default location if not given.
                
        Returns:
            A list of configured :class:'MockAuth' instances.
            
        """
        
        self.auth_file = auth_file
        self.auth_data = self.load_toml(
            toml_file=self.test_auth_toml,
            key="auth"
        )   
        return [MockAuth(self.auth_data)]


    def load_toml(self, toml_file, key):
    
        """Loads data from a mocked .toml file.
    
        Args:
            toml_file: Path to a .toml file.
            key: The specified .toml table from which to load data
    
        Returns:
            A dict containing the mocked .toml data.
    
        """
    
        with open(toml_file, "r") as f:
            try:
                toml_data = toml.loads(f.read())[key]
            except KeyError:
                raise RuntimeError(
                    f"configuration file has no section "
                    f"'[{key}]'"
                )
            except Exception as config_error:
                raise RuntimeError(
                    f"configuration file is not configured correctly: "
                    f"{config_error}"
                )
        return toml_data


def get_message_data():
    
    """Returns a dict conating the mocked topics and a list of mocked 
    messages for each given topic.
    
    """
    
    return {
        "topic.one" : [ 
        "{message: 1, topic: 'one', payload: '#first from topic.one'}",
        "{message: 2, topic: 'one', payload: '#second from topic.one'}",
        "{message: 3, topic: 'one', payload: '#third from topic.one'}"
        ],
        "topic.two" : [
        "{message: 4, topic: 'two', payload: '#first from topic.two'}",
        "{message: 5, topic: 'two', payload: '#second from topic.two'}",
        "{message: 6, topic: 'two', payload: '#third from topic.two'}"
        ],
        "topic.three" : [
        "{message: 7, topic: 'three', payload: '#first from topic.three'}",
        "{message: 8, topic: 'three', payload: '#second from topic.three'}",
        "{message: 9, topic: 'three', payload: '#third from topic.three'}"
        ]
    }




class TestSettings:
    
    @pytest.fixture
    def mock_toml(self, tmp_path):
        return MockToml(tmp_path)
    
    def test_toml(self, mock_toml):
        assert db.Settings(file=mock_toml.test_config_toml)
        with pytest.raises(RuntimeError):
            db.Settings(file=mock_toml.not_even_wrong_toml)
        with pytest.raises(FileNotFoundError):
            db.Settings(file="none.toml")
            
    def test_make_log(self, mock_toml):
        
        # Test for relevant config file error handling
        settings = db.Settings(file=mock_toml.wrong_toml)
        with pytest.raises(RuntimeError):
                settings.make_log()
        
       
    def test_make_sql_url(self, mock_toml):
        settings = db.Settings(file=mock_toml.test_config_toml)
        assert settings.make_sql_url() == mock_toml.test_sql_url
        
        # Test for relevant config file error handling
        settings = db.Settings(file=mock_toml.wrong_toml)
        with pytest.raises(RuntimeError):
                settings.make_sql_url()

    def test_make_hop_url(self, mock_toml):
        settings = db.Settings(file=mock_toml.test_config_toml)
        with patch("db.list_topics", mock_toml.list_topics):
            assert settings.make_hop_url() == mock_toml.test_hop_url
                
        # Test for relevant config file error handling
        settings = db.Settings(file=mock_toml.wrong_toml)
        with pytest.raises(RuntimeError):
            settings.make_hop_url() 


    def test_create_session(self, mock_toml):
        settings = db.Settings(file=mock_toml.test_config_toml)
        mock_session = settings.create_session()
        assert mock_session.execute(select(db.MessagesTable))
        #remove(mock_toml.sql_file)
        
        # Test for relevant config file error handling
        settings = db.Settings(file=mock_toml.wrong_toml)
        with pytest.raises(RuntimeError):
            settings.create_session() 
    
    def test_create_current(self, mock_toml):
        settings = db.Settings(file=mock_toml.test_config_toml)
        with patch("db.Stream", MockStream),\
        patch("db.Settings.make_hop_url", mock_toml.make_hop_url):
            mock_current = settings.create_current()
            assert isinstance(mock_current, MockConsumer)
            assert mock_current.start_at == StartPosition.EARLIEST
            assert not mock_current.until_eos
            assert mock_toml.group_id in mock_current.group_id
    
        # Test for relevant config file error handling
        settings = db.Settings(file=mock_toml.wrong_toml)
        with patch("db.Stream", MockStream),\
        patch("db.Settings.make_hop_url", mock_toml.make_hop_url):  
            with pytest.raises(RuntimeError):
                settings.create_current() 
    
    
    
class TestFunctions:
    
    @pytest.fixture
    def mock_toml(self, tmp_path):
        return MockToml(tmp_path)
                
        
    def test_insert_messages(self, mock_toml):
        
        # Reset :dataclass:'MockAdc' consumer group parameters
        MockAdc.processed_messages = []
        MockAdc.consumer_group = None

        #        patch("db.load_auth", mock_toml.load_auth),\
        settings = db.Settings(file=mock_toml.test_config_toml)
        with patch("db.list_topics", mock_toml.list_topics),\
        patch("db.Stream", MockStream),\
        patch("db.Settings.make_sql_url", mock_toml.make_sql_url):
            mock_session = settings.create_session() 
            mock_current = settings.create_current()
            
            # Test for database operational error handling
            remove(mock_toml.sql_file) 
            with pytest.raises(RuntimeError):
                db.insert_messages(
                    session=mock_session,
                    current=mock_current
                )
            
            # Reset session and and proceed with testing 
            # that mocked values were added to database
            mock_session = settings.create_session() 
            db.insert_messages(
                    session=mock_session,
                    current=mock_current
                )
            results = mock_session.execute(
                select(db.MessagesTable)
            )
            n = 0
            for result in results.scalars():
                assert result.uid == n + 1 
                assert result.topic == mock_toml.topics[n]
                assert result.payload == mock_toml.messages[n]
                assert result.timestamp >= 0
                n += 1
            assert n == len(mock_toml.topics)
        remove(mock_toml.sql_file)               
                        
                        
    def test_main(self, mock_toml):
        
        # Reset :dataclass:'MockAdc' consumer group parameters
        MockAdc.processed_messages = []
        MockAdc.consumer_group = None
        
        #Set to trigger a 'disconnect' in :meth:'MockConsumer.read'
        MockAdc.disconnect = True
      
        # Test for :class:'Sqlalchemy.Session' error handling protocol
        with patch("db.Settings.make_sql_url", mock_toml.make_bad_url):
            with pytest.raises(Exception): 
                db.main(test=True, n=1)

        # Test for dealing with HOPSKOTCH disconnect errors 
        # using :dataclass:'MockAdc' consumer group settings
        with patch("db.Settings.make_sql_url", mock_toml.make_sql_url),\
        patch("db.Settings.make_hop_url", mock_toml.make_hop_url),\
        patch("db.list_topics", mock_toml.list_topics),\
        patch("db.Stream", MockStream): 
            mock_session = db.main(test=True, n=4)

            # Test that mocked values were added to database
            results = mock_session.execute(
                select(db.MessagesTable)
            )
            n = 0
            for result in results.scalars():
                assert result
                assert result.uid == n + 1 
                assert result.topic == mock_toml.topics[n]
                assert result.payload == mock_toml.messages[n]
                assert result.timestamp >= 0
                n += 1
            assert n == len(mock_toml.topics)                  
        remove(mock_toml.sql_file)


    def test_init (self):

        # Test for correct arguemnt settings on :meth:'db.main'
        with patch("db.main") as mock_main:
            with patch("db.__name__", '__main__'):
                db.init()
                mock_main.assert_called_once_with(test=False, n=None)



        
        
        
        
        