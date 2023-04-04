let config = {};
let hostname = process.env.REACT_APP_REMOTE_HOSTNAME || "127.0.0.1";
let port = process.env.REACT_APP_REMOTE_PORT || "8000";
let protocol = process.env.REACT_APP_REMOTE_PROTOCOL || "http";

config["hostname"] = hostname;
config["port"] = port;
config["protocol"] = protocol;
config["baseUrl"] = '/';
config["baseApiUrl"] = '/api';
config["endpointTest"] = '/test';

export default config;
