let config = {};
let hostname = process.env.REACT_APP_REMOTE_HOSTNAME || "127.0.0.1";
let port = process.env.REACT_APP_REMOTE_PORT || "8000";
let protocol = process.env.REACT_APP_REMOTE_PROTOCOL || "http";
let apiBasePath = process.env.REACT_APP_REMOTE_BASEPATH || "api";

config["hostname"] = hostname;
config["port"] = port;
config["protocol"] = protocol;
config["baseUrl"] = '/';
if (apiBasePath === "") {
    config["baseApiUrl"] = `/api`;
} else {
    config["baseApiUrl"] = `/${apiBasePath}/api`;
}

export default config;
