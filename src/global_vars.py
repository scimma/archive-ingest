import yaml
import os
import logging

## Load configuration file
with open('config.yaml', "r") as conf_file:
    config = yaml.load(conf_file, Loader=yaml.FullLoader)

## Load secrets from env vars (compatible with Kubernetes Secrets)
config['db']['host'] = os.environ['DB_HOST']
config['db']['user'] = os.environ['POSTGRES_USER']
config['db']['pass'] = os.environ['POSTGRES_PASSWORD']
config['db']['database'] = os.environ['POSTGRES_DB']

# Configure logging
logging.basicConfig(
    format='%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s',
)
log = logging.getLogger("app")
try:
    log.setLevel(config['server']['logLevel'].upper())
except:
    log.setLevel('WARNING')

## Load secret configuration

def import_secret_config(in_conf: dict, secret_conf: dict, path=[]):
    conf = dict(in_conf)
    for key in secret_conf:
        # log.debug(f'''key: {key}''')
        if key in conf:
            if isinstance(conf[key], dict) and isinstance(secret_conf[key], dict):
                conf[key] = import_secret_config(conf[key], secret_conf[key], path.append(str(key)))
            elif conf[key] == secret_conf[key]:
                pass
            else:
                log.warning(f'''Duplicate key "{key}" with conflicting value found in secret config. Overriding initial value.''')
                conf[key] = secret_conf[key]
        else:
            conf[key] = secret_conf[key]
    return conf

secret_config = {}
if os.path.isfile('secret.yaml'):
    try:
        with open('secret.yaml', "r") as conf_file:
            secret_config = yaml.load(conf_file, Loader=yaml.FullLoader)
    except Exception as e:
        log.error(f'''Error reading secret configuration: {e}''')
try:
        config = import_secret_config(config, secret_config)
    # log.debug(f'''conf: {yaml.dump(config, indent=2)}''')
except Exception as e:
    log.error(f'''Error importing secret configuration: {e}''')

