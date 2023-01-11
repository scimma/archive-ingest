"""
    Utility  functions 
"""


def merge_config(args):
    """
    produce a configuration dictionary from inpud dictionary args.

    merge hard defaults, args and stanza from toml file into
    a unified configuration dictionary.
    """
    import toml
    hard_defaults = {"region": "us-west-2", "loglevel": "INFO"}
    # this is a poor assumption.
    stanzas = [stanza for stanza in args.keys() if "stanza" in stanza]
    if "toml_file" in args: toml_data = toml.load(args["toml_file"])
    for stanza in stanzas:
        hard_defaults.update(toml_data[args[stanza]])
    hard_defaults.update(args)
    return hard_defaults


def setup_logging(args):
    "return a logger for program use" 
    config = merge_config(args) 
    import logging
    import sys
    logger = logging.getLogger(__name__)
    log_level = config["level"]
    logger.setLevel(logging.__dict__[log_level])
    console_handler = logging.StreamHandler(sys.stdout)
    #console_handler.setFormatter(FORMATTER)
    return logger


def terse(object):
    "shotren the text used to represent an object"
    text = object.__repr__()
    max_length = 30
    if len(text) > max_length:
        return text[:max_length-3] + '...'
    return text


def ask(args, key='quiet'):
    "query stdin before proceeding, based on args[key]}"
    import sys
    if not args[key]:
        print ("> p:pdb; q:quit_ask_mode; anything_else: continue")
        sys.stdout.write(">> ")
        answer = sys.stdin.readline()
        if answer[0].lower() == 'q':
            args[key] = True
        elif answer[0].lower() == 'p':
            import pdb; pdb.set_trace()
        else:
            pass