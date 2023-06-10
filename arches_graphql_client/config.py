import yaml

_CONFIGURATION = {}

def load(config_file):
    global _CONFIGURATION
    with open(config_file, "r") as f:
        config = yaml.load(f)
    _CONFIGURATION.update(config)

def get(key):
    return _CONFIGURATION.get(key, {})
