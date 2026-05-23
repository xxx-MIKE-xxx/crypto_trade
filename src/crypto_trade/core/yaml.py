import yaml
from pathlib import Path
from crypto_trade.core.paths import CONFIG_DIR

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        yaml_file = yaml.safe_load(f)
    return yaml_file

def get_yaml_value(path, *keys):
    data = load_yaml(path)
    for key in keys:
        data = data[key]
    return data
