from dataclasses import dataclass
from typing import Dict
import json

@dataclass
class RemoteDBConfig:
    database1: str
    database2: str
    database3: str

@dataclass
class LocalDBPaths:
    database1: str
    database2: str
    database3: str

@dataclass
class OtherConfig:
    app_name: str
    version: str

@dataclass
class Config:
    use_remote_db: bool
    remote_db_config: RemoteDBConfig
    local_db_paths: LocalDBPaths
    other_config: OtherConfig

def load_config(config_file: str) -> Config:
    with open(config_file, 'r') as f:
        config_dict = json.load(f)

    return Config(
        use_remote_db=config_dict["use_remote_db"],
        remote_db_config=RemoteDBConfig(**config_dict["remote_db_config"]),
        local_db_paths=LocalDBPaths(**config_dict["local_db_paths"]),
        other_config=OtherConfig(**config_dict["other_config"])
    )