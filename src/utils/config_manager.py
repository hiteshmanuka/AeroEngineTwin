import yaml
from box import Box
from pathlib import Path
from typing import Dict, Any

class SimConfigManager:
    _instance = None

    def __new__(cls, config_path: str = "config/sim_config.yaml"):
        if cls._instance is None:
            cls._instance = super(SimConfigManager, cls).__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance

    def _load_config(self, config_path: str):
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        
        with open(path, "r") as file:
            self._config = Box(yaml.safe_load(file), default_box=True)

    @property
    def limits(self) -> Box:
        return self._config.engine.limits

    @property
    def tolerances(self) -> Box:
        return self._config.engine.tolerances

    @property
    def gradients(self) -> Box:
        return self._config.engine.gradients

    @property
    def downsampling(self) -> Box:
        return self._config.downsampling
    
    @property
    def config(self) -> Box:
        return self._config

class ArtifactConfigManager:
    _instance = None

    def __new__(cls, config_path: str = "config/artifacts.yaml"):
            if cls._instance is None:
                cls._instance = super(ArtifactConfigManager, cls).__new__(cls)
                cls._instance._load_config(config_path)
            return cls._instance
    
    def _load_config(self, config_path: str):
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        
        with open(path, "r") as file:
            self._config = Box(yaml.safe_load(file), default_box=True)

    @property
    def sim_paths(self) -> Box:
        return self._config.sim_paths
