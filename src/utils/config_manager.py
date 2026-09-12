import yaml
from pathlib import Path
from typing import Dict, Any

class ConfigManager:
    _instance = None

    def __new__(cls, config_path: str = "config/sim_config.yaml"):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance

    def _load_config(self, config_path: str):
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        
        with open(path, "r") as file:
            self._config = yaml.safe_load(file)

    @property
    def limits(self) -> Dict[str, Dict[str, float]]:
        return self._config.get("engine", {}).get("limits", {})

    @property
    def tolerances(self) -> Dict[str, float]:
        return self._config.get("engine", {}).get("tolerances", {})

    @property
    def gradients(self) -> Dict[str, float]:
        return self._config.get("engine", {}).get("gradients", {})