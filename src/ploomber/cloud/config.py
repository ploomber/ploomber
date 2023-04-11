from pathlib import Path
from typing import Mapping

import yaml
from pydantic import BaseModel


class TaskResource(BaseModel):
    vcpus: int = None
    memory: int = None
    gpu: int = None

    class Config:
        extra = "forbid"


class CloudConfig(BaseModel):
    environment: str = None
    task_resources: Mapping[str, TaskResource] = None

    class Config:
        extra = "forbid"


def validate():
    path = Path("cloud.yaml")

    # cloud.yaml is optional
    if not path.is_file():
        return

    cfg = yaml.safe_load(path.read_text())
    CloudConfig(**cfg)
