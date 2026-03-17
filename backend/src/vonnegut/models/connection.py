from datetime import datetime
from typing import Literal

from pydantic import BaseModel, model_validator

from vonnegut.encryption import encrypt, decrypt

_SENSITIVE_FIELDS = {"password"}


def encrypt_config(config: dict, key: str) -> dict:
    result = dict(config)
    for field in _SENSITIVE_FIELDS:
        if field in result:
            result[field] = encrypt(result[field], key)
    return result


def decrypt_config(config: dict, key: str) -> dict:
    result = dict(config)
    for field in _SENSITIVE_FIELDS:
        if field in result:
            result[field] = decrypt(result[field], key)
    return result


class PostgresDirectConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    user: str
    password: str


class PostgresPodConfig(BaseModel):
    namespace: str
    pod_selector: str
    pick_strategy: Literal["first_ready", "name_contains"] = "first_ready"
    pick_filter: str | None = None
    container: str | None = None
    database: str = ""
    user: str
    password: str
    local_port: int | None = None


class ConnectionCreate(BaseModel):
    name: str
    type: Literal["postgres_direct", "postgres_pod"]
    config: dict

    @property
    def parsed_config(self) -> PostgresDirectConfig | PostgresPodConfig:
        if self.type == "postgres_direct":
            return PostgresDirectConfig(**self.config)
        return PostgresPodConfig(**self.config)

    @model_validator(mode="after")
    def validate_config(self):
        self.parsed_config
        return self


class ConnectionUpdate(BaseModel):
    name: str | None = None
    config: dict | None = None


class Connection(BaseModel):
    id: str
    name: str
    type: Literal["postgres_direct", "postgres_pod"]
    config: dict
    created_at: str
    updated_at: str


class ConnectionResponse(BaseModel):
    id: str
    name: str
    type: Literal["postgres_direct", "postgres_pod"]
    config: dict
    created_at: str
    updated_at: str

    @model_validator(mode="after")
    def mask_password(self):
        if "password" in self.config:
            self.config = {**self.config, "password": "********"}
        return self
