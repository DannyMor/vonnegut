from typing import Annotated, Literal, Union

from pydantic import BaseModel, Discriminator, Tag, field_validator, model_validator

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
    type: Literal["postgres_direct"]
    host: str
    port: int = 5432
    database: str
    user: str
    password: str


class PostgresPodConfig(BaseModel):
    type: Literal["postgres_pod"]
    namespace: str
    pod_selector: str
    pick_strategy: Literal["first_ready", "name_contains"] = "first_ready"
    pick_filter: str | None = None
    container: str | None = None
    host: str
    port: int = 5432
    database: str = ""
    user: str
    password: str


ConnectionConfig = Annotated[
    Union[
        Annotated[PostgresDirectConfig, Tag("postgres_direct")],
        Annotated[PostgresPodConfig, Tag("postgres_pod")],
    ],
    Discriminator("type"),
]


class ConnectionCreate(BaseModel):
    name: str
    config: ConnectionConfig

    @field_validator("name", mode="before")
    @classmethod
    def trim_name(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v


class ConnectionUpdate(BaseModel):
    name: str | None = None
    config: ConnectionConfig | None = None


class ConnectionResponse(BaseModel):
    id: str
    name: str
    config: dict
    created_at: str
    updated_at: str

    @model_validator(mode="after")
    def mask_password(self):
        if "password" in self.config:
            self.config = {**self.config, "password": "********"}
        return self
