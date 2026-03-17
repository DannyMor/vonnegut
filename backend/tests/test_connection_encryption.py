# backend/tests/test_connection_encryption.py
import json

from vonnegut.encryption import get_or_create_key, decrypt
from vonnegut.models.connection import encrypt_config, decrypt_config


def test_encrypt_config_encrypts_password():
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET")
    config = {"host": "localhost", "port": 5432, "database": "db", "user": "admin", "password": "secret123"}
    encrypted = encrypt_config(config, key)
    assert encrypted["host"] == "localhost"
    assert encrypted["port"] == 5432
    assert encrypted["password"] != "secret123"
    assert decrypt(encrypted["password"], key) == "secret123"


def test_decrypt_config_decrypts_password():
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET")
    config = {"host": "localhost", "port": 5432, "database": "db", "user": "admin", "password": "secret123"}
    encrypted = encrypt_config(config, key)
    decrypted = decrypt_config(encrypted, key)
    assert decrypted["password"] == "secret123"
    assert decrypted["host"] == "localhost"


def test_encrypt_config_without_password():
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET")
    config = {"host": "localhost", "port": 5432}
    encrypted = encrypt_config(config, key)
    assert encrypted == config


def test_roundtrip_preserves_all_fields():
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET")
    config = {
        "namespace": "prod",
        "pod_name": "pg-0",
        "container": "postgres",
        "database": "mydb",
        "user": "admin",
        "password": "pod-secret",
        "local_port": 15432,
    }
    encrypted = encrypt_config(config, key)
    decrypted = decrypt_config(encrypted, key)
    assert decrypted == config
