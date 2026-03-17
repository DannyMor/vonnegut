# backend/tests/test_encryption.py
import os

from vonnegut.encryption import decrypt, encrypt, get_or_create_key


def test_encrypt_decrypt_roundtrip():
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET")
    plaintext = "my-secret-password"
    encrypted = encrypt(plaintext, key)
    assert encrypted != plaintext
    decrypted = decrypt(encrypted, key)
    assert decrypted == plaintext


def test_encrypt_produces_different_ciphertexts():
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET")
    encrypted1 = encrypt("same-value", key)
    encrypted2 = encrypt("same-value", key)
    # Fernet uses random IV, so ciphertexts differ
    assert encrypted1 != encrypted2


def test_get_key_from_env(monkeypatch, tmp_path):
    from cryptography.fernet import Fernet

    test_key = Fernet.generate_key().decode()
    monkeypatch.setenv("VONNEGUT_SECRET_KEY", test_key)
    key = get_or_create_key(
        env_key="VONNEGUT_SECRET_KEY", key_path=tmp_path / "secret.key"
    )
    assert key == test_key


def test_get_key_creates_file(tmp_path):
    key_path = tmp_path / "secret.key"
    assert not key_path.exists()
    key = get_or_create_key(env_key="TEST_KEY_NOT_SET", key_path=key_path)
    assert key_path.exists()
    assert key == key_path.read_text().strip()


def test_get_key_reuses_existing_file(tmp_path):
    key_path = tmp_path / "secret.key"
    key1 = get_or_create_key(env_key="TEST_KEY_NOT_SET", key_path=key_path)
    key2 = get_or_create_key(env_key="TEST_KEY_NOT_SET", key_path=key_path)
    assert key1 == key2
