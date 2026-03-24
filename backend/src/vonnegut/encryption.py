# backend/src/vonnegut/encryption.py
import os
from pathlib import Path

from cryptography.fernet import Fernet

_DEFAULT_KEY_PATH = Path.home() / ".vonnegut" / "secret.key"


def get_or_create_key(
    env_key: str = "VONNEGUT_SECRET_KEY",
    key_path: Path = _DEFAULT_KEY_PATH,
) -> str:
    env_val = os.environ.get(env_key)
    if env_val:
        return env_val

    if key_path.exists():
        return key_path.read_text().strip()

    key = Fernet.generate_key().decode()
    key_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    key_path.write_text(key)
    key_path.chmod(0o600)
    return key


def encrypt(plaintext: str, key: str) -> str:
    f = Fernet(key.encode() if isinstance(key, str) else key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str, key: str) -> str:
    f = Fernet(key.encode() if isinstance(key, str) else key)
    return f.decrypt(ciphertext.encode()).decode()
