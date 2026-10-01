"""Generate an RSA key pair for the Snowflake service user and print the SQL to register it.

Run from the project folder:
    docker compose run --rm airflow python /opt/airflow/scripts/generate_keys.py
"""
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

KEY_DIR = Path("/opt/airflow/keys")
passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
if not passphrase or passphrase == "change-me-to-something-long":
    raise SystemExit("Set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE in your .env file first.")

private_path = KEY_DIR / "rsa_key.p8"
public_path = KEY_DIR / "rsa_key.pub"

if private_path.exists():
    print(f"{private_path} already exists - reusing it (delete it to make a new pair).")
    key = serialization.load_pem_private_key(private_path.read_bytes(), passphrase.encode())
else:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(passphrase.encode()),
        )
    )
    os.chmod(private_path, 0o644)  # Superset's container must be able to read it

public_pem = key.public_key().public_bytes(
    serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
)
public_path.write_bytes(public_pem)

body = "".join(
    line for line in public_pem.decode().splitlines() if "PUBLIC KEY" not in line
)
print("\nKeys written to ./keys/. Paste this value into snowflake/setup.sql:\n")
print(f"RSA_PUBLIC_KEY = '{body}'\n")
