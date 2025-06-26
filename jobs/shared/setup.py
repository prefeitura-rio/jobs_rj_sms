#! /usr/bin/env python3
import os
import importlib
import json

import base64
from infisical import InfisicalClient


# -----------------------------
# ENVIRONMENT SETUP
# -----------------------------
token = os.getenv("INFISICAL_TOKEN")
site_url = os.getenv("INFISICAL_ADDRESS")
environment = os.getenv("ENVIRONMENT")

# -----------------------------
# SECRETS SETUP
# -----------------------------
client = InfisicalClient(
    token=token,
    site_url=site_url,
)
secrets = client.get_all_secrets(path="/")
for secret in secrets:
    os.environ[secret.secret_name] = secret.secret_value

# -----------------------------
# GOOGLE CLOUD SETUP
# -----------------------------
service_account = base64.b64decode(
    os.environ.get(f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}")
)
with open("/tmp/credentials.json", "wb") as credentials_file:
    credentials_file.write(service_account)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"