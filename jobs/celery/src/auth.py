# -*- coding: utf-8 -*-
import os
import base64

from loguru import logger
from infisical import InfisicalClient


def getenv_or_action(env_name: str, *, action: str = "raise", default: str = None) -> str:
	"""Get an environment variable or raise an exception.

	Args:
		env_name (str): The name of the environment variable.
		action (str, optional): The action to take if the environment variable is not set.
			Defaults to "raise".
		default (str, optional): The default value to return if the environment variable is not set.
			Defaults to None.

	Raises:
		ValueError: If the action is not one of "raise", "warn", or "ignore".

	Returns:
		str: The value of the environment variable, or the default value if the environment variable
			is not set.
	"""
	if action not in ["raise", "warn", "ignore"]:
		raise ValueError("action must be one of 'raise', 'warn', or 'ignore'")

	value = os.getenv(env_name, default)
	if value is None:
		if action == "raise":
			raise EnvironmentError(
				f"Environment variable {env_name} is not set.")
		elif action == "warn":
			logger.warning(
				f"Warning: Environment variable {env_name} is not set.")
	return value


def inject_environment_variables(environment: str):
	"""Inject environment variables from Infisical."""
	site_url = getenv_or_action("INFISICAL_ADDRESS", action="raise")
	token = getenv_or_action("INFISICAL_TOKEN", action="raise")
	infisical_client = InfisicalClient(
		token=token,
		site_url=site_url,
	)
	secrets = infisical_client.get_all_secrets(
		environment=environment, attach_to_os_environ=True)
	logger.info(
		f"Injecting {len(secrets)} environment variables from Infisical:")
	for secret in secrets:
		logger.info(
			f" - {secret.secret_name}: {len(secret.secret_value)} chars")


def prepare_gcp_credentials() -> None:
	base64_credential = os.environ["BASEDOSDADOS_CREDENTIALS_PROD"]

	# Create tmp directory if it doesn't exist
	os.makedirs("/tmp", exist_ok=True)

	logger.info("Creating '/tmp/credentials.json'...")
	with open("/tmp/credentials.json", "wb") as f:
		f.write(base64.b64decode(base64_credential))
