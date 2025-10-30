from google.cloud import storage
from google.oauth2 import service_account
from loguru import logger

class TaskFailure(Exception):
	pass

def download_from_bucket(bucket_uri: str, file_uuid: str, from_file="/tmp/credentials.json"):
	credentials = service_account.Credentials.from_service_account_file(
		from_file,
	)
	client = storage.Client(credentials=credentials)

	path_parts = bucket_uri[len("gs://"):].split("/", maxsplit=1)
	bucket_name = path_parts[0]
	blob_name = path_parts[1] if len(path_parts) > 1 else ""

	bucket = client.get_bucket(bucket_name)
	blob = bucket.blob(blob_name)

	FILENAME = f"{file_uuid}.gdb"
	with open(f"/data/{FILENAME}", "w+") as f:
		file_path = f.name
		logger.info(f"Downloading '{bucket_uri}' to file '{file_path}'")
		blob.download_to_filename(file_path)
	return FILENAME


def upload_to_bucket(
	src_filepath: str,
	bucket_name: str,
	dest_blob_name: str,
	file_ext: str,
	from_file="/tmp/credentials.json"
):
	credentials = service_account.Credentials.from_service_account_file(
		from_file,
	)
	client = storage.Client(credentials=credentials)

	bucket = client.bucket(bucket_name)
	# Problema: arquivo com esse nome talvez já exista no GCS
	# Aqui testamos se o nome já existe e, se sim, iteramos por
	# sufixos -export1, -export2, ... até que funcione
	blob = None
	output_uri = None
	dest_filepath = dest_blob_name
	i = 0
	while True:
		if i > 0:
			dest_filepath = f"{dest_blob_name}-export{i}"

		blob = bucket.blob(f"{dest_filepath}.{file_ext}")
		output_uri = f"gs://{bucket_name}/{dest_filepath}.{file_ext}"

		if not blob.exists():
			break

		logger.warning(f"'{output_uri}' exists! Retrying with suffix change")
		i += 1

	blob.upload_from_filename(src_filepath)

	logger.info(
		f"File '{src_filepath}' uploaded to '{output_uri}'"
	)
	return output_uri
