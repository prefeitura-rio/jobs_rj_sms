# -*- coding: utf-8 -*-
import os
import uuid
import shutil
import requests
import traceback
# from time import sleep
from loguru import logger
from celery import Celery, Task, states

import auth  # ./auth.py
import utils  # ./utils.py


auth.inject_environment_variables(environment="dev") # FIXME
auth.prepare_gcp_credentials()

celery_app = Celery(
	"celery",
	backend=os.environ.get("REDIS_SERVER"),
	broker=os.environ.get("REDIS_SERVER"),
)

#############################

@celery_app.task(name="dummy.task", bind=True)
def dummy_task(self: Task):
	logger.info("Dummy!")
	return { "success": True }


@celery_app.task(name="export.task", bind=True)
def export_task(self: Task, gcs_uri: str):
	if not gcs_uri.startswith("gs://"):
		state = f"Malformed bucket URI: '{gcs_uri}'"
		self.update_state(state=states.FAILURE, meta={ "status": state })
		logger.warning(state)
		return

	TOTAL_TASKS = 6
	FILE_UUID = (
		str(self.request.id)
		if hasattr(self.request, "id")
		else str(uuid.uuid4())
	)
	CSV_PATH = "/data/csv"

	# ex.: 'gs://bucket_name/path/to/my/file/BACKUP.GDB'
	#      => [ 'bucket_name/path/to/my/file', 'BACKUP.GDB' ]
	(gcs_full_path, gcs_filename) = gcs_uri[5:].rsplit("/", maxsplit=1)
	# 'VERY.IMPORTANT.BACKUP.GDB' => 'VERY.IMPORTANT.BACKUP'
	original_file_name = gcs_filename.rsplit(".", maxsplit=1)[0]
	# => [ 'bucket_name', 'path/to/my/file' ]
	(bucket_name, gcs_path) = gcs_full_path.split("/", maxsplit=1)

	try:
		########################################
		# (1) Baixa o arquivo do bucket
		state = f"Downloading '{gcs_uri}'..."
		self.update_state(state="PROGRESS", meta={
			"status": state,
			"current": 1,
			"total": TOTAL_TASKS
		})
		logger.info(state)
		gdb_filename = utils.download_from_bucket(gcs_uri, FILE_UUID)


		########################################
		# (2) Requisita a exportação pelo outro Docker
		if os.path.exists(CSV_PATH):
			logger.info(f"Found '{len(os.listdir(CSV_PATH))}' file(s) before export; deleting...")
			shutil.rmtree(CSV_PATH)

		state = f"Requesting export of file '{gdb_filename}'..."
		self.update_state(state="PROGRESS", meta={
			"status": state,
			"current": 2,
			"total": TOTAL_TASKS
		})
		logger.info(state)
		EXPORT_SERVER = os.environ.get("EXPORT_SERVER")
		requests.get(f"{EXPORT_SERVER}/export/{gdb_filename}")
		logger.info(f"Found '{len(os.listdir(CSV_PATH))}' file(s) after export")


		########################################
		# (3) Adiciona CSVs resultantes em .ZIP
		state = "Zipping results..."
		self.update_state(state="PROGRESS", meta={
			"status": state,
			"current": 3,
			"total": TOTAL_TASKS
		})
		logger.info(state)
		zip_filepath = shutil.make_archive(FILE_UUID, format="zip", root_dir=CSV_PATH)
		logger.info(f"Created '{zip_filepath}'")


		########################################
		# (4) Faz upload para o bucket
		compressed_file_ext = "unknown"
		if zip_filepath.endswith(".zip"):
			compressed_file_ext = "zip"
		# ...
		state = f"Uploading as 'gs://{bucket_name}/{gcs_path}/{original_file_name}.{compressed_file_ext}'..."
		self.update_state(state="PROGRESS", meta={
			"status": state,
			"current": 4,
			"total": TOTAL_TASKS
		})
		logger.info(state)
		output_uri = utils.upload_to_bucket(
			zip_filepath,
			bucket_name,
			f"{gcs_path}/{original_file_name}",
			compressed_file_ext
		)


		########################################
		# (5) Remove arquivos
		state = "Cleaning up..."
		self.update_state(state="PROGRESS", meta={
			"status": state,
			"current": 5,
			"total": TOTAL_TASKS
		})
		logger.info(state)
		shutil.rmtree(CSV_PATH)
		os.remove(f"/data/{gdb_filename}")
		os.remove(zip_filepath)

		return { "success": True, "output": output_uri }

	except Exception as ex:
		self.update_state(
			state=states.FAILURE,
			meta={
				"exc_type": type(ex).__name__,
				"exc_message": traceback.format_exc().split("\n"),
			},
		)

		raise ex
