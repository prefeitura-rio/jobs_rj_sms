import os
import json
import shutil
from pydantic import BaseModel

from celery import Celery
from fastapi import FastAPI

celery_app = Celery(
	"celery",
	backend=os.environ.get("REDIS_SERVER"),
	broker=os.environ.get("REDIS_SERVER"),
)
app = FastAPI()

#############################

def format_bytes(size):
	# [Ref] https://stackoverflow.com/a/49361727/4824627
	power = 2**10  # 2**10 = 1024
	n = 0
	power_labels = {0 : "", 1: "K", 2: "M", 3: "G", 4: "T", 5: "P"}
	while size > power:
		size /= power
		n += 1
	return f"{size:.2f} {power_labels[n]}B"

@app.get("/list/")
async def list_files():
	out = dict()
	for root, _, files in os.walk("/data"):
		out[root] = [
			(
				filename,
				format_bytes(
					os.path.getsize(os.path.join(root, filename))
				)
			)
			for filename in files
		]
	return out

@app.get("/clear/")
async def list_files():
	PATH = "/data"
	for item in os.listdir(PATH):
		item_path = os.path.join(PATH, item)
		if os.path.isfile(item_path):
			os.remove(item_path)
		elif os.path.isdir(item_path):
			shutil.rmtree(item_path)
	return os.listdir("/data")


class ExportRequest(BaseModel):
	gcs_uri: str

@app.post("/export/")
async def request_export(
	req: ExportRequest
):
	try:
		task = celery_app.send_task("export.task", args=[req.gcs_uri])
	except Exception as e:
		return { "success": False, "error": repr(e) }
	return { "success": True, "id": task.id }


@app.get("/dummy/")
def dummy_task():
	try:
		task = celery_app.send_task("dummy.task")
	except Exception as e:
		return { "success": False, "error": repr(e) }
	return { "success": True, "id": task.id }


@app.get("/check/{id}")
def check_task(id: str):
	task = celery_app.AsyncResult(id)
	if task.state == "SUCCESS":
		response = {
			"status": task.state,
			"result": task.result,
			"task_id": id,
		}
	elif task.state == "FAILURE":
		response = json.loads(
			task.backend.get(
				task.backend.get_key_for_task(task.id),
			).decode("utf-8")
		)
		del response["children"]
		del response["traceback"]
	else:
		response = {
			"status": task.state,
			"result": task.info,
			"task_id": id,
		}
	return response
