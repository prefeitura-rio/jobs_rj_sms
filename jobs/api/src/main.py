# -*- coding: utf-8 -*-
import os
import json
import shutil
from typing import Annotated
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from celery import Celery
from fastapi import Depends, FastAPI
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from loguru import logger

import auth  # ./auth.py
import utils  # ./utils.py
from constants import constants as const  # ./constants.py

from passlib.context import CryptContext


auth.inject_environment_variables(environment=os.environ.get("ENVIRONMENT", "dev"))
auth.prepare_gcp_credentials()

USERNAME = auth.getenv_or_action("GDB_EXPORT_USERNAME", action="raise")
PASSWORD = auth.getenv_or_action("GDB_EXPORT_PW_HASH", action="raise")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

celery_app = Celery(
	"celery",
	backend=os.environ.get("REDIS_SERVER"),
	broker=os.environ.get("REDIS_SERVER"),
)

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


#############################

@app.post("/token")
async def authenticate(
	form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
):
	username = form_data.username or None
	password = form_data.password or None

	# [Ref] https://oauth.com/oauth2-servers/access-tokens/access-token-response/

	correct_password = pwd_context.verify(password, PASSWORD)
	if username != USERNAME or not correct_password:
		state = "Invalid username/password"
		logger.warning(state)
		return JSONResponse(
			status_code=400,
			headers={ "Cache-Control": "no-store" },
			content={
				"status": "ERROR",
				"error": "invalid_client",
				"error_description": state,
			},
		)

	access_token = auth.create_access_token({ "sub": username })
	return JSONResponse(
		status_code=200,
		headers={ "Cache-Control": "no-store" },
		content={
			"status": "SUCCESS",
			"access_token": access_token,
			"token_type": "Bearer",
			"expires_in": (
				const.JWT_ACCESS_TOKEN_EXPIRE_MINUTES.value * 60
			)
		},
	)


@app.get("/list/")
async def list_files(token: Annotated[str, Depends(oauth2_scheme)]):
	out = dict()
	for root, _, files in os.walk("/data"):
		out[root] = [
			(
				filename,
				utils.format_bytes(
					os.path.getsize(os.path.join(root, filename))
				)
			)
			for filename in files
		]
	return out


@app.get("/clear/")
async def list_files(token: Annotated[str, Depends(oauth2_scheme)]):
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
	token: Annotated[str, Depends(oauth2_scheme)],
	req: ExportRequest,
):
	try:
		task = celery_app.send_task("export.task", args=[req.gcs_uri])
	except Exception as e:
		return { "success": False, "error": repr(e) }
	return { "success": True, "id": task.id }


@app.get("/dummy/")
def dummy_task(
	token: Annotated[str, Depends(oauth2_scheme)],
):
	try:
		task = celery_app.send_task("dummy.task")
	except Exception as e:
		return { "success": False, "error": repr(e) }
	return { "success": True, "id": task.id }


@app.get("/check/{id}")
def check_task(
	token: Annotated[str, Depends(oauth2_scheme)],
	id: str,
):
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
