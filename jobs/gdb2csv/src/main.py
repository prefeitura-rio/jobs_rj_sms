from fastapi import FastAPI

from export import export

app = FastAPI()

@app.get("/")
async def abcdef():
	return { 
		"endpoints": {
			"/export": repr(export)
		}
	}


@app.get("/export/{filename}")
async def export_endpoint(
	filename: str
):
	try:
		export(filename)
	except Exception as e:
		return { "success": False, "error": repr(e) }
	return { "success": True }
