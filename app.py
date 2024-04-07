"""

"""

from fastapi import FastAPI, File, UploadFile

app = FastAPI()

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    # Read the contents of the file
    contents = await file.read()
    # Return file details
    return {"filename": file.filename, "content_type": file.content_type, "size": len(contents)}





