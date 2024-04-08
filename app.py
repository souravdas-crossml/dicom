"""

"""

import os
from fastapi import FastAPI, File, UploadFile

from utils.dicom_saver import DicomSaver

app = FastAPI()

dicom_saver = DicomSaver(
    s3_bucket=os.getenv("S3_BUCKET"),
    region_name=os.getenv("REGION_NAME"),
    access_key_id=os.getenv("ACCESS_KEY_ID"),
    secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
)


@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    # Read the contents of the file
    contents = await file.read()
    # Return file details
    
    dicom_saver.process_and_save(
        dicom_bytes=contents,
        filename=file.filename,
    )
    
    return {"status": 200}





