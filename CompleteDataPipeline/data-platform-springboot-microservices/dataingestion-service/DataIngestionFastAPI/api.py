
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from classifier import combine_results, read_text_from_bytes

app = FastAPI(title="PII & Source Classifier API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ClassifyRequest(BaseModel):
    text: str
    had_file: bool = False

@app.post("/classify")
def classify(req: ClassifyRequest):
    result = combine_results(req.text, had_file=req.had_file)
    return result

@app.post("/classify-file")
async def classify_file(file: UploadFile = File(...)):
    data = await file.read()
    text = read_text_from_bytes(file.filename, data)
    result = combine_results(text, had_file=True)
    return {"filename": file.filename, "result": result}

if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)
