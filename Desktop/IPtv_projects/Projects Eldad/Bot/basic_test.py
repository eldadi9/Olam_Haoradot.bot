from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()
security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    username = os.getenv("ADMIN_USERNAME")
    password = os.getenv("ADMIN_PASSWORD")
    print("🔑 Trying login", credentials.username, credentials.password)
    if credentials.username != username or credentials.password != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    return f"✅ Welcome {credentials.username}!"
