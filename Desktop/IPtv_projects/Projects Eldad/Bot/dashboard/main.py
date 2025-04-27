from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd
import logging
import traceback

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

app = FastAPI()
security = HTTPBasic()
templates = Environment(loader=FileSystemLoader("dashboard/templates"))

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    username = os.getenv("ADMIN_USERNAME", "admin")
    password = os.getenv("ADMIN_PASSWORD", "123")
    print(f"🔐 USER: {credentials.username}, PASS: {credentials.password}")
    if credentials.username != username or credentials.password != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials

# 🏠 דף סקירה כללית
@app.get("/", response_class=HTMLResponse)
async def overview(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    try:
        conn = sqlite3.connect('downloads.db')

        df_files1 = pd.read_sql(
            "SELECT file_name, username, uploader_id AS user_id, category, upload_time FROM files", conn
        )

        df_files2 = pd.read_sql(
            "SELECT file_name, username, downloader_id AS user_id, 'מהקבוצה' AS category, download_time AS upload_time FROM downloads_group",
            conn
        )

        df_downloads = pd.read_sql("SELECT * FROM downloads", conn)

        df_views = pd.read_sql_query('''
            SELECT file_name, username, event_time
            FROM group_file_events
            WHERE event_type = 'view'
            ORDER BY event_time DESC
            LIMIT 10
        ''', conn)

        # קבצי פלייליסטים
        df_playlists = df_files1[df_files1['category'] == 'פלייליסטים']
        # קבצי אפליקציות
        df_apps = df_files1[df_files1['category'] == 'אפליקציות']

        df_all_files = pd.concat([df_files1, df_files2], ignore_index=True)
        df_all_files["username"] = df_all_files["username"].fillna("unknown")
        df_all_files["user_id"] = df_all_files["user_id"].fillna("")

        total_files = len(df_all_files)
        total_downloads = len(df_downloads)
        top_uploaders = df_all_files['username'].value_counts().head(5).to_dict()

        recent_files = df_all_files.sort_values(by="upload_time", ascending=False).head(20).to_dict(orient="records")
        recent_views = df_views.to_dict(orient="records")
        recent_playlists = df_playlists.sort_values(by="upload_time", ascending=False).head(20).to_dict(orient="records")
        recent_apps = df_apps.sort_values(by="upload_time", ascending=False).head(20).to_dict(orient="records")
        recent_downloads = df_downloads.sort_values(by="download_time", ascending=False).head(20).to_dict(orient="records")

        conn.close()

        # דואג שגם אם אין נתונים, הדף לא יקרוס
        recent_files = recent_files or []
        recent_views = recent_views or []
        recent_playlists = recent_playlists or []
        recent_apps = recent_apps or []
        recent_downloads = recent_downloads or []

        template = templates.get_template('index.html')

        return template.render(
            total_files=total_files,
            total_downloads=total_downloads,
            top_uploaders=top_uploaders,
            recent_files=recent_files,
            recent_views=recent_views,
            recent_playlists=recent_playlists,
            recent_apps=recent_apps,
            recent_downloads=recent_downloads
        )

    except Exception as e:
        print(f"❌ ERROR: {e}")
        traceback.print_exc()
        return HTMLResponse(content="Internal Server Error", status_code=500)

# 🔹 נתיב נכון ל-/dashboard
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_redirect(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    return await overview(request, credentials)
