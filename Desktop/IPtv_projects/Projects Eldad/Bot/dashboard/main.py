from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd

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
    conn = sqlite3.connect('downloads.db')

    # קבצים מהבוט
    df_files1 = pd.read_sql(
        "SELECT file_name, username, uploader_id AS user_id, category, upload_time FROM files", conn
    )

    # קבצים מהקבוצה
    df_files2 = pd.read_sql(
        "SELECT file_name, username, downloader_id AS user_id, 'מהקבוצה' AS category, download_time AS upload_time FROM downloads_group",
        conn
    )

    # טבלת הורדות
    df_downloads = pd.read_sql("SELECT * FROM downloads", conn)

    # 📄 קבצי פלייליסטים
    df_playlists = df_files1[df_files1['category'] == 'פלייליסטים']

    # 📱 קבצי אפליקציות
    df_apps = df_files1[df_files1['category'] == 'אפליקציות']

    # 📥 קבצים שהורדו לאחרונה
    df_recent_downloads = pd.read_sql_query('''
        SELECT file_name, username, download_time
        FROM downloads
        ORDER BY download_time DESC
        LIMIT 10
    ''', conn)

    recent_playlists = df_playlists.sort_values(by="upload_time", ascending=False).to_dict(orient="records")
    recent_apps = df_apps.sort_values(by="upload_time", ascending=False).to_dict(orient="records")
    recent_downloads = df_recent_downloads.to_dict(orient="records")

    # קבצים שצפו לאחרונה (מהטבלה group_file_events)
    df_views = pd.read_sql_query('''
        SELECT file_name, username, event_time
        FROM group_file_events
        WHERE event_type = 'view'
        ORDER BY event_time DESC
        LIMIT 10
    ''', conn)

    conn.close()

    df_all_files = pd.concat([df_files1, df_files2], ignore_index=True)
    df_all_files["username"] = df_all_files["username"].fillna("unknown")
    df_all_files["user_id"] = df_all_files["user_id"].fillna("")

    total_files = len(df_all_files)
    total_downloads = len(df_downloads)
    top_uploaders = df_all_files['username'].value_counts().head(5).to_dict()

    recent_files = df_all_files.sort_values(by="upload_time", ascending=False).head(20).to_dict(orient="records")
    recent_views = df_views.to_dict(orient="records")  # ← חדש

    template = templates.get_template('index.html')

    return template.render(
        total_files=total_files,
        total_downloads=total_downloads,
        top_uploaders=top_uploaders,
        recent_files=recent_files,
        recent_downloads=recent_downloads,
        recent_views=recent_views,
        recent_playlists=recent_playlists,
        recent_apps=recent_apps
    )


# 🏠 כתובת נוספת /dashboard שמחזירה גם את overview
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_redirect(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    return await overview(request, credentials)


# 📋 דף פעולות מהקבוצה
@app.get("/dashboard/section/events", response_class=HTMLResponse)
async def dashboard_events(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    conn = sqlite3.connect('downloads.db')
    df = pd.read_sql_query('''
        SELECT file_name, file_type, username, event_type, topic_name, event_time
        FROM group_file_events
        WHERE chat_id = -1002464592389
        ORDER BY event_time DESC
        LIMIT 100
    ''', conn)
    conn.close()

    template = templates.get_template("group_events.html")
    return template.render(events=df.to_dict(orient="records"))

# 📈 דף גרפים
@app.get("/dashboard/section/charts", response_class=HTMLResponse)
async def dashboard_charts(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    conn = sqlite3.connect('downloads.db')
    df = pd.read_sql_query(
        'SELECT username, event_type, event_time FROM group_file_events WHERE chat_id = -1002464592389',
        conn
    )
    conn.close()

    if df.empty:
        users_data = {}
        actions_data = {}
        daily_data = {}
    else:
        df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
        df = df.dropna(subset=['event_time'])
        users_data = df['username'].value_counts().to_dict()
        actions_data = df['event_type'].value_counts().to_dict()
        daily_data = df['event_time'].dt.date.value_counts().sort_index().to_dict()

    template = templates.get_template("charts.html")
    return template.render(
        users_data=users_data,
        actions_data=actions_data,
        daily_data=daily_data
    )

# 👤 דף משתמשים
@app.get("/dashboard/section/users", response_class=HTMLResponse)
async def dashboard_users(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    conn = sqlite3.connect('downloads.db')
    df = pd.read_sql_query('''
        SELECT DISTINCT user_id, username
        FROM group_file_events
        WHERE chat_id = -1002464592389
        ORDER BY username
    ''', conn)
    conn.close()

    template = templates.get_template("users.html")
    return template.render(users=df.to_dict(orient="records"))
