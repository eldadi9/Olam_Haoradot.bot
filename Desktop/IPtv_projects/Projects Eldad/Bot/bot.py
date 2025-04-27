# ✅ ייבוא ספריות
import os
import sqlite3
import asyncio
import platform
import logging
import random
import string
from datetime import datetime, timedelta
import tempfile
import shutil

from dotenv import load_dotenv
import pandas as pd
import pyzipper

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

# ✅ טעינת משתני סביבה
load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')

# ✅ הגדרות כלליות
ADMIN_ID = 7773889743
GROUP_ID = -1002464592389
TOPIC_NAME = "פלייליסטים"
PASSWORD = "olam_tov"

# ✅ התחברות למסד נתונים
DB_CONN = sqlite3.connect('downloads.db', check_same_thread=False)

# ✅ מנעול למניעת הורדות כפולות
download_lock = asyncio.Lock()

# ✅ הגדרות לוגים
logging.basicConfig(
    filename='errors.log',
    level=logging.ERROR,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)

# ✅ פונקציה ללוג שגיאות
def log_error(error, context=""):
    logging.error(f"{context}: {str(error)}")



def create_database():
    c = DB_CONN.cursor()

    # טבלת קבצים שהועלו
    c.execute('''CREATE TABLE IF NOT EXISTS files (
        file_id TEXT PRIMARY KEY,
        file_name TEXT,
        uploader_id INTEGER,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        category TEXT,
        upload_time TEXT
    )''')

    # טבלת הורדות מורחבת (מאוחדת)
    c.execute('''CREATE TABLE IF NOT EXISTS downloads (
        download_id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        downloader_id INTEGER,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        download_time TEXT,
        source TEXT,           -- bot / group
        chat_id INTEGER,       -- קבוצת מקור
        topic_name TEXT,       -- נושא
        device_type TEXT,      -- mobile, desktop, web
        platform TEXT,         -- Android, iOS, Windows וכו’
        version TEXT,          -- גרסת קובץ (אם רלוונטי)
        notes TEXT,            -- הערות חופשיות
        file_size INTEGER      -- גודל קובץ בבייטים
    )''')

    # טבלת אינטראקציות
    c.execute('''CREATE TABLE IF NOT EXISTS file_interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        user_id INTEGER,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        interaction_time TEXT
    )''')

    # לוג הורדות וצפיות מהקבוצה
    c.execute('''CREATE TABLE IF NOT EXISTS group_file_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        file_type TEXT,
        user_id INTEGER,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        event_type TEXT,  -- "download" או "view"
        chat_id INTEGER,
        topic_name TEXT,
        event_time TEXT
    )''')

def check_downloads_exist():
    """בודק כמה רשומות יש בטבלה downloads"""
    try:
        conn = sqlite3.connect('downloads.db')
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM downloads")
        count = c.fetchone()[0]
        conn.close()
        print(f"✅ כמות נתונים בטבלה downloads: {count}")
    except Exception as e:
        log_error(e, "check_downloads_exist")


def backup_and_merge_downloads_group():
    """מגבה את טבלת downloads_group ומאחד אותה ל־downloads"""
    try:
        conn = sqlite3.connect('downloads.db')
        c = conn.cursor()

        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='downloads_group'")
        if c.fetchone():
            print("📋 מבצע גיבוי של טבלת downloads_group...")

            # יצירת טבלת גיבוי אם צריך
            c.execute('''
                CREATE TABLE IF NOT EXISTS downloads_group_backup AS
                SELECT * FROM downloads_group
            ''')

            # הוספה ל־downloads
            c.execute('''
                INSERT INTO downloads (
                    file_name, downloader_id, username, first_name, last_name,
                    download_time, source, chat_id, topic_name
                )
                SELECT file_name, downloader_id, username, first_name, last_name,
                       download_time, 'group', chat_id, topic_name
                FROM downloads_group
            ''')

            # מחיקת הטבלה הישנה
            c.execute("DROP TABLE downloads_group")
            print("✅ טבלת downloads_group גובתה ונמחקה בהצלחה.")

            conn.commit()
        conn.close()

    except Exception as e:
        log_error(e, "backup_and_merge_downloads_group")

def generate_user_password(length=8):
    """יוצר סיסמה אקראית באורך נתון."""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


async def download_zip_by_category_secure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """יוצר ZIP מוצפן עם סיסמה ייחודית לפי הקטגוריה שנבחרה (פלייליסטים או אפליקציות)."""
    query = update.callback_query
    await query.answer()

    category = "פלייליסטים" if query.data == "category_playlists" else "אפליקציות"
    user = query.from_user
    download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # קבצים מהקטגוריה
    file_paths = [
        os.path.join(root, file)
        for root, _, files in os.walk(f'uploads/{category}')
        for file in files
    ]

    if not file_paths:
        await query.message.edit_text("❌ אין קבצים זמינים בקטגוריה שנבחרה.")
        return

    # יצירת סיסמה אישית
    user_password = generate_user_password()

    zip_path = f"{category}_{user.id}.zip"
    temp_dir = tempfile.mkdtemp()
    temp_zip_path = os.path.join(temp_dir, zip_path)

    try:
        # יצירת ZIP מוצפן
        with pyzipper.AESZipFile(temp_zip_path, 'w',
                                 compression=pyzipper.ZIP_DEFLATED,
                                 encryption=pyzipper.WZ_AES) as zf:
            zf.setpassword(user_password.encode('utf-8'))
            for file_path in file_paths:
                zf.write(file_path, os.path.basename(file_path))

        shutil.move(temp_zip_path, zip_path)
        shutil.rmtree(temp_dir)

        # לוג במסד
        c = DB_CONN.cursor()
        c.execute('''
            INSERT INTO downloads (file_name, downloader_id, username, first_name, last_name, download_time)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (zip_path, user.id, user.username or "N/A", user.first_name, user.last_name or "N/A", download_time))
        DB_CONN.commit()

        # שליחת הסיסמה בנפרד
        await query.message.reply_text(
            f"🔐 סיסמה לפתיחת הקובץ: `{user_password}`",
            parse_mode="Markdown"
        )

        # שליחת הקובץ
        with open(zip_path, 'rb') as file:
            await query.message.reply_document(
                document=file,
                filename=zip_path,
                caption="📦 הקובץ שלך מוכן. השתמש בסיסמה שנשלחה בהודעה נפרדת כדי לפתוח אותו."
            )

    except Exception as e:
        await query.message.edit_text(f"שגיאה ביצירת קובץ: {str(e)}")

    finally:
        download_lock.release()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📤 העלאת קובץ", callback_data='upload')],
        [InlineKeyboardButton("📥 הורדת קבצים", callback_data='download')],
        [InlineKeyboardButton("📊 הצגת דוחות", callback_data='reports')],
        [InlineKeyboardButton("📋 תפריט מתקדם", callback_data='advanced_menu')]  # ← חדש
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)
    else:
        await update.callback_query.message.edit_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)

# נשאר כמו שהיה:
GROUP_ID = -1002464592389
TOPIC_NAME = "פלייליסטים"
ADMIN_ID = 7773889743

async def advanced_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📂 צפייה בקבצים שהועלו", callback_data='uploaded_files')],
        [InlineKeyboardButton("📥 הורדות אחרונות", callback_data='download_logs')],
        [InlineKeyboardButton("📊 יצירת דוחות גרפיים", callback_data='generate_reports')],
        [InlineKeyboardButton("📈 סיכום סטטיסטיקות כולל", callback_data='stats_summary')],
        [InlineKeyboardButton("🧩 סטטיסטיקות בקבוצה", callback_data='group_stats')],
        [InlineKeyboardButton("🆔 הצגת מזהה קבוצה", callback_data='getid')],
        [InlineKeyboardButton("👥 רשימת משתמשים שהורידו", callback_data='download_users_list')],
        [InlineKeyboardButton("💻 סיכום לפי פלטפורמה ומכשיר", callback_data='platform_summary_report')],
        [InlineKeyboardButton("🗂️ דוח פעולות קבצים בקבוצה", callback_data='group_file_events_report')],
        [InlineKeyboardButton("🎵 דוח הורדות פלייליסט", callback_data='playlist_download_report')],
        [InlineKeyboardButton("📅 סינון פעולות לפי תאריכים", callback_data='group_file_events_filter')],
        [InlineKeyboardButton("⬅️ חזרה", callback_data='start')]  # כפתור חזרה
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text("📋 תפריט מתקדם - בחר פעולה:", reply_markup=reply_markup)



async def new_member_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bans users from joining the group if they don't have a username (@handle)."""
    if update.message.chat_id != GROUP_ID:
        return  # Ignore other groups

    for member in update.message.new_chat_members:
        if not member.username:
            await update.message.reply_text(
                f"❌ {member.first_name}, לא ניתן להצטרף לקבוצה ללא שם משתמש (@)."
            )
            await context.bot.ban_chat_member(update.message.chat_id, member.id)  # Ban user permanently


async def track_group_download(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """רושם הורדות מקבוצת טלגרם לטבלת downloads המאוחדת"""
    try:
        message = update.message

        if message.chat_id != GROUP_ID or (message.message_thread_id is None):
            return

        topic = await context.bot.get_forum_topic(chat_id=message.chat_id, message_thread_id=message.message_thread_id)
        if topic.name.lower() != TOPIC_NAME.lower():
            return

        if message.document:
            file_name = message.document.file_name
            file_size = message.document.file_size or 0  # 🆕
            user = message.from_user
            download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # ערכים משוערים (אין לנו מזה באופן מדויק)
            device_type = "mobile"  # ברירת מחדל
            platform = "Telegram"
            version = None
            notes = "group auto-download"

            c = DB_CONN.cursor()
            c.execute('''
                INSERT INTO downloads (
                    file_name, downloader_id, username, first_name, last_name,
                    download_time, source, chat_id, topic_name,
                    device_type, platform, version, notes, file_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_name, user.id, user.username or "N/A", user.first_name, user.last_name or "N/A",
                download_time, "group", message.chat_id, topic.name,
                device_type, platform, version, notes, file_size
            ))
            DB_CONN.commit()

    except Exception as e:
        log_error(e, "track_group_download")


async def send_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file_path = 'uploads/פלייליסטים/EG(Israel)17.3.25.m3u'
    if not os.path.exists(file_path):
        await update.callback_query.message.reply_text("הקובץ המבוקש לא נמצא.")
        return

    user = update.callback_query.from_user
    interaction_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    c = DB_CONN.cursor()
    c.execute('''
        INSERT INTO file_interactions (file_name, user_id, username, first_name, last_name, interaction_time)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (os.path.basename(file_path), user.id, user.username or "N/A", user.first_name, user.last_name or "N/A", interaction_time))
    DB_CONN.commit()

    with open(file_path, 'rb') as file:
        await update.callback_query.message.reply_document(
            document=file,
            caption=f'📥 הנה הקובץ שלך: {os.path.basename(file_path)}'
        )
    c.execute('''
        INSERT INTO downloads (file_name, downloader_id, username, first_name, last_name, download_time)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (os.path.basename(file_path), user.id, user.username or "N/A", user.first_name, user.last_name or "N/A",
          interaction_time))

    DB_CONN.commit()  # ⬅️ הוספנו את זה


async def upload_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.message.edit_text("🔼 שלח את הקובץ להעלאה.")


async def download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    keyboard = [
        [InlineKeyboardButton("🎵 פלייליסטים", callback_data='category_playlists')],
        [InlineKeyboardButton("📲 אפליקציות", callback_data='category_apps')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text("📥 בחר קטגוריה להורדה:", reply_markup=reply_markup)


async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    file = update.message.document

    if not file:
        return

    file_name = file.file_name
    upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    category = 'פלייליסטים' if file_name.endswith(('.m3u', '.m3u8')) else 'אפליקציות' if file_name.endswith('.apk') else 'אחר'

    os.makedirs(f'uploads/{category}', exist_ok=True)
    file_path = f'uploads/{category}/{file_name}'

    new_file = await context.bot.get_file(file.file_id)
    await new_file.download_to_drive(file_path)

    # שמירה למסד
    c = DB_CONN.cursor()
    c.execute('''
        INSERT OR REPLACE INTO files (
            file_id, file_name, uploader_id, username, first_name, last_name, category, upload_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        file.file_id, file_name, user.id, user.username or "N/A", user.first_name,
        user.last_name or "N/A", category, upload_time
    ))
    DB_CONN.commit()

    await update.message.reply_text("✅ הקובץ הועלה ונשמר בהצלחה.")


async def monitor_group_file_events(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or not message.document:
        return

    if message.chat_id != GROUP_ID:
        return

    file_name = message.document.file_name
    file_type = os.path.splitext(file_name)[-1].lower()
    user = message.from_user
    event_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ניסיון לזיהוי נושא השיחה בקבוצה (thread)
    topic_name = "לא זמין"
    if message.is_topic_message and message.message_thread_id:
        topic = await context.bot.get_forum_topic(
            chat_id=message.chat_id,
            message_thread_id=message.message_thread_id
        )
        topic_name = topic.name

    # הכנסה למסד הנתונים
    c = DB_CONN.cursor()
    c.execute('''
        INSERT INTO group_file_events (
            file_name, file_type, user_id, username, first_name, last_name,
            event_type, chat_id, topic_name, event_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        file_name,
        file_type,
        user.id,
        user.username or "N/A",
        user.first_name,
        user.last_name or "N/A",
        "download",  # או "view" אם תתמוך בזיהוי עתידי
        message.chat_id,
        topic_name,
        event_time
    ))
    DB_CONN.commit()



async def download_zip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    """יוצר ZIP מוגן בסיסמה ושולח למשתמש, כולל תיעוד מלא"""
    if not download_lock.acquire(blocking=False):
        await update.callback_query.answer("ההורדה כבר מתבצעת, נסה שוב בעוד רגע.")
        return

    try:
        zip_path = f'{category}.zip'
        user = update.callback_query.from_user
        download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        file_paths = [
            os.path.join(root, file)
            for root, _, files in os.walk(f'uploads/{category}')
            for file in files
        ]

        if not file_paths:
            await update.callback_query.answer()
            await update.callback_query.message.edit_text("אין קבצים בקטגוריה שנבחרה.")
            return

        temp_dir = tempfile.mkdtemp()
        temp_zip_path = os.path.join(temp_dir, f"{category}.zip")

        # יצירת קובץ ZIP עם סיסמה
        with pyzipper.AESZipFile(temp_zip_path, 'w', compression=ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zipf:
            zipf.setpassword(PASSWORD.encode('utf-8'))
            for file_path in file_paths:
                zipf.write(file_path, os.path.basename(file_path))

        shutil.move(temp_zip_path, zip_path)
        shutil.rmtree(temp_dir)

        # 🧮 מידע נוסף לתיעוד
        file_size = os.path.getsize(zip_path)
        device_type = "mobile"
        platform = "Telegram"
        version = None
        notes = f"{category} zip"

        # רישום למסד הנתונים
        c = DB_CONN.cursor()
        c.execute('''
            INSERT INTO downloads (
                file_name, downloader_id, username, first_name, last_name, download_time,
                source, chat_id, topic_name, device_type, platform, version, notes, file_size
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f"{category}.zip", user.id, user.username or "N/A", user.first_name, user.last_name or "N/A",
            download_time, "bot", None, None,  # no group or topic
            device_type, platform, version, notes, file_size
        ))
        DB_CONN.commit()

        await update.callback_query.answer()
        await update.callback_query.message.reply_document(
            document=open(zip_path, 'rb'),
            caption=f'📦 הורדה מוכנה. הסיסמה לפתיחה: {PASSWORD}',
            filename=f"{category}.zip"
        )

    except Exception as e:
        log_error(e, "download_zip_callback")

    finally:
        download_lock.release()


async def uploaded_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📂 שליחת רשימת קבצים שהועלו מהבוט ומהקבוצה."""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות ברשימה זו.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')

        # טבלת קבצים מהבוט
        df_files = pd.read_sql_query('''
            SELECT file_name, username, uploader_id AS user_id, category, upload_time
            FROM files
        ''', conn)

        # טבלת קבצים מהקבוצה
        df_group = pd.read_sql_query('''
            SELECT file_name, username, downloader_id AS user_id, 'מהקבוצה' AS category, download_time AS upload_time
            FROM downloads_group
        ''', conn)

        conn.close()

        # איחוד טבלאות
        df_all = pd.concat([df_files, df_group], ignore_index=True)

        # יצירת קובץ Excel
        output_file = "uploaded_files.xlsx"
        df_all.to_excel(output_file, index=False)

        await update.callback_query.message.reply_document(
            document=open(output_file, 'rb'),
            caption="📂 רשימת קבצים שהועלו (מהבוט ומהקבוצה)"
        )

    except Exception as e:
        print(f"שגיאה ב-uploaded_files: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה בשליחת רשימת קבצים.")


def print_downloads_columns():
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute("PRAGMA table_info(downloads)")
    rows = c.fetchall()
    conn.close()
    print("🧾 שדות בטבלת downloads:")
    for row in rows:
        print(f"- {row[1]}")

def insert_test_download():
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''
        INSERT INTO downloads (
            file_name, downloader_id, username, first_name, last_name,
            download_time, source, chat_id, topic_name,
            device_type, platform, version, notes, file_size
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        "test_file.zip", 123456, "tester", "Test", "User",
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "bot", None, None,
        "desktop", "Windows", "1.0", "debug", 12345
    ))
    conn.commit()
    conn.close()
    print("🧪 שורת בדיקה הוזנה לטבלה downloads.")

def update_excel():
    conn = sqlite3.connect('downloads.db')
    df = pd.read_sql_query(
        'SELECT file_name AS "שם הקובץ", username AS "שם משתמש", uploader_id AS "מזהה משתמש", category AS "קטגוריה", upload_time AS "זמן העלאה" FROM files',
        conn
    )
    conn.close()

    output_path = r"C:\Users\Master_PC\Desktop\IPtv_projects\Projects Eldad\Bot\Upload Playlits\uploaded_files.xlsx"
    df.to_excel(output_path, index=False)


async def download_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📥 שליחת לוג הורדות אחרונות."""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות בלוג הורדות.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')

        df = pd.read_sql_query('''
            SELECT
                downloader_id AS "מזהה משתמש",
                username AS "שם משתמש",
                first_name AS "שם פרטי",
                last_name AS "שם משפחה",
                file_name AS "שם קובץ",
                download_time AS "זמן הורדה",
                source AS "מקור",
                platform AS "פלטפורמה",
                device_type AS "סוג מכשיר",
                notes AS "הערות"
            FROM downloads
            ORDER BY download_time DESC
            LIMIT 100
        ''', conn)

        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📭 אין הורדות להצגה.")
            return

        output_file = "download_logs.xlsx"
        df.to_excel(output_file, index=False)

        await update.callback_query.message.reply_document(
            document=open(output_file, 'rb'),
            caption="📥 לוג 100 הורדות אחרונות (Excel)"
        )

    except Exception as e:
        print(f"שגיאה ב-download_logs: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה בשליחת לוג הורדות.")



def test_download_count():
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM downloads")
    print("סה\"כ הורדות:", c.fetchone()[0])
    conn.close()


async def show_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Group ID: {update.message.chat_id}")


async def generate_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📊 יצירת גרפים חכמים ושליחתם."""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה ליצירת דוחות.", show_alert=True)
            return

        await update.callback_query.message.edit_text("⏳ מייצר עבורך דוחות... המתן בבקשה...")

        conn = sqlite3.connect('downloads.db')
        df_downloads = pd.read_sql_query('SELECT * FROM downloads', conn)
        df_events = pd.read_sql_query('SELECT * FROM group_file_events', conn)
        conn.close()

        # 🎯 גרף 1: פעילות יומית (downloads)
        if not df_downloads.empty:
            df_downloads['download_time'] = pd.to_datetime(df_downloads['download_time'], errors='coerce')
            df_downloads['date'] = df_downloads['download_time'].dt.date
            daily_downloads = df_downloads['date'].value_counts().sort_index()

            plt.figure(figsize=(10,6))
            daily_downloads.plot(kind='line', marker='o')
            plt.title('📈 הורדות יומיות')
            plt.xlabel('תאריך')
            plt.ylabel('מספר הורדות')
            plt.grid()
            plt.tight_layout()
            plt.savefig('daily_downloads_report.png')
            plt.close()

            await update.callback_query.message.reply_document(
                document=open('daily_downloads_report.png', 'rb'),
                caption="📈 גרף הורדות יומיות"
            )

        # 🎯 גרף 2: טבלת פעולות מהקבוצה (views/downloads)
        if not df_events.empty:
            df_events['event_time'] = pd.to_datetime(df_events['event_time'], errors='coerce')
            df_events['date'] = df_events['event_time'].dt.date
            daily_events = df_events['date'].value_counts().sort_index()

            plt.figure(figsize=(10,6))
            daily_events.plot(kind='bar')
            plt.title('📊 פעולות בקבוצה לפי תאריך')
            plt.xlabel('תאריך')
            plt.ylabel('מספר פעולות')
            plt.tight_layout()
            plt.savefig('group_events_report.png')
            plt.close()

            await update.callback_query.message.reply_document(
                document=open('group_events_report.png', 'rb'),
                caption="📊 גרף פעולות קבוצה"
            )

        await update.callback_query.message.edit_text("✅ הדוחות נוצרו ונשלחו בהצלחה!")

    except Exception as e:
        print(f"שגיאה ב-generate_reports: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה ביצירת הדוחות.")


async def main():
    create_database()

    app = Application.builder().token(TOKEN).build()

    # פקודות
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("uploaded_files", uploaded_files))
    app.add_handler(CommandHandler("download_logs", download_logs))
    app.add_handler(CommandHandler("generate_reports", generate_reports))
    app.add_handler(CommandHandler("stats_summary", stats_summary))
    app.add_handler(CommandHandler("group_stats", group_stats))
    app.add_handler(CallbackQueryHandler(advanced_menu, pattern='advanced_menu'))

    # <-- ADD YOUR NEW HANDLER HERE!
    app.add_handler(CommandHandler("getid", show_group_id))

    # existing callback handlers and other handlers
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_member_check))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_all_documents))

    app.add_handler(CallbackQueryHandler(lambda u, c: group_file_events_filtered(u, c, 2), pattern='filter_days_2'))
    app.add_handler(CallbackQueryHandler(lambda u, c: group_file_events_filtered(u, c, 7), pattern='filter_days_7'))
    app.add_handler(CallbackQueryHandler(lambda u, c: group_file_events_filtered(u, c, 30), pattern='filter_days_30'))



    # CallbackQueryHandlers לתפריט הדוחות
    app.add_handler(CallbackQueryHandler(reports_menu, pattern='reports'))
    app.add_handler(CallbackQueryHandler(uploaded_files, pattern='uploaded_files'))
    app.add_handler(CallbackQueryHandler(download_logs, pattern='download_logs'))
    app.add_handler(CallbackQueryHandler(generate_reports, pattern='generate_reports'))
    app.add_handler(CallbackQueryHandler(stats_summary, pattern='stats_summary'))
    app.add_handler(CallbackQueryHandler(upload_callback, pattern='upload'))
    app.add_handler(CallbackQueryHandler(download_callback, pattern='download'))
    app.add_handler(CallbackQueryHandler(group_download_summary, pattern='group_download_summary'))
    app.add_handler(CallbackQueryHandler(send_playlist, pattern='download_playlist'))
    app.add_handler(CallbackQueryHandler(playlist_download_report, pattern='playlist_download_report'))
    app.add_handler(CallbackQueryHandler(download_users_list, pattern='download_users_list'))
    app.add_handler(CallbackQueryHandler(download_zip_by_category_secure, pattern='category_playlists'))
    app.add_handler(CallbackQueryHandler(download_zip_by_category_secure, pattern='category_apps'))
    app.add_handler(CallbackQueryHandler(group_file_events_report, pattern='group_file_events_report'))
    app.add_handler(CallbackQueryHandler(group_file_events_filter, pattern='group_file_events_filter'))
    app.add_handler(CallbackQueryHandler(lambda u, c: group_file_events_filtered(u, c, 2), pattern='filter_days_2'))
    app.add_handler(CallbackQueryHandler(lambda u, c: group_file_events_filtered(u, c, 7), pattern='filter_days_7'))
    app.add_handler(CallbackQueryHandler(lambda u, c: group_file_events_filtered(u, c, 30), pattern='filter_days_30'))
    app.add_handler(CallbackQueryHandler(platform_summary_report, pattern='platform_summary_report'))

    # חיבור לפונקציות שמייצרות גרפים
    app.add_handler(CallbackQueryHandler(plot_top_uploaders, pattern='plot_top_uploaders'))
    app.add_handler(CallbackQueryHandler(plot_download_activity, pattern='plot_download_activity'))

    # כפתור חזרה לתפריט הראשי
    app.add_handler(CallbackQueryHandler(start, pattern='start'))

    # מאזין להעלאת קבצים
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))

    if platform.system() == "Windows":
        asyncio.set_event_loop(asyncio.ProactorEventLoop())

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

    try:
        await asyncio.Future()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.shutdown()

async def handle_all_documents(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await monitor_group_file_events(update, context)
    await track_group_download(update, context)
    await file_handler(update, context)


def load_data():
    """טוען נתונים מהמסד."""
    conn = sqlite3.connect('downloads.db')
    query_files = "SELECT * FROM files"
    query_downloads = "SELECT * FROM downloads"
    files_data = pd.read_sql_query(query_files, conn)
    downloads_data = pd.read_sql_query(query_downloads, conn)
    conn.close()
    return files_data, downloads_data

async def plot_download_activity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """יוצר גרף של פעילות הורדות יומית ושולח אותו"""
    import pandas as pd  # תיקון
    import matplotlib.pyplot as plt

    _, downloads_data = load_data()
    if downloads_data.empty:
        await update.callback_query.message.edit_text("⚠️ אין מספיק נתונים להצגת גרף.")
        return

    downloads_data['download_time'] = pd.to_datetime(downloads_data['download_time'])
    downloads_data['date'] = downloads_data['download_time'].dt.date
    daily_downloads = downloads_data.groupby('date').size()

    plt.figure(figsize=(10, 6))
    daily_downloads.plot(kind='line', marker='o')
    plt.title("פעילות הורדות יומית")
    plt.xlabel("תאריך")
    plt.ylabel("מספר הורדות")
    plt.grid()
    plt.tight_layout()
    plt.savefig('daily_downloads.png')
    plt.close()

    await update.callback_query.message.reply_document(
        document=open('daily_downloads.png', 'rb'),
        caption="📈 גרף פעילות הורדות יומית"
    )
async def playlist_download_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎵 דוח הורדות קובץ פלייליסט ספציפי"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות בדוח זה.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('''
            SELECT 
                user_id AS "מזהה משתמש",
                username AS "שם משתמש",
                first_name AS "שם פרטי",
                last_name AS "שם משפחה",
                file_name AS "שם קובץ",
                interaction_time AS "זמן הורדה"
            FROM file_interactions
            WHERE file_name = ?
            ORDER BY interaction_time DESC
        ''', conn, params=('EG(Israel)17.3.25.m3u',))  # ← כאן שם הקובץ הקבוע (אפשר לשדרג בעתיד לבחירה דינמית)
        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📄 לא נמצאו הורדות לקובץ הפלייליסט שביקשת.")
            return

        file_path = "playlist_download_report.xlsx"
        df.to_excel(file_path, index=False)

        await update.callback_query.message.reply_document(
            document=open(file_path, 'rb'),
            caption="🎵 דוח הורדות פלייליסט (קובץ Excel מצורף)"
        )

    except Exception as e:
        print(f"שגיאה ב-playlist_download_report: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה ביצירת דוח הורדות פלייליסט.")


async def group_file_events_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🗂️ דוח פעולות קבצים בקבוצה"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות בדוח זה.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('''
            SELECT 
                file_name AS "שם קובץ",
                file_type AS "סוג קובץ",
                username AS "שם משתמש",
                event_type AS "סוג פעולה",
                topic_name AS "נושא",
                event_time AS "זמן פעולה"
            FROM group_file_events
            WHERE chat_id = ?
            ORDER BY event_time DESC
            LIMIT 100
        ''', conn, params=(GROUP_ID,))
        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📂 אין פעולות קבצים מהקבוצה להצגה.")
            return

        file_path = "group_file_events_report.xlsx"
        df.to_excel(file_path, index=False)

        await update.callback_query.message.reply_document(
            document=open(file_path, 'rb'),
            caption="🗂️ דוח פעולות קבצים מהקבוצה (קובץ Excel מצורף)"
        )

    except Exception as e:
        print(f"שגיאה ב-group_file_events_report: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה ביצירת דוח פעולות קבצים מהקבוצה.")



async def plot_top_uploaders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """יוצר גרף של המשתמשים שהעלו הכי הרבה קבצים ושולח אותו"""
    import pandas as pd  # תיקון: ייבוא pandas
    import matplotlib.pyplot as plt

    files_data, _ = load_data()
    if files_data.empty:
        await update.callback_query.message.edit_text("⚠️ אין מספיק נתונים להצגת גרף.")
        return

    top_uploaders = files_data['username'].value_counts().head(10)
    plt.figure(figsize=(10, 6))
    top_uploaders.plot(kind='bar')
    plt.title("משתמשים שהעלו הכי הרבה קבצים")
    plt.xlabel("שם משתמש")
    plt.ylabel("מספר קבצים שהועלו")
    plt.tight_layout()
    plt.savefig('top_uploaders.png')
    plt.close()

    await update.callback_query.message.reply_document(
        document=open('top_uploaders.png', 'rb'),
        caption="📊 גרף משתמשים שהעלו הכי הרבה קבצים"
    )


async def stats_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📈 שליחת סיכום סטטיסטיקות כולל"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות בסטטיסטיקות.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')
        df_files = pd.read_sql_query('SELECT * FROM files', conn)
        df_downloads = pd.read_sql_query('SELECT * FROM downloads', conn)
        conn.close()

        total_uploads = len(df_files)
        total_downloads = len(df_downloads)

        # 🏷️ הקטגוריה הכי פופולרית
        if 'category' in df_files.columns and not df_files.empty:
            top_category = df_files['category'].value_counts().idxmax()
        else:
            top_category = "אין מידע"

        # 📄 הקובץ הכי מורד
        if not df_downloads.empty:
            top_file = df_downloads['file_name'].value_counts().idxmax()
            top_file_count = df_downloads['file_name'].value_counts().max()
        else:
            top_file = "אין קבצים"
            top_file_count = 0

        # 👤 המשתמש שהוריד הכי הרבה
        if not df_downloads.empty:
            top_user_id = df_downloads['downloader_id'].value_counts().idxmax()
            top_user_info = df_downloads[df_downloads['downloader_id'] == top_user_id].iloc[0]
            top_user_username = top_user_info['username'] or "N/A"
            top_user_firstname = top_user_info['first_name'] or ""
            top_user_lastname = top_user_info['last_name'] or ""
            top_user_downloads = df_downloads['downloader_id'].value_counts().max()
        else:
            top_user_id = "-"
            top_user_username = "-"
            top_user_firstname = "-"
            top_user_lastname = "-"
            top_user_downloads = 0

        # 📊 הכנת סיכום יפה
        summary = (
            f"📈 **סיכום סטטיסטיקות כולל**\n\n"
            f"📂 סה\"כ קבצים שהועלו: {total_uploads}\n"
            f"📥 סה\"כ הורדות: {total_downloads}\n"
            f"🏷️ הקטגוריה הפופולרית ביותר: {top_category}\n\n"
            f"🔥 הקובץ הכי פופולרי:\n"
            f"`{top_file}` ({top_file_count} הורדות)\n\n"
            f"🏆 המשתמש שהוריד הכי הרבה:\n"
            f"👤 {top_user_firstname} {top_user_lastname} (@{top_user_username})\n"
            f"🆔 {top_user_id}\n"
            f"📥 {top_user_downloads} הורדות"
        )

        await update.callback_query.message.edit_text(summary, parse_mode="Markdown")

    except Exception as e:
        print(f"שגיאה ב-stats_summary: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה ביצירת סיכום סטטיסטיקות.")

async def group_download_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.callback_query.from_user

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    query = '''
        SELECT downloader_id, username, first_name, last_name, file_name, download_time
        FROM downloads_group WHERE chat_id = ? AND topic_name = ?
    '''
    df = pd.read_sql_query(query, conn, params=(GROUP_ID, TOPIC_NAME))
    conn.close()

    if df.empty:
        await update.callback_query.message.edit_text("📥 אין הורדות מהקבוצה בנושא הנבחר.")
        return

    summary_file = "group_topic_downloads.xlsx"
    df.to_excel(summary_file, index=False)

    await update.callback_query.message.reply_document(
        document=open(summary_file, 'rb'),
        caption=f"📥 דוח הורדות מפורט מקבוצתך בנושא '{TOPIC_NAME}'"
    )

async def group_download_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.callback_query.from_user

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    query = '''
        SELECT downloader_id AS "מזהה משתמש",
               username AS "שם משתמש",
               first_name AS "שם פרטי",
               last_name AS "שם משפחה",
               file_name AS "שם הקובץ",
               chat_id AS "מזהה קבוצה",
               topic_name AS "נושא",
               download_time AS "זמן ההורדה"
        FROM downloads_group
        ORDER BY download_time DESC
    '''

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        await update.callback_query.message.edit_text("📥 אין נתונים של הורדות מהקבוצה.")
        return

    report_file = "group_download_report.xlsx"
    df.to_excel(report_file, index=False)

    await update.callback_query.message.reply_document(
        document=open(report_file, 'rb'),
        caption="📥 דוח מפורט: מי הוריד, מה הוריד, מתי והיכן"
    )


async def group_file_events_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📅 תפריט סינון לפי טווחי תאריכים (2 ימים / 7 ימים / 30 ימים)"""
    try:
        await update.callback_query.answer()

        keyboard = [
            [InlineKeyboardButton("📆 יומיים אחרונים", callback_data='filter_days_2')],
            [InlineKeyboardButton("🗓️ 7 ימים אחרונים", callback_data='filter_days_7')],
            [InlineKeyboardButton("📅 חודש אחרון", callback_data='filter_days_30')],
            [InlineKeyboardButton("⬅️ חזרה לתפריט הדוחות", callback_data='reports')]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.message.edit_text("📅 בחר טווח תאריכים להצגת פעולות:", reply_markup=reply_markup)

    except Exception as e:
        print(f"שגיאה ב-group_file_events_filter: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה בפתיחת תפריט הסינון לפי תאריכים.")

async def group_file_events_filtered(update: Update, context: ContextTypes.DEFAULT_TYPE, days_back: int):
    """📅 מחלץ קבצים מהקבוצה לפי מספר ימים אחורה"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות במידע זה.", show_alert=True)
            return

        since = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('''
            SELECT file_name AS "שם קובץ", file_type AS "סוג קובץ", username AS "שם משתמש",
                   first_name AS "שם פרטי", last_name AS "שם משפחה",
                   event_type AS "סוג פעולה", topic_name AS "נושא", event_time AS "זמן"
            FROM group_file_events
            WHERE chat_id = ? AND event_time >= ?
            ORDER BY event_time DESC
        ''', conn, params=(GROUP_ID, since))
        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📭 אין נתונים בטווח שבחרת.")
            return

        file_path = f"group_file_events_last_{days_back}_days.xlsx"
        df.to_excel(file_path, index=False)

        with open(file_path, 'rb') as f:
            await update.callback_query.message.reply_document(
                document=f,
                caption=f"📊 דוח פעולות קבצים בקבוצה ({days_back} ימים אחרונים)"
            )

    except Exception as e:
        print(f"שגיאה ב-group_file_events_filtered: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה בשליפת הנתונים.")




async def download_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """👥 רשימת כל המשתמשים שהורידו קבצים"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות במידע זה.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('''
            SELECT
                downloader_id AS "מזהה משתמש",
                username AS "שם משתמש",
                first_name AS "שם פרטי",
                last_name AS "שם משפחה",
                file_name AS "שם קובץ",
                download_time AS "תאריך הורדה",
                platform AS "פלטפורמה",
                device_type AS "סוג מכשיר",
                version AS "גרסה",
                notes AS "הערות",
                file_size AS "גודל קובץ (bytes)",
                source AS "מקור הורדה",
                topic_name AS "נושא",
                chat_id AS "מזהה קבוצה"
            FROM downloads
            ORDER BY download_time DESC
        ''', conn)
        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📥 אין נתונים להצגה.")
            return

        output_file = "all_users_downloads.xlsx"
        df.to_excel(output_file, index=False)

        await update.callback_query.message.reply_document(
            document=open(output_file, 'rb'),
            caption="📥 דוח מלא: כל המשתמשים שהורידו קבצים"
        )

    except Exception as e:
        print(f"שגיאה ב-download_users_list: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה בעת יצירת דוח משתמשים.")


async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🧩 שליחת סיכום סטטיסטיקות מהקבוצה"""
    try:
        user = update.effective_user

        if user.id != ADMIN_ID:
            await update.message.reply_text("❌ אין לך הרשאה לצפות במידע זה.")
            return

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('''
            SELECT file_name, user_id, username, event_type, event_time
            FROM group_file_events
            WHERE chat_id = ?
        ''', conn, params=(GROUP_ID,))
        conn.close()

        if df.empty:
            await update.message.reply_text("📭 אין פעולות בקבוצה עד כה.")
            return

        total_actions = len(df)
        unique_users = df['user_id'].nunique()
        top_file = df['file_name'].value_counts().idxmax()
        top_user_id = df['user_id'].value_counts().idxmax()

        top_user_info = df[df['user_id'] == top_user_id].iloc[0]
        top_user_username = top_user_info['username'] or "N/A"

        summary = (
            f"🧩 **סטטיסטיקות קבוצה**\n\n"
            f"🔢 סה\"כ פעולות: {total_actions}\n"
            f"👥 מספר משתמשים שונים: {unique_users}\n"
            f"🔥 הקובץ הכי פופולרי: `{top_file}`\n"
            f"🏆 המשתמש הכי פעיל: @{top_user_username} ({top_user_id})"
        )

        await update.message.reply_text(summary, parse_mode="Markdown")

    except Exception as e:
        print(f"שגיאה ב-group_stats: {e}")
        await update.message.reply_text("❌ שגיאה ביצירת סיכום פעילות בקבוצה.")


async def getid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆔 שליחת מזהה הקבוצה או המשתמש"""
    try:
        if update.message:
            chat_id = update.message.chat_id
            user_id = update.message.from_user.id

            text = (
                f"🆔 **המזהים שלך:**\n\n"
                f"• מזהה משתמש: `{user_id}`\n"
                f"• מזהה קבוצה: `{chat_id}`"
            )
            await update.message.reply_text(text, parse_mode="Markdown")

        elif update.callback_query:
            chat_id = update.callback_query.message.chat_id
            user_id = update.callback_query.from_user.id

            text = (
                f"🆔 **המזהים שלך:**\n\n"
                f"• מזהה משתמש: `{user_id}`\n"
                f"• מזהה קבוצה: `{chat_id}`"
            )
            await update.callback_query.message.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        print(f"שגיאה ב-getid: {e}")
        if update.message:
            await update.message.reply_text("❌ שגיאה בשליפת מזהה.")
        elif update.callback_query:
            await update.callback_query.answer("❌ שגיאה בשליפת מזהה.", show_alert=True)


async def platform_summary_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """💻 סיכום לפי פלטפורמה ומכשיר"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות בדוח זה.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('SELECT * FROM downloads', conn)
        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📊 אין מספיק נתונים להצגה.")
            return

        summary = "**📊 סיכום לפי פלטפורמה / מכשיר:**\n\n"

        # פלטפורמות
        if 'platform' in df.columns:
            platform_counts = df['platform'].value_counts()
            summary += "💻 **מערכות הפעלה:**\n"
            for platform, count in platform_counts.items():
                summary += f"• {platform}: {count}\n"
            summary += "\n"

        # סוגי מכשירים
        if 'device_type' in df.columns:
            device_counts = df['device_type'].value_counts()
            summary += "📱 **סוגי מכשירים:**\n"
            for device, count in device_counts.items():
                summary += f"• {device}: {count}\n"
            summary += "\n"

        # הערות נפוצות
        if 'notes' in df.columns:
            notes_counts = df['notes'].value_counts()
            summary += "🏷️ **הערות נפוצות:**\n"
            for note, count in notes_counts.items():
                summary += f"• {note}: {count}\n"
            summary += "\n"

        # מקור הורדה
        if 'source' in df.columns:
            source_counts = df['source'].value_counts()
            summary += "🔄 **מקור הורדה:**\n"
            for source, count in source_counts.items():
                summary += f"• {source}: {count}\n"
            summary += "\n"

        # גודל ממוצע קבצים
        if 'file_size' in df.columns:
            avg_size = df['file_size'].mean()
            summary += f"📦 **גודל ממוצע קובץ:** {int(avg_size):,} bytes\n"

        await update.callback_query.message.edit_text(summary, parse_mode="Markdown")

    except Exception as e:
        print(f"שגיאה ב-platform_summary_report: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה ביצירת סיכום פלטפורמות.")




async def reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    keyboard = [
        [InlineKeyboardButton("📁 קבצים שהועלו", callback_data='uploaded_files')],
        [InlineKeyboardButton("📥 לוג הורדות", callback_data='download_logs')],
        [InlineKeyboardButton("📊 משתמשים שהעלו הכי הרבה קבצים", callback_data='plot_top_uploaders')],
        [InlineKeyboardButton("📈 פעילות הורדות יומית", callback_data='plot_download_activity')],
        [InlineKeyboardButton("📑 יצירת דוחות מלאים", callback_data='generate_reports')],
        [InlineKeyboardButton("📊 סיכום סטטיסטיקות", callback_data='stats_summary')],
        [InlineKeyboardButton("👤 רשימת משתמשים שהורידו קבצים", callback_data='download_users_list')],
        [InlineKeyboardButton("📊 דוח קבצים מהקבוצה", callback_data='group_file_events_report')],
        [InlineKeyboardButton("📅 דוח לפי תאריכים", callback_data='group_file_events_filter')],
        [InlineKeyboardButton("📊 סיכום לפי פלטפורמה / מכשיר", callback_data='platform_summary_report')],
        [InlineKeyboardButton("⬅️ חזרה לתפריט הראשי", callback_data='start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text("📊 בחר דוח להצגה:", reply_markup=reply_markup)


async def download_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """👥 יצירת דוח רשימת משתמשים שהורידו קבצים"""
    try:
        user = update.callback_query.from_user

        if user.id != ADMIN_ID:
            await update.callback_query.answer("❌ אין לך הרשאה לצפות בדוח זה.", show_alert=True)
            return

        conn = sqlite3.connect('downloads.db')
        df = pd.read_sql_query('''
            SELECT
                downloader_id AS "מזהה משתמש",
                username AS "שם משתמש",
                first_name AS "שם פרטי",
                last_name AS "שם משפחה",
                file_name AS "שם קובץ",
                download_time AS "זמן הורדה",
                platform AS "מערכת הפעלה",
                device_type AS "סוג מכשיר",
                version AS "גרסה",
                notes AS "הערות",
                file_size AS "גודל (bytes)",
                source AS "מקור הורדה",
                topic_name AS "נושא",
                chat_id AS "מזהה קבוצה"
            FROM downloads
            ORDER BY download_time DESC
        ''', conn)
        conn.close()

        if df.empty:
            await update.callback_query.message.edit_text("📭 אין נתונים זמינים ליצירת דוח.")
            return

        output_file = "all_users_downloads.xlsx"
        df.to_excel(output_file, index=False)

        await update.callback_query.message.reply_document(
            document=open(output_file, 'rb'),
            caption="👥 דוח מפורט: כל המשתמשים שהורידו קבצים"
        )

    except Exception as e:
        print(f"שגיאה ב-download_users_list: {e}")
        await update.callback_query.message.edit_text("❌ שגיאה ביצירת דוח משתמשים.")



if __name__ == '__main__':
    import sys

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())