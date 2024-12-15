import os
import sqlite3
import zipfile
import asyncio
import platform
from datetime import datetime
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from zipfile import ZipFile, ZIP_DEFLATED
from threading import Lock  # מנעול למניעת כפילות בהורדה
import shutil
import tempfile

TOKEN = '7908068063:AAG_5AmVEn0RFKD6ghIy_gzBS8e4r1uF_AI'
PASSWORD = 'olam_tov'  # סיסמת ZIP

# מנעול למניעת הורדות כפולות בו-זמנית
download_lock = Lock()

def create_database():
    """יוצר את בסיס הנתונים והטבלאות הדרושות אם הן לא קיימות."""
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()

    # טבלת קבצים שהועלו
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            file_name TEXT,
            uploader_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            category TEXT,
            upload_time TEXT
        )
    ''')

    # טבלת לוג הורדות
    c.execute('''
        CREATE TABLE IF NOT EXISTS downloads (
            download_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            downloader_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            download_time TEXT
        )
    ''')

    conn.commit()
    conn.close()

def create_secure_zip(file_paths, output_zip_path, password):
    """יוצר קובץ ZIP מוגן בסיסמה."""
    try:
        with ZipFile(output_zip_path, 'w', ZIP_DEFLATED) as zipf:
            zipf.setpassword(password.encode('utf-8'))
            for file_path in file_paths:
                zipf.write(file_path, os.path.basename(file_path))
        print(f"קובץ ZIP נוצר בהצלחה: {output_zip_path}")
    except Exception as e:
        print(f"שגיאה ביצירת קובץ ה-ZIP: {str(e)}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """תפריט ראשי."""
    print(update.message.from_user.id)  # הדפסת מזהה המשתמש למסוף
    keyboard = [
        [InlineKeyboardButton("📤 העלאת קובץ", callback_data='upload')],
        [InlineKeyboardButton("📥 הורדת קבצים", callback_data='download')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)

async def upload_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """מבקש מהמשתמש לשלוח קובץ להעלאה."""
    await update.callback_query.answer()
    await update.callback_query.message.reply_text("אנא שלח את הקובץ להעלאה.")

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """מטפל בהעלאת קובץ ושומר את הנתונים בבסיס הנתונים."""
    user = update.message.from_user
    file = update.message.document
    file_name = file.file_name
    upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    category = 'פלייליסטים' if file_name.endswith(('.m3u', '.m3u8')) else 'אפליקציות' if file_name.endswith('.apk') else 'אחר'
    os.makedirs(f'uploads/{category}', exist_ok=True)
    file_path = f'uploads/{category}/{file_name}'
    new_file = await context.bot.get_file(file.file_id)
    await new_file.download_to_drive(file_path)

    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO files (file_id, file_name, uploader_id, username, first_name, last_name, category, upload_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (file.file_id, file_name, user.id, user.username or "לא זמין", user.first_name, user.last_name or "לא זמין", category, upload_time))
    conn.commit()
    conn.close()

    await update.message.reply_text("תודה רבה! הקובץ הועלה בהצלחה.")

async def download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """תפריט הורדות עם קטגוריות."""
    await update.callback_query.answer()
    keyboard = [
        [InlineKeyboardButton("🎵 פלייליסטים", callback_data='category_playlists')],
        [InlineKeyboardButton("📲 אפליקציות", callback_data='category_apps')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.reply_text("בחר קטגוריה להורדה:", reply_markup=reply_markup)

async def download_zip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    """יוצר ZIP מוגן בסיסמה ושולח למשתמש."""
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
            await update.callback_query.message.reply_text("אין קבצים בקטגוריה שנבחרה.")
            return

        temp_dir = tempfile.mkdtemp()
        temp_zip_path = os.path.join(temp_dir, f"{category}.zip")

        with ZipFile(temp_zip_path, 'w', ZIP_DEFLATED) as zipf:
            zipf.setpassword(PASSWORD.encode('utf-8'))  # סיסמה להגנה לפתיחת הקובץ
            for file_path in file_paths:
                zipf.write(file_path, os.path.basename(file_path))


        shutil.move(temp_zip_path, zip_path)
        shutil.rmtree(temp_dir)

        conn = sqlite3.connect('downloads.db')
        c = conn.cursor()
        c.execute('''
            INSERT INTO downloads (file_name, downloader_id, username, first_name, last_name, download_time)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (f"{category}.zip", user.id, user.username or "לא זמין", user.first_name, user.last_name or "לא זמין", download_time))
        conn.commit()
        conn.close()

        await update.callback_query.answer()
        await update.callback_query.message.reply_document(
            document=open(zip_path, 'rb'),
            caption=f'להורדת הקובץ השתמש בסיסמה: {PASSWORD}',
            filename=f"{category}.zip"
        )

    finally:
        download_lock.release()

async def uploaded_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """מציג רשימה מסודרת של הקבצים שהועלו."""
    if update.message.from_user.id != 504019926:
        await update.message.reply_text("אין לך הרשאה לצפות במידע זה.")
        return

    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('SELECT file_name, username, uploader_id, category, upload_time FROM files')
    files = c.fetchall()
    conn.close()

    if not files:
        await update.message.reply_text("לא נמצאו קבצים.")
        return

    response = "📂 **רשימת קבצים שהועלו**\n"
    response += "---------------------------------\n"
    response += "{:<20} {:<10} {:<10} {:<10} {:<20}\n".format(
        "שם הקובץ", "משתמש", "ID", "קטגוריה", "תאריך"
    )
    response += "---------------------------------\n"

    for file in files:
        response += "{:<20} {:<10} {:<10} {:<10} {:<20}\n".format(
            file[0][:20],  # שם הקובץ
            file[1] or "לא זמין",  # שם המשתמש
            file[2],  # ID
            file[3],  # קטגוריה
            file[4]  # תאריך
        )

    await update.message.reply_text(f"```{response}```", parse_mode="Markdown")

async def download_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """מציג לוג הורדות מסודר."""
    if update.message.from_user.id != 504019926:
        await update.message.reply_text("אין לך הרשאה לצפות במידע זה.")
        return

    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('SELECT file_name, username, downloader_id, download_time FROM downloads')
    downloads = c.fetchall()
    conn.close()

    if not downloads:
        await update.message.reply_text("לא נמצאו הורדות.")
        return

    response = "📥 **לוג הורדות:**\n\n"
    for log in downloads:
        response += (
            f"📄 שם הקובץ: {log[0]}\n"
            f"👤 משתמש: {log[1] or 'לא זמין'} (ID: {log[2]})\n"
            f"📅 תאריך: {log[3]}\n\n"
        )

    await update.message.reply_text(response)

async def main():
    create_database()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("uploaded_files", uploaded_files))
    app.add_handler(CommandHandler("download_logs", download_logs))
    app.add_handler(CallbackQueryHandler(upload_callback, pattern='upload'))
    app.add_handler(CallbackQueryHandler(download_callback, pattern='download'))
    app.add_handler(CallbackQueryHandler(lambda u, c: download_zip_callback(u, c, 'פלייליסטים'), pattern='category_playlists'))
    app.add_handler(CallbackQueryHandler(lambda u, c: download_zip_callback(u, c, 'אפליקציות'), pattern='category_apps'))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))

    if platform.system() == "Windows":
        asyncio.set_event_loop(asyncio.ProactorEventLoop())

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    try:
        await asyncio.Future()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.shutdown()

# קוד קיים נשאר כפי שהוא

# ייבוא ספריות נוספות
import matplotlib.pyplot as plt
import pandas as pd

# פונקציות חדשות
def load_data():
    """טוען נתונים מהמסד."""
    conn = sqlite3.connect('downloads.db')
    query_files = "SELECT * FROM files"
    query_downloads = "SELECT * FROM downloads"
    files_data = pd.read_sql_query(query_files, conn)
    downloads_data = pd.read_sql_query(query_downloads, conn)
    conn.close()
    return files_data, downloads_data

def plot_top_uploaders(files_data):
    """גרף של משתמשים שהעלו הכי הרבה קבצים."""
    top_uploaders = files_data['username'].value_counts().head(10)
    plt.figure(figsize=(10, 6))
    top_uploaders.plot(kind='bar')
    plt.title("משתמשים שהעלו הכי הרבה קבצים")
    plt.xlabel("שם משתמש")
    plt.ylabel("מספר קבצים שהועלו")
    plt.tight_layout()
    plt.savefig('top_uploaders.png')
    plt.close()

def plot_download_activity(downloads_data):
    """גרף פעילות הורדות לפי תאריכים."""
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

async def generate_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """יוצר דוחות ויזואליים ושולח למשתמש."""
    if update.message.from_user.id != 504019926:
        await update.message.reply_text("אין לך הרשאה לצפות במידע זה.")
        return

    files_data, downloads_data = load_data()

    # יצירת גרפים
    plot_top_uploaders(files_data)
    plot_download_activity(downloads_data)

    # שליחת קבצי הגרפים למשתמש
    await update.message.reply_document(
        document=open('top_uploaders.png', 'rb'),
        caption="גרף משתמשים שהעלו הכי הרבה קבצים"
    )
    await update.message.reply_document(
        document=open('daily_downloads.png', 'rb'),
        caption="גרף פעילות הורדות יומית"
    )

async def stats_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """שולח למשתמש סיכום סטטיסטיקות כולל."""
    if update.message.from_user.id != 504019926:
        await update.message.reply_text("אין לך הרשאה לצפות במידע זה.")
        return

    files_data, downloads_data = load_data()

    total_uploads = len(files_data)
    total_downloads = len(downloads_data)
    top_category = files_data['category'].value_counts().idxmax()

    summary = (
        f"📊 **סיכום סטטיסטיקות**:\n"
        f"📁 סך כל הקבצים שהועלו: {total_uploads}\n"
        f"📥 סך כל ההורדות: {total_downloads}\n"
        f"📂 הקטגוריה הפופולרית ביותר: {top_category}"
    )
    await update.message.reply_text(summary, parse_mode='Markdown')

# הוספת הפונקציות החדשות להנדלרים של Telegram
async def main():
    create_database()

    app = Application.builder().token(TOKEN).build()

    # הקוד הקיים
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("uploaded_files", uploaded_files))
    app.add_handler(CommandHandler("download_logs", download_logs))
    app.add_handler(CallbackQueryHandler(upload_callback, pattern='upload'))
    app.add_handler(CallbackQueryHandler(download_callback, pattern='download'))
    app.add_handler(CallbackQueryHandler(lambda u, c: download_zip_callback(u, c, 'פלייליסטים'), pattern='category_playlists'))
    app.add_handler(CallbackQueryHandler(lambda u, c: download_zip_callback(u, c, 'אפליקציות'), pattern='category_apps'))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))

    # הוספת הפונקציות החדשות
    app.add_handler(CommandHandler("generate_reports", generate_reports))
    app.add_handler(CommandHandler("stats_summary", stats_summary))

    if platform.system() == "Windows":
        asyncio.set_event_loop(asyncio.ProactorEventLoop())

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    try:
        await asyncio.Future()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.shutdown()

if __name__ == '__main__':
    asyncio.run(main())