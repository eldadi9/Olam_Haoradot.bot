
import os
import sqlite3
import zipfile
import asyncio
import platform
from datetime import datetime
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from zipfile import ZipFile, ZIP_DEFLATED
from threading import Lock
import shutil
import tempfile
import pandas as pd

# Global database connection
DB_CONN = sqlite3.connect('downloads.db', check_same_thread=False)

TOKEN = '7757317671:AAHlq8yWLzP4mrgEovVoVZb_2j9ilWt0OlQ'
PASSWORD = 'olam_tov'  # סיסמת ZIP

# מנעול למניעת הורדות כפולות בו-זמנית
download_lock = Lock()

# Global database connection
DB_CONN = sqlite3.connect('downloads.db', check_same_thread=False)

def create_database():
    c = DB_CONN.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            file_name TEXT,
            uploader_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            category TEXT,
            upload_time TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS downloads (
            download_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            downloader_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            download_time TEXT)''')
    DB_CONN.commit()


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
    keyboard = [[InlineKeyboardButton("📤 העלאת קובץ", callback_data='upload')],
                [InlineKeyboardButton("📥 הורדת קבצים", callback_data='download')],
                [InlineKeyboardButton("📊 הצגת דוחות", callback_data='reports')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)
    else:
        await update.callback_query.message.edit_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)

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

async def download_zip_playlists(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await download_zip_callback(update, context, "פלייליסטים")

async def download_zip_apps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await download_zip_callback(update, context, "אפליקציות")


async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    file = update.message.document
    file_name = file.file_name
    upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    category = 'Playlists' if file_name.endswith(('.m3u', '.m3u8')) else 'Apps' if file_name.endswith('.apk') else 'Other'
    os.makedirs(f'uploads/{category}', exist_ok=True)
    file_path = f'uploads/{category}/{file_name}'
    new_file = await context.bot.get_file(file.file_id)
    await new_file.download_to_drive(file_path)
    c = DB_CONN.cursor()
    c.execute('''INSERT OR REPLACE INTO files (file_id, file_name, uploader_id, username, first_name, last_name, category, upload_time)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
              (file.file_id, file_name, user.id, user.username or "N/A", user.first_name, user.last_name or "N/A", category, upload_time))
    DB_CONN.commit()
    await update.message.reply_text("✅ הקובץ הועלה בהצלחה.")



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
            await update.callback_query.message.edit_text("אין קבצים בקטגוריה שנבחרה.")
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
    user = update.callback_query.from_user  # תיקון: שימוש נכון ב- CallbackQuery

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('SELECT file_name, username, uploader_id, category, upload_time FROM files')
    files = c.fetchall()
    conn.close()

    if not files:
        await update.callback_query.message.edit_text("📂 אין קבצים זמינים.")

        return

    response = "**📂 רשימת קבצים שהועלו:**\n"
    for file in files:
        response += f"📄 {file[0]} | 👤 {file[1]} | 🆔 {file[2]} | 📂 {file[3]} | 📅 {file[4]}\n"

    await update.callback_query.message.edit_text(response[:4000], parse_mode="Markdown")  # מגבלת טלגרם

async def download_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """מציג לוג הורדות מסודר."""
    user = update.callback_query.from_user  # תיקון

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('SELECT file_name, username, downloader_id, download_time FROM downloads')
    downloads = c.fetchall()
    conn.close()

    if not downloads:
        await update.callback_query.message.edit_text("📥 אין הורדות זמינות.")
        return

    response = "**📥 לוג הורדות:**\n"
    for log in downloads:
        response += f"📄 {log[0]} | 👤 {log[1]} | 🆔 {log[2]} | 📅 {log[3]}\n"

    await update.callback_query.message.edit_text(response[:4000], parse_mode="Markdown")


async def main():
    create_database()

    app = Application.builder().token(TOKEN).build()

    # פקודות
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("uploaded_files", uploaded_files))
    app.add_handler(CommandHandler("download_logs", download_logs))
    app.add_handler(CommandHandler("generate_reports", generate_reports))
    app.add_handler(CommandHandler("stats_summary", stats_summary))



    # CallbackQueryHandlers לתפריט הדוחות
    app.add_handler(CallbackQueryHandler(reports_menu, pattern='reports'))
    app.add_handler(CallbackQueryHandler(uploaded_files, pattern='uploaded_files'))
    app.add_handler(CallbackQueryHandler(download_logs, pattern='download_logs'))
    app.add_handler(CallbackQueryHandler(generate_reports, pattern='generate_reports'))
    app.add_handler(CallbackQueryHandler(stats_summary, pattern='stats_summary'))
    app.add_handler(CallbackQueryHandler(upload_callback, pattern='upload'))
    app.add_handler(CallbackQueryHandler(download_callback, pattern='download'))
    app.add_handler(CallbackQueryHandler(download_zip_playlists, pattern='category_playlists'))
    app.add_handler(CallbackQueryHandler(download_zip_apps, pattern='category_apps'))

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

async def generate_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """יוצר דוחות ויזואליים ושולח למשתמש."""
    user = update.callback_query.from_user  # תיקון

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    # ✨ הוספת הודעה לפני יצירת הדוחות
    await update.callback_query.message.edit_text("🔎 יצירת דוחות... נא להמתין.")

    await plot_top_uploaders(update, context)
    await plot_download_activity(update, context)

    # ✨ לאחר השלמת הדוחות, עדכון המשתמש
    await update.callback_query.message.edit_text("✅ דוחות נוצרו ונשלחו בהצלחה!")


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
    """שולח למשתמש סיכום סטטיסטיקות כולל."""
    user = update.callback_query.from_user  # תיקון

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
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
    await update.callback_query.message.edit_text(summary, parse_mode='Markdown')

async def reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    keyboard = [
        [InlineKeyboardButton("📁 קבצים שהועלו", callback_data='uploaded_files')],
        [InlineKeyboardButton("📥 לוג הורדות", callback_data='download_logs')],
        [InlineKeyboardButton("📊 משתמשים שהעלו הכי הרבה קבצים", callback_data='plot_top_uploaders')],
        [InlineKeyboardButton("📈 פעילות הורדות יומית", callback_data='plot_download_activity')],
        [InlineKeyboardButton("📑 יצירת דוחות מלאים", callback_data='generate_reports')],
        [InlineKeyboardButton("📊 סיכום סטטיסטיקות", callback_data='stats_summary')],
        [InlineKeyboardButton("⬅️ חזרה לתפריט הראשי", callback_data='start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text("📊 בחר דוח להצגה:", reply_markup=reply_markup)


if __name__ == '__main__':
    import sys

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
