
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
import pyzipper

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

    c.execute('''CREATE TABLE IF NOT EXISTS file_interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            user_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            interaction_time TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS downloads_group (
            download_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            downloader_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            chat_id INTEGER,
            topic_name TEXT,
            download_time TEXT)''')

    DB_CONN.commit()


async def download_zip_by_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """יוצר ZIP מוצפן עם סיסמה לפי הקטגוריה שנבחרה (פלייליסטים או אפליקציות)."""
    query = update.callback_query
    await query.answer()

    category = "פלייליסטים" if query.data == "category_playlists" else "אפליקציות"
    user = query.from_user
    download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # קבצים בתיקייה שנבחרה
    file_paths = [
        os.path.join(root, file)
        for root, _, files in os.walk(f'uploads/{category}')
        for file in files
    ]

    if not file_paths:
        await query.message.edit_text("❌ אין קבצים זמינים בקטגוריה שנבחרה.")
        return

    zip_path = f"{category}.zip"
    temp_dir = tempfile.mkdtemp()
    temp_zip_path = os.path.join(temp_dir, zip_path)

    try:
        # יצירת קובץ ZIP מוצפן
        with pyzipper.AESZipFile(temp_zip_path, 'w',
                                 compression=pyzipper.ZIP_DEFLATED,
                                 encryption=pyzipper.WZ_AES) as zf:
            zf.setpassword(PASSWORD.encode('utf-8'))
            for file_path in file_paths:
                zf.write(file_path, os.path.basename(file_path))

        shutil.move(temp_zip_path, zip_path)
        shutil.rmtree(temp_dir)

        # שמירת לוג במסד
        c = DB_CONN.cursor()
        c.execute('''
            INSERT INTO downloads (file_name, downloader_id, username, first_name, last_name, download_time)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (zip_path, user.id, user.username or "N/A", user.first_name, user.last_name or "N/A", download_time))
        DB_CONN.commit()

        with open(zip_path, 'rb') as file:
            await query.message.reply_document(
                document=file,
                filename=zip_path,
                caption=f"📦 הקובץ שלך מוכן.\n🔐 סיסמה לפתיחה: `{PASSWORD}`",
                parse_mode="Markdown"
            )

    except Exception as e:
        await query.message.edit_text(f"שגיאה ביצירת קובץ: {str(e)}")

    finally:
        download_lock.release()



async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("📤 העלאת קובץ", callback_data='upload')],
                [InlineKeyboardButton("📥 הורדת קבצים", callback_data='download')],
                [InlineKeyboardButton("📊 הצגת דוחות", callback_data='reports')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)
    else:
        await update.callback_query.message.edit_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)

GROUP_ID = -1002464592389  # Replace with your group's actual
TOPIC_NAME = "פלייליסטים"   # Your actual topic name

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
    """Track file downloads specifically from a group under 'playlists' topic."""
    message = update.message

    if message.chat_id != GROUP_ID or (message.message_thread_id is None):
        return  # Ignore messages not from the specific group/topic

    topic = await context.bot.get_forum_topic(chat_id=message.chat_id, message_thread_id=message.message_thread_id)
    if topic.name.lower() != TOPIC_NAME.lower():
        return  # Only proceed if the topic matches exactly 'playlists'

    if message.document:
        file_name = message.document.file_name
        user = message.from_user
        download_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert download details into the database
        c = DB_CONN.cursor()
        c.execute('''
            INSERT INTO downloads_group (
                file_name, downloader_id, username, first_name, last_name, chat_id, topic_name, download_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (file_name, user.id, user.username or "N/A", user.first_name, user.last_name or "N/A",
              message.chat_id, topic.name, download_time))
        DB_CONN.commit()

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
    file_name = file.file_name
    upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    category = 'פלייליסטים' if file_name.endswith(('.m3u', '.m3u8')) else 'אפליקציות' if file_name.endswith('.apk') else 'Other'
    os.makedirs(f'uploads/{category}', exist_ok=True)
    file_path = f'uploads/{category}/{file_name}'
    new_file = await context.bot.get_file(file.file_id)
    await new_file.download_to_drive(file_path)
    c = DB_CONN.cursor()
    c.execute('''INSERT OR REPLACE INTO files (file_id, file_name, uploader_id, username, first_name, last_name, category, upload_time)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
              (file.file_id, file_name, user.id, user.username or "N/A", user.first_name, user.last_name or "N/A", category, upload_time))
    DB_CONN.commit()

    update_excel()  # ⬅️ Automatically updates Excel after each file upload
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
    """מציג רשימה מסודרת של הקבצים שהועלו (Excel עם פילטרים)."""
    user = update.callback_query.from_user

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    df = pd.read_sql_query('SELECT file_name AS "שם הקובץ", username AS "שם משתמש", uploader_id AS "מזהה משתמש", category AS "קטגוריה", upload_time AS "זמן העלאה" FROM files', conn)
    conn.close()

    if df.empty:
        await update.callback_query.message.edit_text("📂 אין קבצים זמינים.")
        return

    output_file = "uploaded_files.xlsx"
    df.to_excel(output_file, index=False)

    await update.callback_query.message.reply_document(
        document=open(output_file, 'rb'),
        caption="📂 רשימת קבצים שהועלו (Excel מפורט עם אפשרות פילטר וסינון)"
    )
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


async def show_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Group ID: {update.message.chat_id}")


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

async def main():
    create_database()

    app = Application.builder().token(TOKEN).build()

    # פקודות
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("uploaded_files", uploaded_files))
    app.add_handler(CommandHandler("download_logs", download_logs))
    app.add_handler(CommandHandler("generate_reports", generate_reports))
    app.add_handler(CommandHandler("stats_summary", stats_summary))

    # <-- ADD YOUR NEW HANDLER HERE!
    app.add_handler(CommandHandler("getid", show_group_id))

    # existing callback handlers and other handlers
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_member_check))

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
    app.add_handler(CallbackQueryHandler(download_zip_by_category, pattern='category_playlists'))
    app.add_handler(CallbackQueryHandler(download_zip_by_category, pattern='category_apps'))

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
async def playlist_download_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.callback_query.from_user

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    query = '''
        SELECT user_id, username, first_name, last_name, file_name, interaction_time
        FROM file_interactions
        WHERE file_name = ?
    '''
    df = pd.read_sql_query(query, conn, params=('EG(Israel)17.3.25.m3u',))
    conn.close()

    if df.empty:
        await update.callback_query.message.edit_text("אין הורדות של הקובץ המבוקש.")
        return

    report_file = "playlist_user_interactions.xlsx"
    df.to_excel(report_file, index=False)

    await update.callback_query.message.reply_document(
        document=open(report_file, 'rb'),
        caption="📥 דוח מפורט: מי הוריד את הקובץ EG(Israel)17.3.25.m3u"
    )




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
    """שולח למשתמש סיכום סטטיסטיקות כולל ומידע נוסף על הורדות."""
    user = update.callback_query.from_user

    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    files_data, downloads_data = load_data()

    total_uploads = len(files_data)
    total_downloads = len(downloads_data)

    # Check if there are any downloads to prevent errors
    if downloads_data.empty:
        await update.callback_query.message.edit_text("⚠️ אין מספיק נתונים להצגה.")
        return

    top_category = files_data['category'].value_counts().idxmax()

    # Most downloaded file
    most_downloaded_file = downloads_data['file_name'].value_counts().idxmax()
    most_downloaded_file_count = downloads_data['file_name'].value_counts().max()

    # User who downloaded the most files
    top_downloader_id = downloads_data['downloader_id'].value_counts().idxmax()
    top_downloader_downloads = downloads_data['downloader_id'].value_counts().max()
    top_downloader_info = downloads_data[downloads_data['downloader_id'] == top_downloader_id].iloc[0]

    top_downloader_username = top_downloader_info['username']
    top_downloader_firstname = top_downloader_info['first_name']
    top_downloader_lastname = top_downloader_info['last_name']

    summary = (
        f"📊 **סיכום סטטיסטיקות כולל**:\n"
        f"📁 **סך כל הקבצים שהועלו:** {total_uploads}\n"
        f"📥 **סך כל ההורדות:** {total_downloads}\n"
        f"📂 **הקטגוריה הפופולרית ביותר:** {top_category}\n\n"

        f"🔥 **הקובץ שהורד הכי הרבה:**\n"
        f"📄 `{most_downloaded_file}` ({most_downloaded_file_count} הורדות)\n\n"

        f"👤 **משתמש שהוריד הכי הרבה קבצים:**\n"
        f"🆔 `{top_downloader_id}`\n"
        f"💬 @{top_downloader_username}\n"
        f"🙍‍♂️ {top_downloader_firstname} {top_downloader_lastname}\n"
        f"📥 **מספר ההורדות:** {top_downloader_downloads}"
    )

    await update.callback_query.message.edit_text(summary, parse_mode='Markdown')
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
        [InlineKeyboardButton("⬅️ חזרה לתפריט הראשי", callback_data='start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text("📊 בחר דוח להצגה:", reply_markup=reply_markup)


async def download_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.callback_query.from_user

    # בדיקת הרשאות למנהל בלבד (שנה לפי הצורך)
    if user.id != 7773889743:
        await update.callback_query.answer("אין לך הרשאה לצפות במידע זה.", show_alert=True)
        return

    conn = sqlite3.connect('downloads.db')
    query = '''
        SELECT downloader_id AS "מזהה משתמש",
               username AS "שם משתמש",
               first_name AS "שם פרטי",
               last_name AS "שם משפחה",
               file_name AS "שם קובץ",
               download_time AS "זמן הורדה"
        FROM downloads
        ORDER BY download_time DESC
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        await update.callback_query.message.edit_text("📥 אין נתונים של הורדות זמינים.")
        return

    report_file = "all_users_downloads.xlsx"
    df.to_excel(report_file, index=False)

    await update.callback_query.message.reply_document(
        document=open(report_file, 'rb'),
        caption="📥 דוח מפורט: רשימת כל המשתמשים שהורידו קבצים"
    )



if __name__ == '__main__':
    import sys

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
