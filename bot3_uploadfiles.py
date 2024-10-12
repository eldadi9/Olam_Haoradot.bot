import os
import sqlite3
import zipfile
import asyncio
import platform
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

TOKEN = 'הכנס כאן את הטוקן שקיבלת'
PASSWORD = 'olam_tov'  # סיסמת ה-ZIP

def create_database():
    """יוצר את בסיס הנתונים אם הוא לא קיים."""
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            file_name TEXT,
            uploader_name TEXT,
            category TEXT
        )
    ''')
    conn.commit()
    conn.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """תפריט ראשי."""
    keyboard = [
        [InlineKeyboardButton("📤 העלאת קובץ", callback_data='upload')],
        [InlineKeyboardButton("📥 הורדת קבצים", callback_data='download')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("ברוכים הבאים! מה תרצה לעשות?", reply_markup=reply_markup)

async def upload_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """מבקש מהמשתמש לשלוח קובץ להעלאה."""
    await update.callback_query.answer()
    await update.callback_query.message.reply_text("אנא שלח את הקובץ להעלאה למערכת:")

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """טיפול בהעלאת קובץ ושמירתו בדיסק ובבסיס הנתונים."""
    user = update.message.from_user
    file = update.message.document
    file_id = file.file_id
    file_name = file.file_name

    # זיהוי קטגוריה על פי סיומת הקובץ
    if file_name.endswith(('.m3u', '.m3u8')):
        category = 'פלייליסטים'
    elif file_name.endswith('.apk'):
        category = 'אפליקציות'
    else:
        category = 'אחר'

    # שמירת הקובץ בדיסק
    file_path = f"./uploads/{category}/{file_name}"
    os.makedirs(f"./uploads/{category}", exist_ok=True)
    new_file = await context.bot.get_file(file_id)
    await new_file.download_to_drive(file_path)

    # שמירת פרטים בבסיס הנתונים
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO files (file_id, file_name, uploader_name, category)
        VALUES (?, ?, ?, ?)
    ''', (file_id, file_name, user.username or user.first_name, category))
    conn.commit()
    conn.close()

    await update.message.reply_text(f'הקובץ "{file_name}" נשמר בקטגוריה "{category}".')

async def download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """תפריט הורדות עם קטגוריות."""
    await update.callback_query.answer()
    keyboard = [
        [InlineKeyboardButton("🎵 פלייליסטים חינם", callback_data='free_playlists')],
        [InlineKeyboardButton("📲 אפליקציות (APK)", callback_data='apps')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.reply_text("בחר קטגוריה:", reply_markup=reply_markup)

async def free_playlists_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """הצגת רשימת קבצים מתוך 'פלייליסטים חינם'."""
    await send_file_list(update, 'פלייליסטים')

async def apps_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """הצגת רשימת קבצים מתוך 'אפליקציות'."""
    await send_file_list(update, 'אפליקציות')

async def send_file_list(update, category):
    """הצגת רשימת קבצים לפי קטגוריה."""
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('SELECT file_name FROM files WHERE category = ?', (category,))
    files = c.fetchall()
    conn.close()

    if files:
        response = f"קבצים זמינים בקטגוריית '{category}':\n"
        for file in files:
            response += f"- {file[0]}\n"
        response += "\nלחץ על הכפתור להורדת הקבצים כ-ZIP."
    else:
        response = f"אין כרגע קבצים בקטגוריית '{category}'."

    keyboard = [[InlineKeyboardButton("📥 הורד ZIP", callback_data=f'download_zip_{category}')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.reply_text(response, reply_markup=reply_markup)

async def download_zip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, category):
    """יצירת קובץ ZIP עם סיסמה ושליחתו למשתמש."""
    await update.callback_query.answer()

    zip_path = f"./{category}_files.zip"
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, _, files in os.walk(f"./uploads/{category}"):
            for file in files:
                zipf.write(os.path.join(root, file), arcname=file)

    await update.callback_query.message.reply_document(
        document=open(zip_path, 'rb'),
        caption=f'סיסמה לזיפ: {PASSWORD}'
    )

async def main():
    create_database()  # יצירת בסיס הנתונים

    app = Application.builder().token(TOKEN).build()

    # הגדרת Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(upload_callback, pattern='upload'))
    app.add_handler(CallbackQueryHandler(download_callback, pattern='download'))
    app.add_handler(CallbackQueryHandler(free_playlists_callback, pattern='free_playlists'))
    app.add_handler(CallbackQueryHandler(apps_callback, pattern='apps'))
    app.add_handler(CallbackQueryHandler(lambda u, c: download_zip_callback(u, c, 'פלייליסטים'), pattern='download_zip_פלייליסטים'))
    app.add_handler(CallbackQueryHandler(lambda u, c: download_zip_callback(u, c, 'אפליקציות'), pattern='download_zip_אפליקציות'))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))

    # טיפול ב-event loop לפי מערכת ההפעלה
    if platform.system() == "Windows":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    try:
        await asyncio.Future()  # שמירה על ריצה עד עצירה ידנית
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
