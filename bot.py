from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import sqlite3
import asyncio
import platform

TOKEN = '7769754941:AAHOUYo_OvNEqIaxYeRQeH_6yQjOo_iMcaA'

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('ברוך הבא! שלח קובץ כדי שאוכל לעקוב אחרי מי שמוריד אותו או שלח את שם הקובץ כדי להוריד.')

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    file_id = update.message.document.file_id
    file_name = update.message.document.file_name
    username = user.username or "לא זמין"
    first_name = user.first_name
    last_name = user.last_name or "לא זמין"
    
    # שמירת פרטים בבסיס הנתונים
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files
                 (file_id TEXT PRIMARY KEY, file_name TEXT, uploader_id INTEGER, username TEXT, first_name TEXT, last_name TEXT)''')
    c.execute('''INSERT OR REPLACE INTO files (file_id, file_name, uploader_id, username, first_name, last_name)
                 VALUES (?, ?, ?, ?, ?, ?)''', (file_id, file_name, user.id, username, first_name, last_name))
    conn.commit()
    conn.close()
    
    await update.message.reply_text(f'הקובץ "{file_name}" נשמר במערכת. כדי להוריד אותו, שלח את השם שלו שוב.')

async def download_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''SELECT file_name, uploader_id, username, first_name, last_name FROM files''')
    files = c.fetchall()
    conn.close()
    
    if files:
        response = "רשימת הקבצים והמשתמשים שהעלו אותם:\n"
        for file in files:
            response += (f'קובץ: {file[0]}, הועלה על ידי: {file[2]} (ID: {file[1]}, שם פרטי: {file[3]}, '
                         f'שם משפחה: {file[4]})\n')
    else:
        response = "אין כרגע קבצים במערכת."
    
    await update.message.reply_text(response)

async def file_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    requested_file_name = update.message.text.strip()
    
    # חיפוש הקובץ במערכת
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''SELECT file_id, file_name FROM files WHERE file_name = ?''', (requested_file_name,))
    result = c.fetchone()
    
    if result:
        file_id, file_name = result
        
        # שמירת פרטי המשתמש שהוריד את הקובץ
        c.execute('''CREATE TABLE IF NOT EXISTS downloads
                     (download_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT, first_name TEXT, last_name TEXT, file_name TEXT, file_id TEXT)''')
        c.execute('''INSERT INTO downloads (user_id, username, first_name, last_name, file_name, file_id)
                     VALUES (?, ?, ?, ?, ?, ?)''', (user.id, user.username or user.first_name, user.first_name, user.last_name or "לא זמין", file_name, file_id))
        conn.commit()
        conn.close()
        
        # שליחת הקובץ למשתמש
        await context.bot.send_document(chat_id=update.message.chat_id, document=file_id)
        await update.message.reply_text(f'הקובץ "{file_name}" נשלח אליך.')
    else:
        conn.close()
        await update.message.reply_text('הקובץ לא נמצא במערכת. וודא שהשם נכון.')

async def downloads_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('downloads.db')
    c = conn.cursor()
    c.execute('''SELECT user_id, username, first_name, last_name, file_name FROM downloads''')
    downloads = c.fetchall()
    conn.close()
    
    if downloads:
        response = "רשימת ההורדות:\n"
        for download in downloads:
            response += (f'משתמש: {download[1]} (ID: {download[0]}, שם פרטי: {download[2]}, '
                         f'שם משפחה: {download[3]}), הוריד את הקובץ: {download[4]}\n')
    else:
        response = "אין כרגע הורדות במערכת."
    
    await update.message.reply_text(response)

async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))
    app.add_handler(CommandHandler("info", download_info))
    app.add_handler(CommandHandler("downloads", downloads_info))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, file_request_handler))

    # שימוש ב-event loop שמותאם ל-Windows אם מדובר ב-Windows
    if platform.system() == "Windows":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    try:
        await asyncio.Future()  # שמירה על הרצה עד שייבקשו עצירה
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
