import sqlite3

# התחברות לבסיס הנתונים
conn = sqlite3.connect('downloads.db')
c = conn.cursor()

# קבצים שהועלו
c.execute("SELECT * FROM files")
files = c.fetchall()
print("קבצים שהועלו:")
for file in files:
    print(file)

# לוג הורדות
c.execute("SELECT * FROM downloads")
downloads = c.fetchall()
print("\nלוג הורדות:")
for download in downloads:
    print(download)

# סיום החיבור
conn.close()
