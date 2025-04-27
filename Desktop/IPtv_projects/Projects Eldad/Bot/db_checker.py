import sqlite3

def check_table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None

def check_column_exists(conn, table_name, column_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    return column_name in columns

def main():
    conn = sqlite3.connect('downloads.db')
    tables = ['files', 'downloads', 'downloads_group', 'group_file_events']

    for table in tables:
        exists = check_table_exists(conn, table)
        if exists:
            print(f"✅ טבלה {table} קיימת")
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [info[1] for info in cursor.fetchall()]
            print(f"   ➔ עמודות: {columns}")
        else:
            print(f"❌ טבלה {table} לא קיימת!")

    # בדיקה מיוחדת לעמודות שצריכות להיות קיימות
    print("\nבדיקות עמודות ספציפיות:")

    important_columns = {
        'files': ['file_name', 'username', 'uploader_id', 'category', 'upload_time'],
        'downloads': ['file_name', 'username', 'download_time'],
        'downloads_group': ['file_name', 'username', 'downloader_id', 'download_time'],
        'group_file_events': ['file_name', 'username', 'event_time', 'event_type']
    }

    for table, columns in important_columns.items():
        for column in columns:
            if check_column_exists(conn, table, column):
                print(f"✅ {table}.{column} נמצא")
            else:
                print(f"❌ {table}.{column} חסר!")

    conn.close()

if __name__ == "__main__":
    main()
