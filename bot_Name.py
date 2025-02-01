import requests

TOKEN = "7757317671:AAHlq8yWLzP4mrgEovVoVZb_2j9ilWt0OlQ"
CHAT_ID = "@Olam_Haoradot_IL"  # יש להכניס את שם המשתמש של הקבוצה (ללא קישור)
MESSAGE = "זוהי הודעה שנשלחת בשם הקבוצה דרך הבוט!"

url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
data = {"chat_id": CHAT_ID, "text": MESSAGE}

response = requests.post(url, data=data)
print(response.json())
