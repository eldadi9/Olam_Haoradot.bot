@echo off
title 📊 הפעלת הבוט והדשבורד

:: מעבר לתיקיית הפרויקט שלך
cd /d "C:\Users\Master_PC\Desktop\IPtv_projects\Projects Eldad\Bot"

:: הפעלת הבוט בחלון CMD חדש
start "BOT" cmd /k python bot.py

:: הפעלת שרת הדשבורד FastAPI בחלון CMD חדש
start "DASHBOARD" cmd /k uvicorn dashboard.main:app --reload

:: המתנה קצרה שהשרת יעלה
timeout /t 3 >nul

:: פתיחת דפדפן על הדשבורד
start http://127.0.0.1:8000/dashboard

exit
