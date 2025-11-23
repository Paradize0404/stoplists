
import json
from fastapi import FastAPI, Request
import asyncio
import threading
from main import main, run_daily_scheduler  # Импортируем основную функцию и планировщик
from daily_report import main as send_daily_report
import logging

app = FastAPI()

@app.get("/")
def index():
    return {"status": "ok", "info": "Webhook listener is alive"}

@app.on_event("startup")
async def startup_event():
    logging.info("🚀 Startup: отправляю ежедневный отчёт (тестовый запуск после деплоя)")
    try:
        await send_daily_report()
    except Exception as e:
        logging.error(f"Ошибка при отправке отчёта на деплое: {e}")
    
    # Запускаем фоновый планировщик для ежедневных отчётов
    logging.info("📅 Запускаю фоновый планировщик ежедневных отчётов...")
    threading.Thread(target=run_daily_scheduler, daemon=True).start()


@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()
    
    # 🔍 Красивый вывод в консоль:
    print("\n📦 Входящий вебхук от iiko:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    # 👇 Проверка, есть ли среди событий StopListUpdate
    if isinstance(data, list) and any(event.get("eventType") == "StopListUpdate" for event in data):
        print("🚀 Обнаружен StopListUpdate! Запускаю синхронизацию стоп-листа...")
        asyncio.create_task(main())
        return {"status": "ok", "detail": "Stop list update task started"}
    
    return {"status": "ignored", "detail": "No StopListUpdate event in payload"}