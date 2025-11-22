import json
from fastapi import FastAPI, Request
import asyncio

from main import sync_stoplist, iiko_token   # <-- правильный импорт
from daily_report import main as send_daily_report

app = FastAPI()

@app.get("/")
def index():
    return {"status": "ok", "info": "Webhook listener is alive"}


@app.on_event("startup")
async def startup_event():
    print("🚀 Startup: отправляю ежедневный отчёт (тестовый запуск после деплоя)")
    asyncio.create_task(send_daily_report())


@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()
    
    print("\n📦 Входящий вебхук от iiko:")
    print(json.dumps(data, indent=2, ensure_ascii=False))

    # Проверяем StopListUpdate
    if isinstance(data, list) and any(event.get("eventType") == "StopListUpdate" for event in data):
        print("🚀 Обнаружен StopListUpdate! Запускаю синхронизацию стоп-листа...")

        token = await iiko_token()                 # <-- получить токен
        asyncio.create_task(sync_stoplist(token))  # <-- вызвать правильную функцию

        return {"status": "ok", "detail": "Stop list update task started"}

    return {"status": "ignored", "detail": "No StopListUpdate event in payload"}
