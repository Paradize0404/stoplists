from fastapi import FastAPI, Request
import asyncio
from main import main  # Импортируем основную функцию

app = FastAPI()

@app.get("/")
def index():
    return {"status": "ok", "info": "Webhook listener is alive"}

@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()
    
    # Проверка, что это нужный вебхук
    if data.get("eventType") == "StopListUpdate":
        print("📩 Получен вебхук StopListUpdate!")
        asyncio.create_task(main())  # Асинхронный запуск основного скрипта
        return {"status": "ok", "detail": "Stop list update task started"}
    
    return {"status": "ignored", "detail": "Not a StopListUpdate event"}