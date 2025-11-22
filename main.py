import os
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import httpx
import asyncpg
import asyncio

from daily_report import send_daily_report

# ----------------------------------------------------------
#   НАСТРОЙКИ
# ----------------------------------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN")
STOPLIST_CHAT_IDS = os.getenv("STOPLIST_CHAT_IDS", "").split(",")

IIKO_API_LOGIN = os.getenv("IIKO_API_LOGIN")
IIKO_ORG_ID = os.getenv("IIKO_ORG_ID")

DATABASE_URL = os.getenv("DATABASE_URL")

KLG = ZoneInfo("Europe/Kaliningrad")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ----------------------------------------------------------
#   БАЗА ДАННЫХ
# ----------------------------------------------------------

async def db():
    return await asyncpg.connect(DATABASE_URL)


async def ensure_tables():
    conn = await db()

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS stoplist_messages (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT NOT NULL,
        message_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL
    );
    """)

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS stoplist_log (
        id SERIAL PRIMARY KEY,
        product_id TEXT NOT NULL,
        product_name TEXT NOT NULL,
        started_at TIMESTAMP NOT NULL,
        ended_at TIMESTAMP
    );
    """)

    await conn.close()

# ----------------------------------------------------------
#   TELEGRAM
# ----------------------------------------------------------

async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": int(chat_id),
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
        )
        data = r.json()
        if not data.get("ok"):
            logging.error(f"Ошибка Telegram: {data}")
        return data


async def tg_delete(chat_id, message_id):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
            json={"chat_id": int(chat_id), "message_id": int(message_id)}
        )


# ----------------------------------------------------------
#   IIKO AUTH
# ----------------------------------------------------------

async def iiko_token():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://api-ru.iiko.services/api/1/access_token",
            json={"apiLogin": IIKO_API_LOGIN}
        )
        return r.json()["token"]


# ----------------------------------------------------------
#   ЗАПРОС СТОП-ЛИСТА
# ----------------------------------------------------------

async def fetch_stoplist(token):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://api-ru.iiko.services/api/1/stop_lists",
            headers={"Authorization": f"Bearer {token}"},
            json={"organizationId": IIKO_ORG_ID}
        )
        return r.json()

# ----------------------------------------------------------
#   ПОЛУЧЕНИЕ ПОСЛЕДНЕГО СООБЩЕНИЯ СТОП-ЛИСТА
# ----------------------------------------------------------

async def get_last_message(chat_id):
    conn = await db()
    row = await conn.fetchrow(
        """
        SELECT message_id
        FROM stoplist_messages
        WHERE chat_id=$1
        ORDER BY id DESC
        LIMIT 1
        """,
        int(chat_id)
    )
    await conn.close()
    return row["message_id"] if row else None


async def save_message(chat_id, msg_id):
    conn = await db()
    await conn.execute(
        """
        INSERT INTO stoplist_messages (chat_id, message_id, created_at)
        VALUES ($1, $2, $3)
        """,
        int(chat_id),
        int(msg_id),
        datetime.now(KLG)
    )
    await conn.close()


# ----------------------------------------------------------
#   ОТРИСОВКА СТОП-ЛИСТА
# ----------------------------------------------------------

def render_stoplist(items):
    if not items:
        return "✔️ <b>Стоп-лист пуст</b>"

    text = "🚫 <b>СТОП-ЛИСТ</b>\n\n"
    for p in items:
        name = p.get("name")
        bal = p.get("balance", 0)
        text += f"• <b>{name}</b> — {bal}\n"
    return text


# ----------------------------------------------------------
#   ОБНОВЛЕНИЕ СООБЩЕНИЯ СТОП-ЛИСТА (удаляем старое → отправляем новое)
# ----------------------------------------------------------

async def update_stoplist_message(stop_items):
    text = render_stoplist(stop_items)

    for chat in STOPLIST_CHAT_IDS:
        if not chat.strip():
            continue

        last = await get_last_message(chat)

        # удалить старое
        if last:
            try:
                await tg_delete(chat, last)
            except Exception as e:
                logging.error(f"Не удалось удалить старое сообщение: {e}")

        # отправить новое
        msg = await tg_send(chat, text)
        if msg.get("ok"):
            await save_message(chat, msg["result"]["message_id"])


# ----------------------------------------------------------
#   СИНХРОНИЗАЦИЯ СТОП-ЛИСТА С БАЗОЙ ДАННЫХ
# ----------------------------------------------------------

async def sync_stoplist(token):
    data = await fetch_stoplist(token)

    # собираем список товаров с балансом 0
    stop_items = []
    for tg in data.get("terminalGroups", []):
        for item in tg.get("items", []):
            if item["balance"] == 0:
                stop_items.append(item)

    now = datetime.now(KLG)

    conn = await db()

    # ------------------------------------------------------
    # Открываем новые стопы
    # ------------------------------------------------------

    for p in stop_items:
        pid = p["productId"]
        pname = p["name"]

        exists = await conn.fetchrow(
            """
            SELECT 1 FROM stoplist_log
            WHERE product_id=$1 AND ended_at IS NULL
            """,
            pid
        )

        if not exists:
            await conn.execute(
                """
                INSERT INTO stoplist_log (product_id, product_name, started_at)
                VALUES ($1, $2, $3)
                """,
                pid,
                pname,
                now
            )

    # ------------------------------------------------------
    # Закрываем те, которых больше нет в стопе
    # ------------------------------------------------------

    active_ids = {p["productId"] for p in stop_items}

    open_rows = await conn.fetch(
        "SELECT * FROM stoplist_log WHERE ended_at IS NULL"
    )

    for row in open_rows:
        if row["product_id"] not in active_ids:
            await conn.execute(
                "UPDATE stoplist_log SET ended_at=$1 WHERE id=$2",
                now,
                row["id"]
            )

    await conn.close()

    # ------------------------------------------------------
    # Обновляем сообщение стоп-листа
    # ------------------------------------------------------

    await update_stoplist_message(stop_items)

# ----------------------------------------------------------
#   SCHEDULER — ежедневный отчёт в 21:00 (Калининград)
# ----------------------------------------------------------

async def scheduler():
    while True:
        now = datetime.now(KLG)
        target = now.replace(hour=21, minute=0, second=0, microsecond=0)

        # если время уже прошло — перенос на завтра
        if now > target:
            target += timedelta(days=1)

        wait_seconds = (target - now).total_seconds()
        logging.info(f"⏳ Следующий ежедневный отчёт запланирован на {target}")

        await asyncio.sleep(wait_seconds)

        try:
            logging.info("📤 Отправляю ежедневный отчёт...")
            await send_daily_report()
        except Exception as e:
            logging.error(f"Ошибка при отправке ежедневного отчёта: {e}")


# ----------------------------------------------------------
#   MAIN
# ----------------------------------------------------------

async def main():
    logging.info("🔧 Инициализация таблиц...")
    await ensure_tables()

    # запускаем ежедневный планировщик
    asyncio.create_task(scheduler())

    # отправляем отчёт при деплое — разово
    asyncio.create_task(send_daily_report())

    # ничего не запускаем в цикле — webhook сам вызывает sync_stoplist()


if __name__ == "__main__":
    asyncio.run(main())
