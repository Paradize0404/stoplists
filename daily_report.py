import os
import asyncio
import asyncpg
import httpx
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "database": os.getenv("PGDATABASE"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
}

BOT_TOKEN = os.getenv("BOT_TOKEN")

# сюда впишешь Telegram ID, кому нужен отчёт в конце дня
REPORT_RECIPIENTS = [
    # 1877127405,
]


async def db():
    return await asyncpg.connect(**DB_CONFIG)


def format_duration(seconds: int) -> str:
    """Перевод секунд в формат ЧЧ:ММ."""
    if seconds <= 0:
        return "00:00"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours:02}:{minutes:02}"


async def fetch_daily_stats():
    conn = await db()

    rows = await conn.fetch("""
        SELECT sku, name, SUM(duration_seconds) AS total_sec
        FROM stoplist_history
        WHERE date = CURRENT_DATE
          AND duration_seconds IS NOT NULL
        GROUP BY sku, name
        ORDER BY total_sec DESC
    """)

    # закрываем «висящие» стопы, если день закончился
    await conn.execute("""
        UPDATE stoplist_history
        SET ended_at = NOW(),
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
        WHERE date = CURRENT_DATE
          AND ended_at IS NULL
    """)

    await conn.close()
    return rows


def build_report(rows):
    today = date.today().strftime("%d.%m.%Y")
    msg = f"📊 Отчёт по стоп-листу за {today}\n\n"

    if not rows:
        msg += "Сегодня не было стопов."
        return msg

    for row in rows:
        sku = row["sku"]
        name = row["name"]
        sec = int(row["total_sec"])

        msg += f"▫️ {name} — {format_duration(sec)}\n"

    return msg


async def send_report(text):
    if not REPORT_RECIPIENTS:
        print("⚠️ REPORT_RECIPIENTS пуст — отчёт некому отправлять.")
        return

    for chat_id in REPORT_RECIPIENTS:
        try:
            httpx.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text}
            )
            print(f"Отчёт отправлен → {chat_id}")
        except Exception as e:
            print(f"Ошибка отправки отчёта {chat_id}: {e}")


async def main():
    rows = await fetch_daily_stats()
    report = build_report(rows)
    await send_report(report)


if __name__ == "__main__":
    asyncio.run(main())
