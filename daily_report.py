import os
import asyncio
import asyncpg
import httpx
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

KLG = ZoneInfo("Europe/Kaliningrad")

DB_CONFIG = {
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "database": os.getenv("PGDATABASE"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
}

BOT_TOKEN = os.getenv("BOT_TOKEN")

# сюда впишешь Telegram ID, кому нужен отчёт
REPORT_RECIPIENTS = [
    1877127405,
]


async def db():
    return await asyncpg.connect(**DB_CONFIG)


def format_duration(seconds: int) -> str:
    if seconds <= 0:
        return "00:00"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours:02}:{minutes:02}"


async def fetch_daily_stats():

    # вчера в Кёнигсберге
    now_klg = datetime.now(KLG)
    target_day = (now_klg - timedelta(days=1)).date()

    day_start = datetime.combine(target_day, time(8, 0), tzinfo=KLG)
    day_end   = datetime.combine(target_day, time(21, 0), tzinfo=KLG)

    conn = await db()

    # тянем ВСЕ записи, которые пересекли период
    rows = await conn.fetch("""
        SELECT
            sku,
            name,
            started_at,
            ended_at
        FROM stoplist_history
        WHERE started_at <= $2
          AND (ended_at IS NULL OR ended_at >= $1)
    """, day_start, day_end)

    await conn.close()

    # считаем длительность в рамках окна
    stats = {}

    for row in rows:
        sku = row["sku"]
        name = row["name"]

        s = row["started_at"]
        if s.tzinfo is None:
            s = s.replace(tzinfo=KLG)
        else:
            s = s.astimezone(KLG)
        e = row["ended_at"]
        if e:
            if e.tzinfo is None:
                e = e.replace(tzinfo=KLG)
            else:
                e = e.astimezone(KLG)
        else:
            # стоп продолжается — обрезаем по day_end
            e = now_klg
            if e > day_end:
                e = day_end

        # пересечение с окном
        seg_start = max(s, day_start)
        seg_end   = min(e, day_end)

        duration = (seg_end - seg_start).total_seconds()

        if duration > 0:
            stats.setdefault(sku, {"name": name, "sec": 0})
            stats[sku]["sec"] += duration

    return stats


def build_report(stats):
    target_day = (datetime.now(KLG) - timedelta(days=1)).strftime("%d.%m.%Y")
    msg = f"📊 Отчёт по стоп-листу за {target_day}\n\n"

    if not stats:
        msg += "Не было стопов в этот период."
        return msg

    # сортировка по времени
    items = sorted(stats.items(), key=lambda x: x[1]["sec"], reverse=True)

    for sku, data in items:
        msg += f"▫️ {data['name']} — {format_duration(int(data['sec']))}\n"

    return msg


async def send_report(text):
    for chat_id in REPORT_RECIPIENTS:
        try:
            httpx.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text}
            )
            print(f"Отчёт отправлен → {chat_id}")
        except Exception as e:
            print(f"Ошибка отправки отчёта {chat_id}: {e}")


async def send_daily_report():
    stats = await fetch_daily_stats()
    report = build_report(stats)
    await send_report(report)


async def main():
    await send_daily_report()


if __name__ == "__main__":
    asyncio.run(main())
