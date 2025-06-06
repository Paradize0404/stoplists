import os
import asyncio
import asyncpg
import httpx
from dotenv import load_dotenv

import requests
load_dotenv()

DB_CONFIG = {
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "database": os.getenv("PGDATABASE"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
}

BOT_TOKEN = os.getenv("BOT_TOKEN")
#CHAT_ID = 1877127405

IIKO_ORG_ID = os.getenv("ORG_ID")

async def get_all_chat_ids():
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        rows = await conn.fetch("SELECT chat_id FROM users WHERE chat_id IS NOT NULL;")
        await conn.close()
        return [row["chat_id"] for row in rows]
    except Exception as e:
        print(f"❌ Ошибка при получении chat_id пользователей: {e}")
        return []


async def fetch_token():
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        row = await conn.fetchrow("SELECT token FROM iiko_access_tokens ORDER BY created_at DESC LIMIT 1")
        await conn.close()
        return row["token"] if row else None
    except Exception as e:
        print(f"❌ Ошибка подключения к базе или получения токена: {e}")
        return None

def fetch_terminal_groups(token):
    url = "https://api-ru.iiko.services/api/1/terminal_groups"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    json_data = {
        "organizationIds": [IIKO_ORG_ID]
    }

    response = requests.post(url, headers=headers, json=json_data)
    response.raise_for_status()

    data = response.json()

    try:
        items = data["terminalGroups"][0]["items"]
        for item in items:
            print(f"📟 Терминальная группа: {item['name']} | ID: {item['id']}")
        return [item["id"] for item in items]
    except Exception as e:
        print(f"❌ Ошибка извлечения ID терминальных групп: {e}")
        return []



def fetch_stop_list_raw(token, terminal_group_ids):
    url = "https://api-ru.iiko.services/api/1/stop_lists"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    json_data = {
        "organizationIds": [IIKO_ORG_ID],
        "terminalGroupIds": terminal_group_ids  # ← исправлено: terminalGroupIds, не terminalGroupsIds
    }

    response = requests.post(url, headers=headers, json=json_data)
    print(f"\n📡 Ответ от stopLists:\n{response.status_code}\n{response.text}")

    if response.status_code == 200:
        try:
            data = response.json()
            # Достаём стоп-лист всех терминальных групп
            items = data["terminalGroupStopLists"][0]["items"][0]["items"]
            return items
        except Exception as e:
            print(f"❌ Ошибка при парсинге стоп-листа: {e}")
            return []
    else:
        print("❌ Запрос стоп-листа не удался.")
        return []

async def map_stoplist_with_db(db_config, stoplist_items):
    conn = await asyncpg.connect(**db_config)

    # Собираем список всех sku из стоп-листа
    skus = [item["sku"] for item in stoplist_items]
    query = "SELECT code, name FROM nomenclature WHERE code = ANY($1)"
    rows = await conn.fetch(query, skus)

    await conn.close()

    # Словарь: sku → name
    sku_name_map = {row["code"]: row["name"] for row in rows}

    # Формируем результат
    result = []
    for item in stoplist_items:
        name = sku_name_map.get(item["sku"], "[НЕ НАЙДЕНО В БД]")
        item["name"] = name  # ← Вот это важно!
        result.append(f"{name} | SKU: {item['sku']} | Остаток: {item['balance']}")
    
    return result



async def sync_stoplist_with_db(stoplist_items):
    conn = await asyncpg.connect(**DB_CONFIG)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_stoplist (
            sku TEXT PRIMARY KEY,
            balance REAL,
            name TEXT
        );
    """)

    # Получение текущих SKU и названий из таблицы
    current_rows = await conn.fetch("SELECT sku, name FROM active_stoplist;")
    current_skus = {row["sku"]: row["name"] for row in current_rows}

    # SKU из API
    incoming_skus = {item["sku"]: item["name"] for item in stoplist_items}

    # Новые
    new_items = [(sku, name) for sku, name in incoming_skus.items() if sku not in current_skus]
    # Уже были
    existing_items = [(sku, name) for sku, name in incoming_skus.items() if sku in current_skus]
    # Удаляемые
    to_delete = [sku for sku in current_skus if sku not in incoming_skus]

    # Вставка новых
    for sku, name in new_items:
        item = next(i for i in stoplist_items if i["sku"] == sku)
        await conn.execute("""
            INSERT INTO active_stoplist (sku, balance, name) VALUES ($1, $2, $3);
        """, sku, item["balance"], name)

    # Удаление исчезнувших
    if to_delete:
        await conn.execute("""
            DELETE FROM active_stoplist WHERE sku = ANY($1);
        """, to_delete)

    await conn.close()

    # 🧾 Логируем результат
    print("\nНовые блюда в стоп-листе 🚫")
    for _, name in new_items:
        print(f"▫️ {name}")
    print("\nУже в стоп-листе")
    for _, name in existing_items:
        print(f"▫️ {name}")
    print("\n#стоплист")

    print(f"\n✅ Синхронизация завершена. Добавлено: {len(new_items)}, удалено: {len(to_delete)}")

def format_stoplist_message(added_items, removed_items, existing_items):
    message = "Новые блюда в стоп-листе 🚫"
    if added_items:
        for item in added_items:
            message += f"\n▫️ {item['name']}"
    else:
        message += "\n▫️ —"

    message += "\n\nУдалены из стоп-листа ✅"
    if removed_items:
        for item in removed_items:
            message += f"\n▫️ {item['name']}"
    else:
        message += "\n▫️ —"

    message += "\n\nОстались в стоп-листе"
    if existing_items:
        for item in existing_items:
            message += f"\n▫️ {item['name']}"
    else:
        message += "\n▫️ —"

    message += f"\n\n#стоплист\n\n✅ Синхронизация завершена. Добавлено: {len(added_items)}, удалено: {len(removed_items)}"
    return message

async def send_telegram_message(text: str):
    chat_ids = await get_all_chat_ids()
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": text
        }
        try:
            response = httpx.post(url, json=payload)
            response.raise_for_status()
            print(f"✅ Сообщение успешно отправлено chat_id={chat_id}")
        except Exception as e:
            print(f"❌ Ошибка при отправке в Telegram (chat_id={chat_id}): {e}")


async def main():
    token = await fetch_token()
    if not token:
        print("❌ Токен не найден.")
        return

    print(f"✅ Актуальный токен: {token}")
    terminal_group_ids = fetch_terminal_groups(token)
    stoplist_items = fetch_stop_list_raw(token, terminal_group_ids)

    # 🧾 Сопоставим с БД и выведем в консоль
    mapped = await map_stoplist_with_db(DB_CONFIG, stoplist_items)
    print("\n🧾 Сопоставленные стоп-позиции:")
    for line in mapped:
        print("•", line)

    # 📥 Получаем текущее состояние до обновления
    conn = await asyncpg.connect(**DB_CONFIG)
    current_rows = await conn.fetch("SELECT sku, name FROM active_stoplist;")
    await conn.close()
    current_skus = {row["sku"]: row["name"] for row in current_rows}
    incoming_skus = {item["sku"]: item["name"] for item in stoplist_items}

    added_items = [{"sku": sku, "name": name} for sku, name in incoming_skus.items() if sku not in current_skus]
    removed_items = [{"sku": sku, "name": name} for sku, name in current_skus.items() if sku not in incoming_skus]
    existing_items = [{"sku": sku, "name": name} for sku, name in incoming_skus.items() if sku in current_skus]

    # 💾 Обновим таблицу
    await sync_stoplist_with_db(stoplist_items)

    # 📤 Отправим сообщение
    msg = format_stoplist_message(added_items, removed_items, existing_items)
    await send_telegram_message(msg)

if __name__ == "__main__":
    asyncio.run(main())

